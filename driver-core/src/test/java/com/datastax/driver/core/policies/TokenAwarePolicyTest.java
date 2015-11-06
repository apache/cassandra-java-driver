/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core.policies;

import java.nio.ByteBuffer;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;

public class TokenAwarePolicyTest {
    @Test(groups = "long")
    public void should_shuffle_replicas_when_requested() {
    	CCMBridge ccm = CCMBridge.builder("test").withNodes(3).build();
        testShuffleReplicas(new TokenAwarePolicy(new RoundRobinPolicy(), true), true, ccm, 3);
    }

    @Test(groups = "long")
    public void should_not_shuffle_replicas_when_not_requested() {
    	CCMBridge ccm = CCMBridge.builder("test").withNodes(3).build();
        testShuffleReplicas(new TokenAwarePolicy(new RoundRobinPolicy(), false), false, ccm, 3);
    }

    @Test(groups = "long")
    @CassandraVersion(major=2.1)
    public void should_not_plan_queries_for_incompatible_protocol_versions() {
    	CCMBridge ccm = CCMBridge.builder("test").withNodes(3).notStarted().build();
    	// contact node is native v3 so we should not query 2.0 nodes
        ccm.setNodeVersion(2, "2.0.16");  
        ccm.start();
        testShuffleReplicas(new TokenAwarePolicy(new RoundRobinPolicy(), false), null, ccm, 2);
    }

    @Test(groups = "long")
    @CassandraVersion(major=2.1)
    public void should_plan_queries_for_downgraded_protocol_version() {
		CCMBridge ccm = CCMBridge.builder("test").withNodes(3).notStarted().build();
        ccm.setNodeVersion(1, "2.0.16"); // contact node - should downgrade to native v2
        ccm.start();
        testShuffleReplicas(new TokenAwarePolicy(new RoundRobinPolicy(), false), null, ccm, 3);
    }

    private void testShuffleReplicas(TokenAwarePolicy loadBalancingPolicy, Boolean expectShuffled,
    		CCMBridge ccm, int expectQueryPlans) {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                             .addContactPoint(CCMBridge.ipOfNode(1))
                             .withLoadBalancingPolicy(loadBalancingPolicy)
                             .withQueryOptions(nonDebouncingQueryOptions())
                             .build();

            String keyspace = "ks";
            ByteBuffer routingKey = Bytes.fromHexString("0xCAFEBABE");

            Session session = cluster.connect();
            // Use a replication factor of 2, each routing key on this keyspace will corresponding to 2 hosts out of 3
            session.execute(String.format("CREATE KEYSPACE %s "
                                          + "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}",
                                          keyspace));

            // Actual query does not matter, only the keyspace and routing key will be used
            SimpleStatement statement = new SimpleStatement("foo");
            statement.setKeyspace(keyspace);
            statement.setRoutingKey(routingKey);

            // Identify the two replicas for our routing key
            List<Host> replicas = Lists.newArrayList(cluster.getMetadata().getReplicas(keyspace, routingKey));
            assertThat(replicas).hasSize(2);

            if(expectShuffled == null) {
            	List<Host> queryPlan = Lists.newArrayList(loadBalancingPolicy.newQueryPlan(null, statement));
                assertThat(queryPlan).hasSize(expectQueryPlans);
            } else {
            	// Run a few query plans, make sure that these two replicas come first, shuffled or not depending on
                // what we expect
            	boolean shuffledAtLeastOnce = false;
                for (int i = 0; i < 10; i++) {
                    List<Host> queryPlan = Lists.newArrayList(loadBalancingPolicy.newQueryPlan(null, statement));
                    assertThat(queryPlan).hasSize(expectQueryPlans);

                    List<Host> firstTwo = queryPlan.subList(0, 2);
                    assertThat(firstTwo).containsAll(replicas); // order does not matter

                    if (!firstTwo.equals(replicas))
                        shuffledAtLeastOnce = true;
                }
                assertThat(shuffledAtLeastOnce).isEqualTo(expectShuffled);
            }

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
