package com.datastax.driver.core.policies;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.Bytes;

public class TokenAwarePolicyTest {
    @Test(groups = "long")
    public void should_shuffle_replicas_when_requested() {
        testShuffleReplicas(new TokenAwarePolicy(new RoundRobinPolicy(), true),
                            true);
    }

    @Test(groups = "long")
    public void should_not_shuffle_replicas_when_not_requested() {
        testShuffleReplicas(new TokenAwarePolicy(new RoundRobinPolicy(), false),
                            false);
    }

    private void testShuffleReplicas(TokenAwarePolicy loadBalancingPolicy, boolean expectShuffled) {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 3);
            cluster = Cluster.builder()
                             .addContactPoint(CCMBridge.ipOfNode(1))
                             .withLoadBalancingPolicy(loadBalancingPolicy)
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

            // Run a few query plans, make sure that these two replicas come first, shuffled or not depending on
            // what we expect
            boolean shuffledAtLeastOnce = false;
            for (int i = 0; i < 10; i++) {
                List<Host> queryPlan = Lists.newArrayList(loadBalancingPolicy.newQueryPlan(null, statement));
                assertThat(queryPlan).hasSize(3);

                List<Host> firstTwo = queryPlan.subList(0, 2);
                assertThat(firstTwo).containsAll(replicas); // order does not matter

                if (!firstTwo.equals(replicas))
                    shuffledAtLeastOnce = true;
            }
            assertThat(shuffledAtLeastOnce).isEqualTo(expectShuffled);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
