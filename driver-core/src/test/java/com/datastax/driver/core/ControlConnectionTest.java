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
package com.datastax.driver.core;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.utils.CassandraVersion;

public class ControlConnectionTest {
    @Test(groups = "short")
    public void should_prevent_simultaneous_reconnection_attempts() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;

        // Custom load balancing policy that counts the number of calls to newQueryPlan().
        // Since we don't open any session from our Cluster, only the control connection reattempts are calling this
        // method, therefore the invocation count is equal to the number of attempts.
        QueryPlanCountingPolicy loadBalancingPolicy = new QueryPlanCountingPolicy(Policies.defaultLoadBalancingPolicy());
        AtomicInteger reconnectionAttempts = loadBalancingPolicy.counter;

        // Custom reconnection policy with a very large delay (longer than the test duration), to make sure we count
        // only the first reconnection attempt of each reconnection handler.
        ReconnectionPolicy reconnectionPolicy = new ReconnectionPolicy() {
            @Override
            public ReconnectionSchedule newSchedule() {
                return new ReconnectionSchedule() {
                    @Override
                    public long nextDelayMs() {
                        return 60 * 1000;
                    }
                };
            }
        };

        try {
            ccm = CCMBridge.builder("test").withNodes(2).build();
            // We pass only the first host as contact point, so we know the control connection will be on this host
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withReconnectionPolicy(reconnectionPolicy)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build();
            cluster.init();

            // Kill the control connection host, there should be exactly one reconnection attempt
            ccm.stop(1);
            TimeUnit.SECONDS.sleep(1); // Sleep for a while to make sure our final count is not the result of lucky timing
            assertThat(reconnectionAttempts.get()).isEqualTo(1);

            ccm.stop(2);
            TimeUnit.SECONDS.sleep(1);
            assertThat(reconnectionAttempts.get()).isEqualTo(2);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * Test for JAVA-509: UDT definitions were not properly parsed when using the default protocol version.
     *
     * This did not appear with other tests because the UDT needs to already exist when the driver initializes.
     * Therefore we use two different driver instances in this test.
     */
    @Test(groups = "short")
    @CassandraVersion(major=2.1)
    public void should_parse_UDT_definitions_when_using_default_protocol_version() {
        CCMBridge ccm = null;
        Cluster cluster = null;

        try {
            ccm = CCMBridge.builder("test").build();

            // First driver instance: create UDT
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
            Session session = cluster.connect();
            session.execute("create keyspace ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("create type ks.foo (i int)");
            cluster.close();

            // Second driver instance: read UDT definition
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
            UserType fooType = cluster.getMetadata().getKeyspace("ks").getUserType("foo");

            assertThat(fooType.getFieldNames()).containsExactly("i");
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
    
    /**
     * Ensures that if the host that the Control Connection is connected to is removed/decommissioned that the
     * Control Connection is reestablished to another host.
     *
     * @since 2.0.9
     * @jira_ticket JAVA-597
     * @expected_result Control Connection is reestablished to another host.
     * @test_category control_connection
     */
    @Test(groups = "long")
    public void should_reestablish_if_control_node_decommissioned() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;

        try {
            ccm = CCMBridge.builder("test").withNodes(3).build();

            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .build();
            cluster.init();

            // Ensure the control connection host is that of the first node.
            String controlHost = cluster.manager.controlConnection.connectedHost().getAddress().getHostAddress();
            assertThat(controlHost).isEqualTo(CCMBridge.ipOfNode(1));

            // Decommission the node.
            ccm.decommissionNode(1);

            // Ensure that the new control connection is not null and it's host is not equal to the decommissioned node.
            Host newHost = cluster.manager.controlConnection.connectedHost();
            assertThat(newHost).isNotNull();
            assertThat(newHost.getAddress().getHostAddress()).isNotEqualTo(controlHost);
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

   static class QueryPlanCountingPolicy extends DelegatingLoadBalancingPolicy {

        final AtomicInteger counter = new AtomicInteger();

        public QueryPlanCountingPolicy(LoadBalancingPolicy delegate) {
            super(delegate);
        }

        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            counter.incrementAndGet();
            return super.newQueryPlan(loggedKeyspace, statement);
        }
    }
}
