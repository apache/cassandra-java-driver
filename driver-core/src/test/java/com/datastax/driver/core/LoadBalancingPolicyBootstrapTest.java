/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;

import static com.datastax.driver.core.LoadBalancingPolicyBootstrapTest.HistoryPolicy.Action.*;
import static com.datastax.driver.core.LoadBalancingPolicyBootstrapTest.HistoryPolicy.entry;

public class LoadBalancingPolicyBootstrapTest {

    /**
     * Ensures that when a cluster is initialized that {@link LoadBalancingPolicy#init(Cluster, Collection)} is called
     * with each reachable contact point.
     *
     * @test_category load_balancing:notification
     * @expected_result init() is called for each of two contact points.
     */
    @Test(groups = "short")
    public void should_init_policy_with_up_contact_points() throws Exception {
        HistoryPolicy policy = new HistoryPolicy(new RoundRobinPolicy());

        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 2);
            cluster = Cluster.builder()
                .addContactPoints(CCMBridge.ipOfNode(1), CCMBridge.ipOfNode(2))
                .withLoadBalancingPolicy(policy)
                .build();

            cluster.init();

            assertThat(policy.history).containsExactly(
                entry(INIT, TestUtils.findHost(cluster, 1)),
                entry(INIT, TestUtils.findHost(cluster, 2))
            );
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * Ensures that {@link LoadBalancingPolicy#onDown(Host)} is called for a contact point that goes down,
     * but only after {@link LoadBalancingPolicy#init(Cluster, Collection)} has been called.
     *
     * @test_category load_balancing:notification
     * @expected_result init() is called
     * @jira_ticket JAVA-613
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short")
    public void should_send_down_notifications_after_init_when_contact_points_are_down() throws Exception {
        HistoryPolicy policy = new HistoryPolicy(new RoundRobinPolicy());

        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            cluster = Cluster.builder()
                .addContactPoints(CCMBridge.ipOfNode(1), CCMBridge.ipOfNode(2))
                .withLoadBalancingPolicy(policy)
                .build();

            // In order to validate this behavior, we need to stop the first node that would be attempted to be
            // established as the control connection.  The control connection uses the Cluster metadata hosts and
            // tries hosts in the order of the hosts iterator.  Therefore this logic is dependent on the internal
            // behavior of the control connection which is subject to change.
            Metadata m = new Metadata(cluster.manager);
            m.add(new InetSocketAddress(CCMBridge.ipOfNode(1), 9042));
            m.add(new InetSocketAddress(CCMBridge.ipOfNode(2), 9042));
            String hostName = m.allHosts().iterator().next().getSocketAddress().getHostName();
            int nodeToStop = hostName.endsWith("2") ? 2 : 1;
            int activeNode = nodeToStop == 2 ? 1 : 2;

            ccm = CCMBridge.create("test", 2);
            ccm.stop(nodeToStop);
            ccm.waitForDown(nodeToStop);

            cluster.init();

            assertThat(policy.history).containsExactly(
                entry(INIT, TestUtils.findHost(cluster, activeNode)),
                entry(DOWN, TestUtils.findHost(cluster, nodeToStop))
            );

            cluster.connect();
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    static class HistoryPolicy extends DelegatingLoadBalancingPolicy {
        enum Action {INIT, UP, DOWN, ADD, REMOVE, SUSPECT}

        static class Entry {
            final Action action;
            final Host host;

            public Entry(Action action, Host host) {
                this.action = action;
                this.host = host;
            }

            @Override
            public boolean equals(Object other) {
                if (other == this)
                    return true;
                if (other instanceof Entry) {
                    Entry that = (Entry)other;
                    return this.action == that.action && this.host.equals(that.host);
                }
                return false;
            }

            @Override
            public int hashCode() {
                return Objects.hashCode(action, host);
            }

            @Override public String toString() {
                return Objects.toStringHelper(this).add("action", action).add("host", host).toString();
            }
        }

        static Entry entry(Action action, Host host) {
            return new Entry(action, host);
        }

        List<Entry> history = Lists.newArrayList();

        public HistoryPolicy(LoadBalancingPolicy delegate) {
            super(delegate);
        }

        @Override
        public void init(Cluster cluster, Collection<Host> hosts) {
            super.init(cluster, hosts);
            for (Host host : hosts) {
                history.add(entry(INIT, host));
            }
        }

        public void onAdd(Host host) {
            history.add(entry(ADD, host));
            super.onAdd(host);
        }

        public void onSuspected(Host host) {
            history.add(entry(SUSPECT, host));
            super.onSuspected(host);
        }

        public void onUp(Host host) {
            history.add(entry(UP, host));
            super.onUp(host);
        }

        public void onDown(Host host) {
            history.add(entry(DOWN, host));
            super.onDown(host);
        }

        public void onRemove(Host host) {
            history.add(entry(REMOVE, host));
            super.onRemove(host);
        }
    }
}
