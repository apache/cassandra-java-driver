/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.utils.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;

import static com.datastax.driver.core.LoadBalancingPolicyBootstrapTest.HistoryPolicy.Action.*;
import static com.datastax.driver.core.LoadBalancingPolicyBootstrapTest.HistoryPolicy.entry;
import static org.assertj.core.api.Assertions.assertThat;

@CCMConfig(numberOfNodes = 2, dirtiesContext = true, createCluster = false)
public class LoadBalancingPolicyBootstrapTest extends CCMTestsSupport {

    private static final Logger logger = LoggerFactory.getLogger(LoadBalancingPolicyBootstrapTest.class);

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

        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withLoadBalancingPolicy(policy)
                .build());

        try {
            cluster.init();

            assertThat(policy.history).containsOnly(
                    entry(INIT, TestUtils.findHost(cluster, 1)),
                    entry(INIT, TestUtils.findHost(cluster, 2))
            );
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that {@link LoadBalancingPolicy#onDown(Host)} is called for a contact point that couldn't
     * be reached while initializing the control connection, but only after
     * {@link LoadBalancingPolicy#init(Cluster, Collection)} has been called.
     *
     * @test_category load_balancing:notification
     * @expected_result init() is called with the up host, followed by onDown() for the downed host.
     * @jira_ticket JAVA-613
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "short", dependsOnMethods = "should_init_policy_with_up_contact_points")
    public void should_send_down_notifications_after_init_when_contact_points_are_down() throws Exception {

        // In order to validate this behavior, we need to stop the first node that would be attempted to be
        // established as the control connection.
        // This depends on the internal behavior and will even be made totally random by JAVA-618, therefore
        // we retry the scenario until we get the desired preconditions.

        int nodeToStop = 1;
        int tries = 1, maxTries = 10;
        for (; tries <= maxTries; tries++) {
            nodeToStop = (nodeToStop == 1) ? 2 : 1; // switch nodes at each try
            int activeNode = nodeToStop == 2 ? 1 : 2;

            ccm().stop(nodeToStop);
            ccm().waitForDown(nodeToStop);

            HistoryPolicy policy = new HistoryPolicy(new RoundRobinPolicy());
            Cluster cluster = register(Cluster.builder()
                    .addContactPoints(getContactPoints())
                    .withPort(ccm().getBinaryPort())
                    .withLoadBalancingPolicy(policy)
                    .build());

            try {
                cluster.init();

                if (policy.history.contains(entry(DOWN, TestUtils.findHost(cluster, nodeToStop)))) {
                    // This is the situation we're testing, the control connection tried the stopped node first.
                    assertThat(policy.history).containsExactly(
                            entry(INIT, TestUtils.findHost(cluster, activeNode)),
                            entry(DOWN, TestUtils.findHost(cluster, nodeToStop))
                    );
                    break;
                } else {
                    assertThat(policy.history).containsOnly(
                            entry(INIT, TestUtils.findHost(cluster, 1)),
                            entry(INIT, TestUtils.findHost(cluster, 2))
                    );

                    logger.info("Could not get first contact point to fail, retrying");

                    cluster.close();

                    ccm().start(nodeToStop);
                    ccm().waitForUp(nodeToStop);
                }
            } finally {
                cluster.close();
            }
        }

        if (tries == maxTries + 1)
            logger.warn("Could not get first contact point to fail after {} tries", maxTries);
    }

    static class HistoryPolicy extends DelegatingLoadBalancingPolicy {
        enum Action {INIT, UP, DOWN, ADD, REMOVE}

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
                    Entry that = (Entry) other;
                    return this.action == that.action && this.host.equals(that.host);
                }
                return false;
            }

            @Override
            public int hashCode() {
                return MoreObjects.hashCode(action, host);
            }

            @Override
            public String toString() {
                return String.format("Entry(action=%s, host=%s)", action, host);
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
