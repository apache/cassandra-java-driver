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

import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Function;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ITestResult;
import org.testng.annotations.*;

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CCMTestMode.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singleton;

@CCMTestMode(PER_METHOD)
@CCMConfig(dirtiesContext = true, createCluster = false)
public class ControlConnectionTest {

    static final Logger logger = LoggerFactory.getLogger(ControlConnectionTest.class);

    private final CCMTestsSupport ccmSupport = new CCMTestsSupport();

    @BeforeClass(groups = "short")
    public void beforeTestClass() throws Exception {
        ccmSupport.beforeTestClass(this);
    }

    @BeforeMethod(groups = "short")
    public void beforeTestMethod(Method testMethod) throws Exception {
        ccmSupport.beforeTestMethod(this, testMethod);
    }

    @AfterMethod(groups = "short", alwaysRun = true)
    public void afterTestMethod(ITestResult tr) {
        ccmSupport.afterTestMethod(tr);
    }

    @AfterClass(groups = "short", alwaysRun = true)
    public void afterTestClass() {
        ccmSupport.afterTestClass();
    }

    @SuppressWarnings("unused")
    public CCMBridge.Builder createCCMBridgeBuilder() {
        return ccmSupport.createCCMBridgeBuilder();
    }

    @Test(groups = "short")
    @CCMConfig(numberOfNodes = 2)
    public void should_prevent_simultaneous_reconnection_attempts() throws InterruptedException {
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
            // We pass only the first host as contact point, so we know the control connection will be on this host
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(singleton(ccmSupport.getContactPoints().get(0)))
                    .withAddressTranslater(ccmSupport.getAddressTranslator())
                    .withReconnectionPolicy(reconnectionPolicy)
                    .withLoadBalancingPolicy(loadBalancingPolicy)
                    .build();
            cluster.init();

            // Kill the control connection host, there should be exactly one reconnection attempt
            ccmSupport.ccmBridge().stop(1);
            TimeUnit.SECONDS.sleep(1); // Sleep for a while to make sure our final count is not the result of lucky timing
            assertThat(reconnectionAttempts.get()).isEqualTo(1);

            ccmSupport.ccmBridge().stop(2);
            TimeUnit.SECONDS.sleep(1);
            assertThat(reconnectionAttempts.get()).isEqualTo(2);

        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    /**
     * Test for JAVA-509: UDT definitions were not properly parsed when using the default protocol version.
     * <p/>
     * This did not appear with other tests because the UDT needs to already exist when the driver initializes.
     * Therefore we use two different driver instances in this test.
     */
    @Test(groups = "short")
    @CassandraVersion(major = 2.1)
    public void should_parse_UDT_definitions_when_using_default_protocol_version() {
        Cluster cluster = null;
        InetSocketAddress firstHost = ccmSupport.getContactPoints().get(0);

        try {

            // First driver instance: create UDT
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(singleton(firstHost))
                    .withAddressTranslater(ccmSupport.getAddressTranslator())
                    .build();
            Session session = cluster.connect();
            session.execute("create keyspace ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("create type ks.foo (i int)");
            cluster.close();

            // Second driver instance: read UDT definition
            cluster = Cluster.builder().addContactPointsWithPorts(singleton(firstHost)).build();
            UserType fooType = cluster.getMetadata().getKeyspace("ks").getUserType("foo");

            assertThat(fooType.getFieldNames()).containsExactly("i");
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    /**
     * Ensures that if the host that the Control Connection is connected to is removed/decommissioned that the
     * Control Connection is reestablished to another host.
     *
     * @jira_ticket JAVA-597
     * @expected_result Control Connection is reestablished to another host.
     * @test_category control_connection
     * @since 2.0.9
     */
    @Test(groups = "long")
    @CCMConfig(numberOfNodes = 3)
    public void should_reestablish_if_control_node_decommissioned() throws InterruptedException {
        Cluster cluster = null;

        try {

            InetSocketAddress firstHost = ccmSupport.getContactPoints().get(0);
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(singleton(firstHost))
                    .withAddressTranslater(ccmSupport.getAddressTranslator())
                    .withQueryOptions(nonDebouncingQueryOptions())
                    .build();
            cluster.init();

            // Ensure the control connection host is that of the first node.
            InetAddress controlHost = cluster.manager.controlConnection.connectedHost().getAddress();
            assertThat(controlHost).isEqualTo(firstHost.getAddress());

            // Decommission the node.
            ccmSupport.ccmBridge().decommissionNode(1);

            // Ensure that the new control connection is not null and it's host is not equal to the decommissioned node.
            Host newHost = cluster.manager.controlConnection.connectedHost();
            assertThat(newHost).isNotNull();
            assertThat(newHost.getAddress()).isNotEqualTo(controlHost);
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }

    /**
     * <p>
     * Ensures that contact points are randomized when determining the initial control connection
     * by default.  Initializes a cluster with 5 contact points 100 times and ensures that all 5
     * were used.
     * </p>
     *
     * @jira_ticket JAVA-618
     * @expected_result All 5 hosts were chosen within 100 attempts.  There is a very small possibility
     * that this may not be the case and this is not actually an error.
     * @test_category control_connection
     * @since 2.0.11, 2.1.8, 2.2.0
     */
    @Test(groups = "short")
    @CCMConfig(createCcm = false)
    public void should_randomize_contact_points_when_determining_control_connection() {
        int hostCount = 5;
        int iterations = 100;
        ScassandraCluster scassandras = ScassandraCluster.builder().withNodes(hostCount).build();
        scassandras.init();

        try {
            Collection<String> contactPoints = newArrayList();
            for (int i = 1; i <= hostCount; i++) {
                contactPoints.add(scassandras.address(i));
            }
            final HashMultiset<InetAddress> occurrencesByHost = HashMultiset.create(hostCount);
            for (int i = 0; i < iterations; i++) {
                Cluster cluster = Cluster.builder()
                        .addContactPoints(contactPoints.toArray(new String[hostCount]))
                        .withNettyOptions(nonQuietClusterCloseOptions)
                        .build();

                try {
                    cluster.init();
                    occurrencesByHost.add(cluster.manager.controlConnection.connectedHost().getAddress());
                } finally {
                    cluster.close();
                }
            }
            if (logger.isDebugEnabled()) {
                Map<InetAddress, Integer> hostCounts = Maps.toMap(occurrencesByHost.elementSet(), new Function<InetAddress, Integer>() {
                    @Override
                    public Integer apply(InetAddress input) {
                        return occurrencesByHost.count(input);
                    }
                });
                logger.debug("Control Connection Use Counts by Host: {}", hostCounts);
            }
            // There is an incredibly low chance that a host may not be used based on randomness.
            // This probability is very low however.
            assertThat(occurrencesByHost.elementSet().size())
                    .as("Not all hosts were used as contact points.  There is a very small chance"
                            + " of this happening based on randomness, investigate whether or not this"
                            + " is a bug.")
                    .isEqualTo(hostCount);
        } finally {
            scassandras.stop();
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
