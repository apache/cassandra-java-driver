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

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Function;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.log4j.Level;
import org.scassandra.http.client.PrimingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.ScassandraCluster.SELECT_PEERS;
import static com.datastax.driver.core.ScassandraCluster.datacenter;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static com.google.common.collect.Lists.newArrayList;
import static org.scassandra.http.client.PrimingRequest.then;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true, createCluster = false)
public class ControlConnectionTest extends CCMTestsSupport {

    static final Logger logger = LoggerFactory.getLogger(ControlConnectionTest.class);

    @Test(groups = "short")
    @CCMConfig(numberOfNodes = 2)
    public void should_prevent_simultaneous_reconnection_attempts() throws InterruptedException {

        // Custom load balancing policy that counts the number of calls to newQueryPlan().
        // Since we don't open any session from our Cluster, only the control connection reattempts are calling this
        // method, therefore the invocation count is equal to the number of attempts.
        QueryPlanCountingPolicy loadBalancingPolicy = new QueryPlanCountingPolicy(Policies.defaultLoadBalancingPolicy());
        AtomicInteger reconnectionAttempts = loadBalancingPolicy.counter;

        // Custom reconnection policy with a very large delay (longer than the test duration), to make sure we count
        // only the first reconnection attempt of each reconnection handler.
        ReconnectionPolicy reconnectionPolicy = new ConstantReconnectionPolicy(60 * 1000);

        // We pass only the first host as contact point, so we know the control connection will be on this host
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withReconnectionPolicy(reconnectionPolicy)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build());
        cluster.init();

        // Kill the control connection host, there should be exactly one reconnection attempt
        ccm().stop(1);
        TimeUnit.SECONDS.sleep(1); // Sleep for a while to make sure our final count is not the result of lucky timing
        assertThat(reconnectionAttempts.get()).isEqualTo(1);

        ccm().stop(2);
        TimeUnit.SECONDS.sleep(1);
        assertThat(reconnectionAttempts.get()).isEqualTo(2);
    }

    /**
     * Test for JAVA-509: UDT definitions were not properly parsed when using the default protocol version.
     * <p/>
     * This did not appear with other tests because the UDT needs to already exist when the driver initializes.
     * Therefore we use two different driver instances in this test.
     */
    @Test(groups = "short")
    @CassandraVersion("2.1.0")
    public void should_parse_UDT_definitions_when_using_default_protocol_version() {
        // First driver instance: create UDT
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        Session session = cluster.connect();
        session.execute("create keyspace ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session.execute("create type ks.foo (i int)");
        cluster.close();

        // Second driver instance: read UDT definition
        Cluster cluster2 = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .build());
        UserType fooType = cluster2.getMetadata().getKeyspace("ks").getUserType("foo");

        assertThat(fooType.getFieldNames()).containsExactly("i");
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
        InetSocketAddress firstHost = ccm().addressOfNode(1);
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(firstHost.getAddress())
                .withPort(ccm().getBinaryPort())
                .withQueryOptions(nonDebouncingQueryOptions())
                .build());
        cluster.init();

        // Ensure the control connection host is that of the first node.
        InetAddress controlHost = cluster.manager.controlConnection.connectedHost().getAddress();
        assertThat(controlHost).isEqualTo(firstHost.getAddress());

        // Decommission the node.
        ccm().decommission(1);

        // Ensure that the new control connection is not null and it's host is not equal to the decommissioned node.
        Host newHost = cluster.manager.controlConnection.connectedHost();
        assertThat(newHost).isNotNull();
        assertThat(newHost.getAddress()).isNotEqualTo(controlHost);
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
            Collection<InetAddress> contactPoints = newArrayList();
            for (int i = 1; i <= hostCount; i++) {
                contactPoints.add(scassandras.address(i).getAddress());
            }
            final HashMultiset<InetAddress> occurrencesByHost = HashMultiset.create(hostCount);
            for (int i = 0; i < iterations; i++) {
                Cluster cluster = Cluster.builder()
                        .addContactPoints(contactPoints)
                        .withPort(scassandras.getBinaryPort())
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

    @DataProvider
    public static Object[][] disallowedNullColumnsInPeerData() {
        return new Object[][]{
                {"host_id"},
                {"data_center"},
                {"rack"},
                {"tokens"},
                {"host_id,data_center,rack,tokens"}
        };
    }

    /**
     * Validates that if the com.datastax.driver.EXTENDED_PEER_CHECK system property is set to true that a peer
     * with null values for host_id, data_center, rack, or tokens is ignored.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-852
     * @since 2.1.10
     */
    @Test(groups = "isolated", dataProvider = "disallowedNullColumnsInPeerData")
    @CCMConfig(createCcm = false)
    public void should_ignore_peer_if_extended_peer_check_is_enabled(String columns) {
        System.setProperty("com.datastax.driver.EXTENDED_PEER_CHECK", "true");
        run_with_null_peer_info(columns, false);
    }

    /**
     * Validates that a peer with null values for host_id, data_center, rack, or tokens is ignored.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-852
     * @since 2.1.10
     */
    @Test(groups = "short", dataProvider = "disallowedNullColumnsInPeerData")
    @CCMConfig(createCcm = false)
    public void should_ignore_and_warn_peers_with_null_entries_by_default(String columns) {
        run_with_null_peer_info(columns, false);
    }

    static void run_with_null_peer_info(String columns, boolean expectPeer2) {
        // given: A cluster with peer 2 having a null rack.
        ScassandraCluster.ScassandraClusterBuilder builder = ScassandraCluster.builder()
                .withNodes(3);

        StringBuilder columnDataBuilder = new StringBuilder();
        for (String column : columns.split(",")) {
            builder = builder.forcePeerInfo(1, 2, column, null);
            columnDataBuilder.append(String.format("%s=null, ", column));
        }

        String columnData = columnDataBuilder.toString();
        if (columnData.endsWith(", ")) {
            columnData = columnData.substring(0, columnData.length() - 2);
        }

        ScassandraCluster scassandraCluster = builder.build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        // Capture logs to ensure appropriate warnings are logged.
        org.apache.log4j.Logger cLogger = org.apache.log4j.Logger.getLogger("com.datastax.driver.core");
        Level originalLevel = cLogger.getLevel();
        if (originalLevel != null && !originalLevel.isGreaterOrEqual(Level.WARN)) {
            cLogger.setLevel(Level.WARN);
        }
        MemoryAppender logs = new MemoryAppender();
        cLogger.addAppender(logs);

        try {
            scassandraCluster.init();

            // when: Initializing a cluster instance and grabbing metadata.
            cluster.init();

            InetAddress node2Address = scassandraCluster.address(2).getAddress();
            String expectedError = String.format("Found invalid row in system.peers: [peer=%s, %s]. " +
                    "This is likely a gossip or snitch issue, this host will be ignored.", node2Address, columnData);
            String log = logs.get();
            // then: A peer with a null rack should not show up in host metadata, unless allowed via system property.
            if (expectPeer2) {
                assertThat(cluster.getMetadata().getAllHosts())
                        .hasSize(3)
                        .extractingResultOf("getAddress")
                        .contains(node2Address);

                assertThat(log).doesNotContain(expectedError);
            } else {
                assertThat(cluster.getMetadata().getAllHosts())
                        .hasSize(2)
                        .extractingResultOf("getAddress")
                        .doesNotContain(node2Address);

                assertThat(log)
                        .containsOnlyOnce(expectedError);
            }
        } finally {
            cLogger.removeAppender(logs);
            cLogger.setLevel(originalLevel);
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Ensures that when a node changes its broadcast address (for example, after
     * a shutdown and startup on EC2 and its public IP has changed),
     * the driver will be able to detect that change and recognize the host
     * in the system.peers table in spite of that change.
     *
     * @jira_ticket JAVA-1038
     * @expected_result The driver should be able to detect that a host has changed its broadcast address
     * and update its metadata accordingly.
     * @test_category control_connection
     * @since 2.1.10
     */
    @SuppressWarnings("unchecked")
    @Test(groups = "short")
    @CCMConfig(createCcm = false)
    public void should_fetch_whole_peers_table_if_broadcast_address_changed() throws UnknownHostException {
        ScassandraCluster scassandras = ScassandraCluster.builder().withNodes(2).build();
        scassandras.init();

        InetSocketAddress node2RpcAddress = scassandras.address(2);

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandras.address(1).getAddress())
                .withPort(scassandras.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {

            cluster.init();

            Host host2 = cluster.getMetadata().getHost(node2RpcAddress);
            assertThat(host2).isNotNull();

            InetAddress node2OldBroadcastAddress = host2.getBroadcastAddress();
            InetAddress node2NewBroadcastAddress = InetAddress.getByName("1.2.3.4");

            // host 2 has the old broadcast_address (which is identical to its rpc_broadcast_address)
            assertThat(host2.getAddress())
                    .isEqualTo(node2OldBroadcastAddress);

            // simulate a change in host 2 public IP
            Map<String, ?> rows = ImmutableMap.<String, Object>builder()
                    .put("peer", node2NewBroadcastAddress) // new broadcast address for host 2
                    .put("rpc_address", host2.getAddress()) // rpc_broadcast_address remains unchanged
                    .put("host_id", UUID.randomUUID())
                    .put("data_center", datacenter(1))
                    .put("rack", "rack1")
                    .put("release_version", "2.1.8")
                    .put("tokens", ImmutableSet.of(Long.toString(scassandras.getTokensForDC(1).get(1))))
                    .build();

            scassandras.node(1).primingClient().clearAllPrimes();

            // the driver will attempt to locate host2 in system.peers by its old broadcast address, and that will fail
            scassandras.node(1).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("SELECT * FROM system.peers WHERE peer='" + node2OldBroadcastAddress + "'")
                    .withThen(then()
                            .withColumnTypes(SELECT_PEERS)
                            .build())
                    .build());

            // the driver will then attempt to fetch the whole system.peers
            scassandras.node(1).primingClient().prime(PrimingRequest.queryBuilder()
                    .withQuery("SELECT * FROM system.peers")
                    .withThen(then()
                            .withColumnTypes(SELECT_PEERS)
                            .withRows(rows)
                            .build())
                    .build());

            assertThat(cluster.manager.controlConnection.refreshNodeInfo(host2)).isTrue();

            host2 = cluster.getMetadata().getHost(node2RpcAddress);

            // host2 should now have a new broadcast address
            assertThat(host2).isNotNull();
            assertThat(host2.getBroadcastAddress())
                    .isEqualTo(node2NewBroadcastAddress);

            // host 2 should keep its old rpc broadcast address
            assertThat(host2.getSocketAddress())
                    .isEqualTo(node2RpcAddress);

        } finally {
            cluster.close();
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
