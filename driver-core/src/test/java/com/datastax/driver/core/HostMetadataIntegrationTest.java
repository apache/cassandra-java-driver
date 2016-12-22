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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CCMAccess.Workload.solr;
import static com.datastax.driver.core.CCMAccess.Workload.spark;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.scassandra.http.client.types.ColumnMetadata.column;

public class HostMetadataIntegrationTest {

    /**
     * Validates that when given a DSE cluster that the workload and dse versions are properly
     * parsed and set on the respective {@link Host} instances.
     * <p/>
     * This test is disabled as running DSE with workloads is intensive and has unreliable results
     * when running CCM with multiple workloads.
     * <p/>
     * FIXME: this test is currently disabled because nodes with specific workload
     * do not seem to come up within the allocated time.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA1042
     * @see HostMetadataIntegrationTest#should_parse_dse_workload_and_version_if_available()
     */
    @Test(groups = "long", enabled = false)
    public void test_mixed_dse_workload() {
        CCMBridge.Builder builder = CCMBridge.builder()
                .withNodes(3)
                .withDSE()
                .withVersion("4.8.3")
                .withWorkload(2, solr)
                .withWorkload(3, spark);
        CCMAccess ccm = CCMCache.get(builder);

        VersionNumber version = VersionNumber.parse("4.8.3");

        Cluster cluster = Cluster.builder()
                .addContactPoints(ccm.addressOfNode(1).getAddress())
                .withPort(ccm.getBinaryPort())
                .build();
        try {
            cluster.connect();

            assertThat(cluster).host(1).hasWorkload("Cassandra").hasDseVersion(version);
            assertThat(cluster).host(2).hasWorkload("Search").hasDseVersion(version);
            assertThat(cluster).host(3).hasWorkload("Analytics").hasDseVersion(version);
        } finally {
            cluster.close();
            ccm.close();
        }
    }

    /**
     * Validates that {@link Host#getDseVersion()} and {@link Host#getDseWorkload()} return values defined in
     * the <code>dse_version</code> and <code>workload</code> columns if they are present in <code>system.local</code>
     * or <code>system.peers</code> otherwise they return null.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1042
     */
    @Test(groups = "short")
    public void should_parse_dse_workload_and_version_if_available() {
        // given: A 5 node cluster with all nodes having a workload and dse_version except node 2.
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(5)
                .forcePeerInfo(1, 1, "workload", "Analytics")
                .forcePeerInfo(1, 1, "dse_version", "4.8.4")
                .forcePeerInfo(1, 3, "workload", "Solr")
                .forcePeerInfo(1, 3, "dse_version", "4.8.4")
                .forcePeerInfo(1, 4, "workload", "Cassandra")
                .forcePeerInfo(1, 4, "dse_version", "4.8.4")
                .forcePeerInfo(1, 5, "workload", "AmazingNewFeature")
                .forcePeerInfo(1, 5, "dse_version", "5.0.0")
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            scassandraCluster.init();
            // when: initializing a cluster instance.
            cluster.init();

            // then: All nodes except node 2 should have a workload and dse version.
            assertThat(cluster).host(1).hasWorkload("Analytics").hasDseVersion(VersionNumber.parse("4.8.4"));
            assertThat(cluster).host(2).hasNoWorkload().hasNoDseVersion();
            assertThat(cluster).host(3).hasWorkload("Solr").hasDseVersion(VersionNumber.parse("4.8.4"));
            assertThat(cluster).host(4).hasWorkload("Cassandra").hasDseVersion(VersionNumber.parse("4.8.4"));
            assertThat(cluster).host(5).hasWorkload("AmazingNewFeature").hasDseVersion(VersionNumber.parse("5.0.0"));
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Validates that {@link Host#getDseVersion()} and {@link Host#getDseWorkload()} return null if
     * the <code>dse_version</code> and <code>workload</code> columns are not present in <code>system.local</code>
     * for the control host.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1042
     */
    @Test(groups = "short")
    public void should_not_parse_dse_workload_and_version_if_not_present_in_local_table() {
        // given: A cluster with node 1 (control host) not having a workload or dse_version set, and node 2 with those
        //        columns set.
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(2)
                .forcePeerInfo(1, 2, "workload", "Analytics")
                .forcePeerInfo(1, 2, "dse_version", "4.8.4")
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            scassandraCluster.init();
            // when: initializing a cluster instance.
            cluster.init();

            // then:
            //  - node 1 should have no workload or dse version.
            //  - node 2 should have a workload and a dse version.
            assertThat(cluster).host(1).hasNoWorkload().hasNoDseVersion();
            assertThat(cluster).host(2).hasWorkload("Analytics").hasDseVersion(VersionNumber.parse("4.8.4"));
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Validates that if <code>dse_version</code> column is a non-version value in <code>system.local</code> or
     * <code>system.peers</code> that a warning is logged and {@link Host#getDseVersion()} returns null.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1042
     */
    @Test(groups = "short")
    public void should_log_warning_when_invalid_version_used_for_dse_version() {
        // given: A cluster with a node that has an invalid version for dse_version in system.local.
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(1)
                .forcePeerInfo(1, 1, "workload", "Analytics")
                .forcePeerInfo(1, 1, "dse_version", "Invalid Version!")
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        MemoryAppender logs = new MemoryAppender();
        Logger logger = Logger.getLogger(Host.class);
        Level originalLoggerLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        logger.addAppender(logs);

        try {
            scassandraCluster.init();
            // when: initializing a cluster instance.
            cluster.init();

            // then: dse version for that host should not be set and a warning shall be logged.
            assertThat(logs.get())
                    .contains("Error parsing DSE version Invalid Version!. This shouldn't have happened");
            assertThat(cluster).host(1).hasNoDseVersion().hasWorkload("Analytics");
        } finally {
            logger.removeAppender(logs);
            logger.setLevel(originalLoggerLevel);
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Validates that {@link Host#isDseGraphEnabled()} returns the value defined in the <code>graph</code> columns if
     * it is present in <code>system.local</code> or <code>system.peers</code> otherwise it returns false.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1171
     */
    @Test(groups = "short")
    public void should_parse_dse_graph_if_available() {
        // given: A 3 node cluster with all nodes having a graph value except node 2.
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(3)
                .forcePeerInfo(1, 1, "graph", true)
                .forcePeerInfo(1, 3, "graph", false)
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            scassandraCluster.init();
            // when: initializing a cluster instance.
            cluster.init();

            // then:
            //  - node 1 should have graph.
            //  - node 2 and node 3 should not have graph.
            assertThat(cluster).host(1).hasDseGraph();
            assertThat(cluster).host(2).hasNoDseGraph();
            assertThat(cluster).host(3).hasNoDseGraph();
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Validates that {@link Host#isDseGraphEnabled()} returns false if the <code>graph</code> column is not present
     * in <code>system.local</code> for the control host.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1171
     */
    @Test(groups = "short")
    public void should_not_parse_dse_graph_if_not_present_in_local_table() {
        // given: A cluster with node 1 (control host) not having graph set, and node 2 with it set.
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(2)
                .forcePeerInfo(1, 2, "graph", true)
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            scassandraCluster.init();
            // when: initializing a cluster instance.
            cluster.init();

            // then:
            //  - node 1 should have no graph.
            //  - node 2 should have graph.
            assertThat(cluster).host(1).hasNoDseGraph();
            assertThat(cluster).host(2).hasDseGraph();
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Validates that {@link Host#getBroadcastAddress()} is set for all hosts (control host or not) in the default
     * case of a cluster, as the broadcast address is derived from <code>broadcast_address</code> in
     * <code>system.local</code> and <code>peer</code> in <code>system.peers</code>.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1035
     */
    @Test(groups = "short")
    public void should_set_broadcast_address_for_all_nodes() {
        // given: A cluster with 3 nodes.
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(3)
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            scassandraCluster.init();
            // when: initializing a cluster instance.
            cluster.init();

            // then: broadcast address should be set for each host:
            //  - broadcast_address is used for system.local and is always present, so control host should have it.
            //  - peer column is used for system.peers to resolve broadcast address and is always present.
            for (int i = 1; i <= scassandraCluster.nodes().size(); i++) {
                assertThat(cluster).host(i).hasBroadcastAddress(TestUtils.addressOfNode(i));
            }
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Validates that {@link Host#getListenAddress()} is set for a host if and only if the <code>listen_address</code>
     * column is present and set in the <code>system.peers</code> table or if the host is the control host.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1035
     */
    @Test(groups = "short")
    public void should_set_listen_address_if_available() {
        // given: A Cluster with 3 nodes and node 2 being configured with a listen address in its peers table.
        InetAddress listenAddress = TestUtils.addressOfNode(10);
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(3)
                .forcePeerInfo(1, 2, "listen_address", listenAddress)
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            // when: initializing a cluster instance.
            scassandraCluster.init();
            cluster.init();

            // then: listen address should be set appropriate for each host:
            // - Control Host will always have a listen address (listen_address should always be present).
            assertThat(cluster).host(1).hasListenAddress(TestUtils.addressOfNode(1));
            // - Host 2 should have a listen address since we provided one.
            assertThat(cluster).host(2).hasListenAddress(listenAddress);
            // - Host 3 should have no listen address as it wasn't provided.
            assertThat(cluster).host(3).hasNoListenAddress();
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }

    /**
     * Validates that when invoking {@link Metadata#getReplicas(String, TokenRange)} with a {@link TokenRange} that
     * spans multiple hosts and those hosts and all their replicas are included in the result.
     * <p>
     * Also ensures that if a {@link TokenRange} is given that covers a full host, that it only returns that host and
     * its replicas.
     * <p>
     * Lastly, ensures that is a {@link TokenRange} representing the entire ring is provided as input, that all hosts
     * are returned.
     *
     * @test_category host:metadata
     * @jira_ticket JAVA-1355.
     */
    @Test(groups = "short")
    public void should_return_all_replicas_for_range_over_multiple_hosts() {
        // Set up a 6 node cluster..
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(6)
                .build();

        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            scassandraCluster.init();

            // Create ksX for X=1,2,6 where RF=X.
            List<Map<String, ? extends Object>> ksRows = Lists.newArrayListWithExpectedSize(3);
            for (Integer rf : Lists.newArrayList(1, 2, 6)) {
                ksRows.add(ImmutableMap.<String, Object>builder()
                        .put("keyspace_name", "rf" + rf)
                        .put("durable_writes", true)
                        .put("strategy_class", "SimpleStrategy")
                        .put("strategy_options", "{\"replication_factor\":\"" + rf + "\"}")
                        .build());
            }

            // Prime keyspace query, note that this is only applicable for C* < 2.2.
            scassandraCluster.node(1, 1).primingClient()
                    .prime(PrimingRequest.queryBuilder().withQuery("SELECT * FROM system.schema_keyspaces")
                            .withThen(PrimingRequest.then().withColumnTypes(
                                    column("keyspace_name", PrimitiveType.TEXT),
                                    column("durable_writes", PrimitiveType.BOOLEAN),
                                    column("strategy_class", PrimitiveType.TEXT),
                                    column("strategy_options", PrimitiveType.TEXT))
                                    .withRows(ksRows)));

            cluster.init();

            // Expected token ranges:
            // /127.0.1.1 []7686143364045646505, 0]]
            // /127.0.1.2 []0, 1537228672809129301]]
            // /127.0.1.3 []1537228672809129301, 3074457345618258602]]
            // /127.0.1.4 []3074457345618258602, 4611686018427387903]]
            // /127.0.1.5 []4611686018427387903, 6148914691236517204]]
            // /127.0.1.6 []6148914691236517204, 7686143364045646505]]

            Metadata metadata = cluster.getMetadata();

            // A token somewhere in the primary range belonging to host2.
            Token r2Token = metadata.tokenFactory().fromString("1037228672809129301");
            // A token somewhere in the primary range belonging to host3.
            Token r3Token = metadata.tokenFactory().fromString("2074457345618258602");
            // A range starting somewhere in r2's primary range and ending somewhere in r3's primary range.
            TokenRange r2r3Range = metadata.newTokenRange(r2Token, r3Token);

            Host host1 = scassandraCluster.host(cluster, 1, 1);
            Host host2 = scassandraCluster.host(cluster, 1, 2);
            Host host3 = scassandraCluster.host(cluster, 1, 3);
            Host host4 = scassandraCluster.host(cluster, 1, 4);

            // With RF = 1, we expect host2 and host3 to be returned since the range spans host2 and host3.
            assertThat(metadata.getReplicas("rf1", r2r3Range))
                    .containsOnly(host2, host3);

            // With RF = 2, we expect host2, 3 and 4 to be returned since the range spans host2 and host3 and host4
            // is a replica of host3.
            assertThat(metadata.getReplicas("rf2", r2r3Range))
                    .containsOnly(host2, host3, host4);

            // With RF = 6, we expect all hosts to be returned.
            assertThat(metadata.getReplicas("rf6", r2r3Range)).containsAll(metadata.getAllHosts());

            // Validate that behavior prior to JAVA-1355 is maintained if using a host's range.
            TokenRange host1Range = metadata.getTokenRanges("rf1", host1).iterator().next();
            // With RF = 1, we expect only host 1.
            assertThat(metadata.getReplicas("rf1", host1Range)).containsOnly(host1);
            // With RF = 2, we expect host1 and it's replica, host2.
            assertThat(metadata.getReplicas("rf2", host1Range)).containsOnly(host1, host2);
            // With RF = 6, we expect all hosts.
            assertThat(metadata.getReplicas("rf6", host1Range)).containsAll(metadata.getAllHosts());

            // With a range covering entire ring, expect all hosts to be returned.
            TokenRange ring = metadata.newTokenRange(metadata.tokenFactory().minToken(), metadata.tokenFactory().minToken());
            assertThat(metadata.getReplicas("rf1", ring).containsAll(metadata.getAllHosts()));
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }
}
