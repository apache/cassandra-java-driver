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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.Test;

import java.net.InetAddress;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;

public class HostMetadataIntegrationTest {

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
}
