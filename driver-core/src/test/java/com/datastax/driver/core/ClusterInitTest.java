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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.scassandra.Scassandra;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.utils.CassandraVersion;

import static com.datastax.driver.core.Assertions.*;
import static com.datastax.driver.core.FakeHost.Behavior.THROWING_CONNECT_TIMEOUTS;
import static com.datastax.driver.core.Host.State.DOWN;
import static com.datastax.driver.core.Host.State.UP;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;

public class ClusterInitTest {
    private static final Logger logger = LoggerFactory.getLogger(ClusterInitTest.class);

    /**
     * Test for JAVA-522: when the cluster and session initialize, if some contact points are behaving badly and
     * causing timeouts, we want to ensure that the driver does not wait multiple times on the same host.
     */
    @Test(groups = "short")
    @CassandraVersion(major=2.0, description = "Scassandra currently broken with protocol version 1.")
    public void should_handle_failing_or_missing_contact_points() throws UnknownHostException {
        if (TestUtils.getDesiredProtocolVersion().compareTo(ProtocolVersion.V2) > 0)
            throw new SkipException("Scassandra does not yet support protocol >= V3");

        Cluster cluster = null;
        Scassandra scassandra = null;
        List<FakeHost> failingHosts = Lists.newArrayList();
        try {
            // Simulate a cluster of 5 hosts.

            // - 1 is an actual Scassandra instance that will accept connections:
            scassandra = TestUtils.createScassandraServer();
            scassandra.start();
            String realHostAddress = "127.0.0.1";
            int port = scassandra.getBinaryPort();

            // - the remaining 4 are fake servers that will throw connect timeouts:
            for (int i = 2; i <= 5; i++) {
                FakeHost failingHost = new FakeHost(CCMBridge.ipOfNode(i), port, THROWING_CONNECT_TIMEOUTS);
                failingHosts.add(failingHost);
                failingHost.start();
            }

            // - we also have a "missing" contact point, i.e. there's no server listening at this address,
            //   and the address is not listed in the live host's system.peers
            String missingHostAddress = CCMBridge.ipOfNode(6);

            primePeerRows(scassandra, failingHosts);

            logger.info("Environment is set up, starting test");
            long start = System.nanoTime();

            // We want to count how many connections were attempted. For that, we rely on the fact that SocketOptions.getKeepAlive
            // is called in Connection.Factory.newBoostrap() each time we prepare to open a new connection.
            SocketOptions socketOptions = spy(new SocketOptions());

            // Set an "infinite" reconnection delay so that reconnection attempts don't pollute our observations
            ConstantReconnectionPolicy reconnectionPolicy = new ConstantReconnectionPolicy(3600 * 1000);

            // Force 1 connection per pool. Otherwise we can't distinguish a failed pool creation from multiple connection
            // attempts, because pools create their connections in parallel (so 1 pool failure equals multiple connection failures).
            PoolingOptions poolingOptions = new PoolingOptions().setConnectionsPerHost(LOCAL, 1, 1);

            cluster = Cluster.builder()
                .withPort(scassandra.getBinaryPort())
                .addContactPoints(
                    realHostAddress,
                    failingHosts.get(0).address, failingHosts.get(1).address,
                    failingHosts.get(2).address, failingHosts.get(3).address,
                    missingHostAddress
                )
                .withSocketOptions(socketOptions)
                .withReconnectionPolicy(reconnectionPolicy)
                .withPoolingOptions(poolingOptions)
                .withProtocolVersion(TestUtils.getDesiredProtocolVersion())
                .withQueryOptions(nonDebouncingQueryOptions())
                .build();
            cluster.connect();

            // For information only:
            long initTimeMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            logger.info("Cluster and session initialized in {} ms", initTimeMs);

            // Expect :
            // - 2 connections for the live host (1 control connection + 1 pooled connection)
            // - 1 attempt per failing host (either a control connection attempt or a failed pool creation)
            // - 0 or 1 for the missing host. We can't know for sure because contact points are randomized. If it's tried
            //   before the live host there will be a connection attempt, otherwise it will be removed directly because
            //   it's not in the live host's system.peers.
            verify(socketOptions, atLeast(6)).getKeepAlive();
            verify(socketOptions, atMost(7)).getKeepAlive();

            assertThat(cluster).host(realHostAddress).isNotNull().hasState(UP);
            for (FakeHost failingHost : failingHosts)
                assertThat(cluster).host(failingHost.address).hasState(DOWN);
            assertThat(cluster).host(missingHostAddress).isNull();

        } finally {
            if (cluster != null)
                cluster.close();
            for (FakeHost fakeHost : failingHosts)
                fakeHost.stop();
            if (scassandra != null)
                scassandra.stop();
        }
    }

    /**
     * Validates that a Cluster that was never able to successfully establish connection a session can be closed
     * properly.
     *
     * @test_category connection
     * @expected_result Cluster closes within 1 second.
     */
    @Test(groups = "unit")
    public void should_be_able_to_close_cluster_that_never_successfully_connected() throws Exception {
        Cluster cluster = Cluster.builder()
            .addContactPointsWithPorts(Collections.singleton(new InetSocketAddress("127.0.0.1", 65534)))
            .build();
        try {
            cluster.connect();
            fail("Should not have been able to connect.");
        } catch (NoHostAvailableException e) {
        } // Expected.
        CloseFuture closeFuture = cluster.closeAsync();
        try {
            closeFuture.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            fail("Close Future did not complete quickly.");
        }
    }

    private void primePeerRows(Scassandra scassandra, List<FakeHost> otherHosts) throws UnknownHostException {
        PrimingClient primingClient = PrimingClient.builder()
            .withHost("localhost").withPort(scassandra.getAdminPort())
            .build();

        List<Map<String, ?>> rows = Lists.newArrayListWithCapacity(5);

        int i = 0;
        for (FakeHost otherHost : otherHosts) {
            InetAddress address = InetAddress.getByName(otherHost.address);
            rows.add(ImmutableMap.<String, Object>builder()
                .put("peer", address)
                .put("rpc_address", address)
                .put("data_center", "datacenter1")
                .put("rack", "rack1")
                .put("release_version", "2.0.1")
                .put("tokens", ImmutableSet.of(Long.toString(Long.MIN_VALUE + i++)))
                .build());
        }

        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.peers")
                .withColumnTypes(SCassandraCluster.SELECT_PEERS_COLUMN_TYPES)
                .withRows(rows)
                .build());
    }
}
