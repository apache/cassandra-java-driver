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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import org.scassandra.Scassandra;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.Assertions.fail;
import static com.datastax.driver.core.FakeHost.Behavior.THROWING_CONNECT_TIMEOUTS;
import static com.datastax.driver.core.HostDistance.LOCAL;
import static com.datastax.driver.core.TestUtils.ipOfNode;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static org.mockito.Mockito.*;
import static org.scassandra.http.client.PrimingRequest.then;

public class ClusterInitTest {
    private static final Logger logger = LoggerFactory.getLogger(ClusterInitTest.class);

    /**
     * Test for JAVA-522: when the cluster and session initialize, if some contact points are behaving badly and
     * causing timeouts, we want to ensure that the driver does not wait multiple times on the same host.
     */
    @Test(groups = "short")
    public void should_handle_failing_or_missing_contact_points() throws UnknownHostException {
        Cluster cluster = null;
        Scassandra scassandra = null;
        List<FakeHost> failingHosts = Lists.newArrayList();
        try {
            // Simulate a cluster of 5 hosts.

            // - 1 is an actual Scassandra instance that will accept connections:
            scassandra = TestUtils.createScassandraServer();
            scassandra.start();
            int port = scassandra.getBinaryPort();

            // - the remaining 4 are fake servers that will throw connect timeouts:
            for (int i = 2; i <= 5; i++) {
                FakeHost failingHost = new FakeHost(TestUtils.ipOfNode(i), port, THROWING_CONNECT_TIMEOUTS);
                failingHosts.add(failingHost);
                failingHost.start();
            }

            // - we also have a "missing" contact point, i.e. there's no server listening at this address,
            //   and the address is not listed in the live host's system.peers
            String missingHostAddress = TestUtils.ipOfNode(6);

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
                            ipOfNode(1),
                            failingHosts.get(0).address, failingHosts.get(1).address,
                            failingHosts.get(2).address, failingHosts.get(3).address,
                            missingHostAddress
                    )
                    .withSocketOptions(socketOptions)
                    .withReconnectionPolicy(reconnectionPolicy)
                    .withPoolingOptions(poolingOptions)
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

            assertThat(cluster).host(1).isNotNull().isUp();
            // It is likely but not guaranteed that a host is marked down at this point.
            // It should eventually be marked down as Cluster.Manager.triggerOnDown should be
            // called and submit a task that marks the host down.
            for (FakeHost failingHost : failingHosts) {
                assertThat(cluster).host(failingHost.address).goesDownWithin(10, TimeUnit.SECONDS);
                Host host = TestUtils.findHost(cluster, failingHost.address);
                // There is a possible race here in that the host is marked down in a separate Executor in onDown
                // and then starts a periodic reconnection attempt shortly after.  Since setDown is called before
                // startPeriodicReconnectionAttempt, we add a slight delay here if the future isn't set yet.
                if (host != null && (host.getReconnectionAttemptFuture() == null || host.getReconnectionAttemptFuture().isDone())) {
                    logger.warn("Periodic Reconnection Attempt hasn't started yet for {}, waiting 1 second and then checking.", host);
                    Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                }
                assertThat(cluster).host(failingHost.address).isReconnectingFromDown();
            }
            assertThat(TestUtils.findHost(cluster, missingHostAddress)).isNull();
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
     * Validates that if hosts are unreachable during Cluster initialization, no background reconnection to them
     * is scheduled before the initialization is complete.
     *
     * @test_category connection
     * @jira_ticket JAVA-954
     * @expected_result No reconnection scheduled.
     */
    @Test(groups = "short", expectedExceptions = NoHostAvailableException.class)
    public void should_not_schedule_reconnections_before_init_complete() {
        // Both contact points time out so we're sure we'll try both of them and init will never complete.
        List<FakeHost> hosts = Lists.newArrayList(
                new FakeHost(TestUtils.ipOfNode(0), 9042, THROWING_CONNECT_TIMEOUTS),
                new FakeHost(TestUtils.ipOfNode(1), 9042, THROWING_CONNECT_TIMEOUTS));
        // Use a low reconnection interval and keep the default connect timeout (5 seconds). So if a reconnection was scheduled,
        // we would see a call to the reconnection policy.
        CountingReconnectionPolicy reconnectionPolicy = new CountingReconnectionPolicy(new ConstantReconnectionPolicy(100));
        Cluster cluster = Cluster.builder()
                .addContactPoints(hosts.get(0).address, hosts.get(1).address)
                .withReconnectionPolicy(reconnectionPolicy)
                .build();
        try {
            cluster.init();
        } finally {
            // We expect a nextDelay invocation from the ConvictionPolicy for each host, but that will
            // not trigger a reconnection.
            assertThat(reconnectionPolicy.count.get()).isEqualTo(2);
            for (FakeHost fakeHost : hosts) {
                fakeHost.stop();
            }
            cluster.close();
        }
        // We don't test that reconnections are scheduled if init succeeds, but that's covered in
        // should_handle_failing_or_missing_contact_points
    }

    /**
     * Validates that a Cluster that was never able to successfully establish connection a session can be closed
     * properly.
     *
     * @test_category connection
     * @expected_result Cluster closes within 1 second.
     */
    @Test(groups = "short")
    public void should_be_able_to_close_cluster_that_never_successfully_connected() throws Exception {
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(new InetSocketAddress("127.0.0.1", 65534))
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();
        try {
            cluster.connect();
            fail("Should not have been able to connect.");
        } catch (NoHostAvailableException e) {
            // Expected.
            CloseFuture closeFuture = cluster.closeAsync();
            try {
                closeFuture.get(1, TimeUnit.SECONDS);
            } catch (TimeoutException e1) {
                fail("Close Future did not complete quickly.");
            }
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that if a node is detected that does not support the protocol version in use on init that
     * the node is ignored and remains in an added state and the all other hosts are appropriately marked up.
     *
     * @jira_ticket JAVA-854
     * @test_category host:state
     */
    @Test(groups = "short")
    public void should_not_abort_init_if_host_does_not_support_protocol_version() {
        ScassandraCluster scassandraCluster = ScassandraCluster.builder()
                .withIpPrefix(TestUtils.IP_PREFIX)
                .withNodes(5)
                // For node 2, report an older version which uses protocol v1.
                .forcePeerInfo(1, 2, "release_version", "1.2.19")
                .build();
        Cluster cluster = Cluster.builder()
                .addContactPoints(scassandraCluster.address(1).getAddress())
                .withPort(scassandraCluster.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
            scassandraCluster.init();
            cluster.init();
            for (int i = 1; i <= 5; i++) {
                InetAddress hostAddress = scassandraCluster.address(i).getAddress();
                if (i == 2) {
                    // As this host is at an older protocol version, it should be ignored and not marked up.
                    assertThat(cluster).host(hostAddress).hasState(Host.State.ADDED);
                } else {
                    // All hosts should be set as 'UP' as part of cluster.init().  If they are
                    // in 'ADDED' state it's possible that cluster.init() did not fully complete.
                    assertThat(cluster).host(hostAddress).hasState(Host.State.UP);
                }
            }
        } finally {
            cluster.close();
            scassandraCluster.stop();
        }
    }

    private void primePeerRows(Scassandra scassandra, List<FakeHost> otherHosts) throws UnknownHostException {
        PrimingClient primingClient =
                PrimingClient.builder()
                        .withHost(ipOfNode(1))
                        .withPort(scassandra.getAdminPort())
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
                    .put("host_id", UUID.randomUUID())
                    .build());
        }

        primingClient.prime(
                PrimingRequest.queryBuilder()
                        .withQuery("SELECT * FROM system.peers")
                        .withThen(then().withRows(rows).withColumnTypes(ScassandraCluster.SELECT_PEERS))
                        .build());
    }
}
