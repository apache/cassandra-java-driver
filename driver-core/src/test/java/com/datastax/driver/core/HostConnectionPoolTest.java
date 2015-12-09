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

import com.codahale.metrics.Gauge;
import com.datastax.driver.core.exceptions.BusyConnectionException;
import com.datastax.driver.core.exceptions.ConnectionException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.google.common.util.concurrent.Uninterruptibles;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.*;
import static org.scassandra.http.client.ClosedConnectionReport.CloseType.CLOSE;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.testng.Assert.fail;

public class HostConnectionPoolTest extends ScassandraTestBase.PerClassCluster {


    @BeforeClass(groups = {"short", "long"})
    public void reinitializeCluster() {
        // Don't use the provided cluster, each test will create its own instead.
        cluster.close();
    }

    /**
     * Sends 101 requests on pool, asserting that the connection used is in expectedConnections, except for the last request.
     */
    private List<MockRequest> fillConnectionToThreshold(HostConnectionPool pool, List<Connection> expectedConnections) throws ConnectionException, BusyConnectionException, TimeoutException {
        List<MockRequest> requests = sendRequests(100, pool, expectedConnections);
        requests.add(MockRequest.send(pool));
        return requests;
    }

    /**
     * Sends count requests on pool.
     */
    private List<MockRequest> sendRequests(int count, HostConnectionPool pool) throws ConnectionException, BusyConnectionException, TimeoutException {
        return sendRequests(count, pool, null);
    }

    /**
     * Sends count requests on pool, asserting that the connection used is in expectedConnections.
     */
    private List<MockRequest> sendRequests(int count, HostConnectionPool pool, List<Connection> expectedConnections) throws ConnectionException, BusyConnectionException, TimeoutException {
        List<MockRequest> requests = newArrayList();
        for (int i = 0; i < count; i++) {
            MockRequest request = MockRequest.send(pool);
            requests.add(request);
            if (expectedConnections != null)
                assertThat(expectedConnections).contains(request.connection);
        }
        return requests;
    }

    @Override
    protected Cluster.Builder configure(Cluster.Builder builder) {
        // Use version 2 at highest.
        ProtocolVersion versionToUse = TestUtils.getDesiredProtocolVersion(ProtocolVersion.V2);
        return builder.withProtocolVersion(versionToUse);
    }

    /**
     * Completes count requests by simulating a successful response.
     */
    private void completeRequests(int count, List<MockRequest> requests) {
        Iterator<MockRequest> requestIt = requests.iterator();
        for (int i = 0; i < count; i++) {
            if (requestIt.hasNext()) {
                MockRequest request = requestIt.next();
                request.simulateSuccessResponse();
                requestIt.remove();
            } else {
                break;
            }
        }
    }

    /**
     * Completes all requests by simulating a successful response.
     */
    private void completeRequests(List<MockRequest> requests) {
        for (MockRequest request : requests) {
            request.simulateSuccessResponse();
        }
    }

    /**
     * Ensures that if a fixed-sized pool has filled its core connections that borrowConnection will timeout instead
     * of creating a new connection.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void fixed_size_pool_should_fill_its_core_connections_and_then_timeout() throws ConnectionException, TimeoutException, BusyConnectionException {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 2, 2);

            assertThat(pool.connections.size()).isEqualTo(2);
            List<Connection> coreConnections = newArrayList(pool.connections);
            requests.addAll(sendRequests(256, pool, coreConnections));

            try {
                MockRequest.send(pool);
                Assertions.fail("Expected a TimeoutException");
            } catch (TimeoutException e) { /*expected*/}
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if a variable-sized pool has filled up to its maximum connections that borrowConnection will
     * timeout instead of creating a new connection.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void variable_size_pool_should_fill_its_connections_and_then_timeout() throws Exception {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            assertThat(pool.connections.size()).isEqualTo(1);
            List<Connection> coreConnections = newArrayList(pool.connections);

            // Fill enough connections to hit the threshold.
            requests.addAll(fillConnectionToThreshold(pool, coreConnections));

            // Allow time for new connection to be spawned.
            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));

            // Borrow more and ensure the connection returned is a non-core connection.
            for (int i = 0; i < 100; i++) {
                MockRequest request = MockRequest.send(pool);
                assertThat(coreConnections).doesNotContain(request.connection);
                requests.add(request);
            }

            // Fill remaining connections (28 + 28) - 1
            requests.addAll(sendRequests(55, pool));

            boolean timedOut = false;
            try {
                MockRequest.send(pool);
            } catch (TimeoutException e) {
                timedOut = true;
            }
            assertThat(timedOut).isTrue();
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if the core connection pool is full that borrowConnection will create and use a new connection.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void should_add_extra_connection_when_core_full() throws Exception {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection core = pool.connections.get(0);

            // Fill core connection + 1
            requests.addAll(fillConnectionToThreshold(pool, singletonList(core)));

            // Reaching the threshold should have triggered the creation of an extra one
            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(2);
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that a trashed connection that has not been timed out should be resurrected into the connection pool if
     * borrowConnection is called and a new connection is needed.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "long")
    public void should_resurrect_trashed_connection_within_idle_timeout() throws Exception {
        Cluster cluster = createClusterBuilder().withPoolingOptions(new PoolingOptions().setIdleTimeoutSeconds(20)).build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection connection1 = pool.connections.get(0);

            requests.addAll(fillConnectionToThreshold(pool, singletonList(connection1)));

            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(2);
            Connection connection2 = pool.connections.get(1);

            assertThat(connection1.inFlight.get()).isEqualTo(101);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Go back under the capacity of 1 connection
            completeRequests(51, requests);

            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Given enough time, one connection gets trashed (and the implementation picks the first one)
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).containsExactly(connection1);

            // Now borrow enough to go just under the 1 connection threshold
            requests.addAll(sendRequests(50, pool));

            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).containsExactly(connection1);
            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(50);

            // Borrowing one more time should resurrect the trashed connection
            requests.addAll(sendRequests(1, pool));
            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));

            assertThat(pool.connections).containsExactly(connection2, connection1);
            assertThat(pool.trash).isEmpty();
            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(51);
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that a trashed connection that has been timed out should not be resurrected into the connection pool if
     * borrowConnection is called and a new connection is needed.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "long")
    public void should_not_resurrect_trashed_connection_after_idle_timeout() throws Exception {
        Cluster cluster = createClusterBuilder().withPoolingOptions(new PoolingOptions().setIdleTimeoutSeconds(20)).build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection connection1 = pool.connections.get(0);

            requests.addAll(fillConnectionToThreshold(pool, singletonList(connection1)));

            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));
            reset(factory);
            assertThat(pool.connections).hasSize(2);
            Connection connection2 = pool.connections.get(1);

            assertThat(connection1.inFlight.get()).isEqualTo(101);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Go back under the capacity of 1 connection
            completeRequests(51, requests);

            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Given enough time, one connection gets trashed (and the implementation picks the first one)
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).containsExactly(connection1);

            // Return trashed connection down to 0 inFlight
            completeRequests(50, requests);
            assertThat(connection1.inFlight.get()).isEqualTo(0);

            // Give enough time for trashed connection to be cleaned up from the trash:
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).isEmpty();
            assertThat(connection1.isClosed()).isTrue();

            // Fill the live connection to go over the threshold where a second one is needed
            requests.addAll(fillConnectionToThreshold(pool, singletonList(connection2)));
            assertThat(connection2.inFlight.get()).isEqualTo(101);
            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));

            // Borrow again to get the new connection
            MockRequest request = MockRequest.send(pool);
            requests.add(request);
            assertThat(request.connection)
                    .isNotEqualTo(connection2) // should not be the full connection
                    .isNotEqualTo(connection1); // should not be the previously trashed one
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that a trashed connection that has been timed out should not be closed until it has 0 in flight requests.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "long")
    public void should_not_close_trashed_connection_until_no_in_flight() throws Exception {
        Cluster cluster = createClusterBuilder().withPoolingOptions(new PoolingOptions().setIdleTimeoutSeconds(20)).build();
        List<MockRequest> requests = newArrayList();

        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection connection1 = pool.connections.get(0);

            // Fill core connection enough to trigger creation of another one
            requests.addAll(fillConnectionToThreshold(pool, singletonList(connection1)));

            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(2);

            // Return enough times to get back under the threshold where one connection is enough
            completeRequests(50, requests);

            // Give enough time for one connection to be trashed. Due to the implementation, this will be the first one.
            // It still has in-flight requests so should not get closed.
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
            assertThat(pool.trash).containsExactly(connection1);
            assertThat(connection1.inFlight.get()).isEqualTo(51);
            assertThat(connection1.isClosed()).isFalse();

            // Consume all inFlight requests on the trashed connection.
            completeRequests(51, requests);

            // Sleep enough time for the connection to be consider idled and closed.
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);

            // The connection should be now closed.
            // The trashed connection should be closed and not in the pool or trash.
            assertThat(connection1.isClosed()).isTrue();
            assertThat(pool.connections).doesNotContain(connection1);
            assertThat(pool.trash).doesNotContain(connection1);
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if a connection that has less than the minimum available stream ids is returned to the pool that
     * the connection is put in the trash.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void should_trash_on_returning_connection_with_insufficient_streams() throws Exception {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection core = pool.connections.get(0);

            requests.addAll(fillConnectionToThreshold(pool, singletonList(core)));

            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(2);

            // Grab the new non-core connection and replace it with a spy.
            Connection extra1 = spy(pool.connections.get(1));
            pool.connections.set(1, extra1);

            // Borrow 10 times to ensure pool is utilized.
            requests.addAll(sendRequests(10, pool));
            assertThat(pool.connections).hasSize(2);

            // stub the maxAvailableStreams method to return 0, indicating there are no remaining streams.
            // this should cause the connection to be replaced and trashed on returnConnection.
            doReturn(0).when(extra1).maxAvailableStreams();

            // On returning of the connection, should detect that there are no available streams and trash it.
            assertThat(pool.trash).hasSize(0);
            pool.returnConnection(extra1);
            assertThat(pool.connections).hasSize(1);
            assertThat(pool.trash).hasSize(1);
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if a connection on a host is lost but other connections remain intact in the Pool that the
     * host is not marked down.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_keep_host_up_when_one_connection_lost() throws Exception {
        Cluster cluster = createClusterBuilder().build();
        try {
            HostConnectionPool pool = createPool(cluster, 2, 2);
            Connection core0 = pool.connections.get(0);
            Connection core1 = pool.connections.get(1);

            // Drop a connection and ensure the host stays up.
            currentClient.disableListener();
            currentClient.closeConnection(CLOSE, ((InetSocketAddress) core0.channel.localAddress()));
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            // connection 0 should be down, while connection 1 and the Host should remain up.
            assertThat(core0.isClosed()).isTrue();
            assertThat(core1.isClosed()).isFalse();
            assertThat(pool.connections).doesNotContain(core0);
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(cluster).hasOpenControlConnection();
        } finally {
            cluster.close();
        }
    }


    /**
     * Ensures that if all connections on a host are closed that the host is marked
     * down and the control connection is notified of that fact and re-established
     * itself.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_mark_host_down_when_no_connections_remaining() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(readTimeout)
                        .setReadTimeoutMillis(reconnectInterval))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        try {
            cluster.init();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            HostConnectionPool pool = createPool(cluster, 8, 8);
            // copy list to track these connections.
            List<Connection> connections = newArrayList(pool.connections);

            reset(factory);

            // Drop all connections.
            currentClient.disableListener();
            currentClient.closeConnections(CLOSE);

            // The host should be marked down and the control connection closed.
            assertThat(cluster).host(1).goesDownWithin(10, TimeUnit.SECONDS);
            assertThat(cluster).hasClosedControlConnection();

            // Ensure all connections are closed.
            for (Connection connection : connections) {
                assertThat(connection.isClosed()).isTrue();
            }

            // Expect a reconnect attempt on host after reconnect interval
            // on behalf of the control connection.
            verify(factory, timeout(reconnectInterval * 2).atLeastOnce()).open(host);

            // Sleep for a bit to allow reconnect to fail.
            Uninterruptibles.sleepUninterruptibly(readTimeout * 2, TimeUnit.MILLISECONDS);

            // Ensure control connection is still closed.
            assertThat(cluster).hasClosedControlConnection();

            // Reenable connectivity.
            currentClient.enableListener();

            // Reconnect attempt should have been connected for control connection
            // and pool.
            // 2 attempts for connection.open (reconnect control connection and initial connection for host state).
            verify(factory, after(reconnectInterval * 2).atLeast(2)).open(host);
            // 7 attempts for core connections after first initial connection.
            verify(factory, timeout(reconnectInterval * 2)).newConnections(any(HostConnectionPool.class), eq(7));

            // Wait some reasonable amount of time for connection to reestablish.
            Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);

            // Control Connection should now be open.
            assertThat(cluster).hasOpenControlConnection();
            assertThat(cluster).host(1).hasState(Host.State.UP);
        } finally {
            cluster.close();
        }
    }


    /**
     * Ensures that if a connection on a host is lost that brings the number of active connections in a pool
     * under core connection count that up to core connections are re-established, but only after the
     * next reconnect schedule has elapsed.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_create_new_connections_when_connection_lost_and_under_core_connections() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(readTimeout)
                        .setReadTimeoutMillis(reconnectInterval))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        List<MockRequest> requests = newArrayList();
        try {
            cluster.init();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            HostConnectionPool pool = createPool(cluster, 3, 3);
            Connection core0 = pool.connections.get(0);
            Connection core1 = pool.connections.get(1);
            Connection core2 = pool.connections.get(2);

            // Drop two core connections.
            // Disable new connections initially and we'll eventually reenable it.
            currentClient.disableListener();
            currentClient.closeConnection(CLOSE, ((InetSocketAddress) core0.channel.localAddress()));
            currentClient.closeConnection(CLOSE, ((InetSocketAddress) core2.channel.localAddress()));
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

            // Since we have a connection left the host should remain up.
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(pool.connections).hasSize(1);

            // The borrowed connection should be the open one.
            MockRequest request = MockRequest.send(pool);
            requests.add(request);
            assertThat(request.connection).isEqualTo(core1);

            // Should not have tried to create a new core connection since reconnection time had not elapsed.
            verify(factory, never()).open(any(HostConnectionPool.class));

            // Sleep to elapse the Reconnection Policy.
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Attempt to borrow connection, this should trigger ensureCoreConnections thus spawning a new connection.
            request = MockRequest.send(pool);
            requests.add(request);
            assertThat(request.connection).isEqualTo(core1);

            // Should have tried to open up to core connections as result of borrowing a connection past reconnect time and not being at core.
            verify(factory, timeout(reconnectInterval).times(1)).open(any(HostConnectionPool.class));
            reset(factory);

            // Sleep for reconnect interval to allow reconnection time to elapse.
            Uninterruptibles.sleepUninterruptibly((readTimeout + reconnectInterval) * 2, TimeUnit.MILLISECONDS);

            // Enable listening so new connections succeed.
            currentClient.enableListener();
            // Sleep to elapse the Reconnection Policy.
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Try to borrow a connection, the pool should grow.
            requests.add(MockRequest.send(pool));
            verify(factory, timeout((reconnectInterval + readTimeout) * 2).times(1)).open(any(HostConnectionPool.class));
            reset(factory);

            // Another core connection should be opened as result of another request to get us up to core connections.
            requests.add(MockRequest.send(pool));
            verify(factory, timeout((reconnectInterval + readTimeout) * 2).times(1)).open(any(HostConnectionPool.class));
            reset(factory);

            // Sending another request should not grow the pool any more, since we are now at core connections.
            requests.add(MockRequest.send(pool));
            verify(factory, after((reconnectInterval + readTimeout) * 2).never()).open(any(HostConnectionPool.class));
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if a connection on a host is lost and the number of remaining connections is at
     * core connection count that no connections are re-established until after there are enough
     * inflight requests to justify creating one and the reconnection interval has elapsed.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_not_schedule_reconnect_when_connection_lost_and_at_core_connections() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(readTimeout)
                        .setReadTimeoutMillis(reconnectInterval))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        List<MockRequest> requests = newArrayList();
        try {
            cluster.init();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection core0 = pool.connections.get(0);

            // Create enough inFlight requests to spawn another connection.
            List<MockRequest> core0requests = newArrayList();
            for (int i = 0; i < 101; i++) {
                MockRequest request = MockRequest.send(pool);
                assertThat(request.connection).isEqualTo(core0);
                core0requests.add(request);
            }

            // Pool should grow by 1.
            verify(factory, after(1000).times(1)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(2);

            // Reset factory mock as we'll be checking for new open() invokes later.
            reset(factory);

            // Grab the new non-core connection.
            Connection extra1 = pool.connections.get(1);

            // Drop a connection and disable listening.
            currentClient.closeConnection(CLOSE, ((InetSocketAddress) core0.channel.localAddress()));
            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            currentClient.disableListener();

            // Since core0 was closed, all of it's requests should have errored.
            for (MockRequest request : core0requests) {
                verify(request, times(1)).onException(any(Connection.class), any(Exception.class), anyLong(), anyInt());
            }

            assertThat(cluster).host(1).hasState(Host.State.UP);

            // Create enough inFlight requests to fill connection.
            requests.addAll(sendRequests(100, pool, singletonList(extra1)));
            assertThat(pool.connections).hasSize(1);

            // A new connection should never have been spawned since we didn't max out core.
            verify(factory, after(readTimeout).never()).open(any(HostConnectionPool.class));

            // Borrow another connection, since we exceed max another connection should be opened.
            MockRequest request = MockRequest.send(pool);
            requests.add(request);
            assertThat(request.connection).isEqualTo(extra1);

            // After some time the a connection should attempt to be opened (but will fail).
            verify(factory, timeout(readTimeout)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(1);

            // Wait some reasonable amount of time for connection to reestablish then check pool size.
            Uninterruptibles.sleepUninterruptibly(readTimeout * 2, TimeUnit.MILLISECONDS);
            // Reconnecting failed since listening was enabled.
            assertThat(pool.connections).hasSize(1);

            // Re enable listening then wait for reconnect.
            currentClient.enableListener();
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Borrow another connection, since we exceed max another connection should be opened.
            request = MockRequest.send(pool);
            requests.add(request);
            assertThat(request.connection).isEqualTo(extra1);

            // Wait some reasonable amount of time for connection to reestablish then check pool size.
            Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);
            // Reconnecting should have exceeded and pool will have grown.
            assertThat(pool.connections).hasSize(2);

            // Borrowed connection should be the newly spawned connection since the other one has some inflight requests.
            request = MockRequest.send(pool);
            requests.add(request);
            assertThat(request.connection).isNotEqualTo(core0).isNotEqualTo(extra1);
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if some connections fail on pool init that the host and subsequently the
     * control connection is not marked down.  The test also ensures that when making requests
     * on the pool that connections are brought up to core.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_not_mark_host_down_if_some_connections_fail_on_init() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(readTimeout)
                        .setReadTimeoutMillis(reconnectInterval))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        List<MockRequest> requests = newArrayList();
        try {
            cluster.init();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            // Allow the first 4 connections to establish, but disable after that.
            currentClient.disableListener(4);
            HostConnectionPool pool = createPool(cluster, 8, 8);

            reset(factory);

            // Pool size should show all successful connections.
            assertThat(pool.connections).hasSize(4);

            // Control connection should remain up in addition to to host.
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(cluster).hasOpenControlConnection();

            // Reenable listener, wait reconnectInterval and then try borrowing a connection.
            currentClient.enableListener();

            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Should open up to core connections, however it will only spawn up to 1 connection
            // per request, so we need to make enough requests to make up the deficit.  Additionally
            // we need to wait for connections to be established between requests for the pool
            // to spawn new connections (since it only allows one simultaneous creation).
            for (int i = 5; i <= 8; i++) {
                requests.add(MockRequest.send(pool));
                verify(factory, timeout(readTimeout)).open(any(HostConnectionPool.class));
                reset(factory);
                Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);
                assertThat(pool.connections).hasSize(i);
            }
        } finally {
            completeRequests(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if all connections fail on pool init that the host and subsequently the
     * control connection is not marked down since the control connection is still active.
     * on the pool that a TimeoutException is yielded if we are still in the reconnection window
     * according to the ConvictionPolicy.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short", expectedExceptions = TimeoutException.class)
    public void should_throw_exception_if_convicted_and_no_connections_available() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(readTimeout)
                        .setReadTimeoutMillis(reconnectInterval))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        try {
            // Init cluster so control connection is created.
            cluster.init();
            assertThat(cluster).hasOpenControlConnection();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            // Disable listener so all connections on pool fail.
            currentClient.disableListener();
            HostConnectionPool pool = createPool(cluster, 8, 8);

            reset(factory);

            // Pool should be empty.
            assertThat(pool.connections).hasSize(0);

            // Control connection should stay up with the host.
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(cluster).hasOpenControlConnection();

            MockRequest.send(pool);
        } finally {
            cluster.close();
        }
    }


    /**
     * Ensures that if all connections fail on pool init that the host and subsequently the
     * control connection is not marked down.  The test also ensures that when making requests
     * on the pool after the conviction period that all core connections are created.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_wait_on_connection_if_not_convicted_and_no_connections_available() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(readTimeout)
                        .setReadTimeoutMillis(reconnectInterval))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        try {
            // Init cluster so control connection is created.
            cluster.init();
            assertThat(cluster).hasOpenControlConnection();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            // Disable listener so all connections on pool fail.
            currentClient.disableListener();
            HostConnectionPool pool = createPool(cluster, 8, 8);

            // Pool should be empty.
            assertThat(pool.connections).hasSize(0);

            // Control connection should stay up with the host.
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(cluster).hasOpenControlConnection();

            currentClient.enableListener();

            // Wait for reconnectInterval so ConvictionPolicy allows connection to be created.
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            reset(factory);
            MockRequest request = MockRequest.send(pool);
            request.simulateSuccessResponse();

            // Should create up to core connections.
            verify(factory, timeout(readTimeout * 8).times(8)).open(any(HostConnectionPool.class));
            Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);
            assertThat(pool.connections).hasSize(8);
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that if a pool is created with zero core connections that when a request
     * is first sent that one and only one connection is created and that it waits on availability
     * of that connection and returns it.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_wait_on_connection_if_zero_core_connections() throws Exception {
        int readTimeout = 1000;
        int reconnectInterval = 1000;
        Cluster cluster = this.createClusterBuilder()
                .withSocketOptions(new SocketOptions()
                        .setConnectTimeoutMillis(readTimeout)
                        .setReadTimeoutMillis(reconnectInterval))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(1000)).build();
        try {
            // Init cluster so control connection is created.
            cluster.init();
            assertThat(cluster).hasOpenControlConnection();

            HostConnectionPool pool = createPool(cluster, 0, 2);

            // Pool should be empty.
            assertThat(pool.connections).hasSize(0);

            // Control connection should stay up with the host.
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(cluster).hasOpenControlConnection();

            // Send a request, this should create a connection.
            MockRequest request = MockRequest.send(pool);
            request.simulateSuccessResponse();

            // Should create up to core connections.
            assertThat(pool.connections).hasSize(1);
        } finally {
            cluster.close();
        }
    }

    private HostConnectionPool createPool(Cluster cluster, int coreConnections, int maxConnections) {
        cluster.getConfiguration().getPoolingOptions()
                .setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnections)
                .setCoreConnectionsPerHost(HostDistance.LOCAL, coreConnections);
        Session session = cluster.connect();
        Host host = TestUtils.findHost(cluster, 1);

        // Replace the existing pool with a spy pool and return it.
        SessionManager sm = ((SessionManager) session);
        return sm.pools.get(host);
    }

    /**
     * <p/>
     * This test uses a table named "Java349" with 1000 column and performs asynchronously 100k insertions. While the
     * insertions are being executed, the number of opened connection is monitored.
     * <p/>
     * If at anytime, the number of opened connections is negative, this test will fail.
     *
     * @jira_ticket JAVA-349
     * @test_category connection:connection_pool
     * @since 2.0.6, 2.1.1
     */
    @Test(groups = "long", enabled = false /* this test causes timeouts on Jenkins */)
    public void open_connections_metric_should_always_be_positive() throws InterruptedException {
        // Track progress in a dedicated thread
        int numberOfInserts = 100 * 1000;
        final CountDownLatch pendingInserts = new CountDownLatch(numberOfInserts);
        ExecutorService progressReportExecutor = Executors.newSingleThreadExecutor();
        final Runnable progressReporter = new Runnable() {
            @Override
            public void run() {
                pendingInserts.countDown();
            }
        };

        // Track opened connections in a dedicated thread every one second
        final AtomicBoolean negativeOpenConnectionCountSpotted = new AtomicBoolean(false);
        final Gauge<Integer> openConnections = cluster.getMetrics().getOpenConnections();
        ScheduledExecutorService openConnectionsWatcherExecutor = Executors.newScheduledThreadPool(1);
        final Runnable openConnectionsWatcher = new Runnable() {
            @Override
            public void run() {
                Integer value = openConnections.getValue();
                if (value < 0) {
                    System.err.println("Negative value spotted for openConnection metric: " + value);
                    negativeOpenConnectionCountSpotted.set(true);
                }
            }
        };
        openConnectionsWatcherExecutor.scheduleAtFixedRate(openConnectionsWatcher, 1, 1, SECONDS);

        // Insert 100k lines in a newly created 1k columns table
        PreparedStatement insertStatement = session.prepare(generateJava349InsertStatement());
        for (int key = 0; key < numberOfInserts; key++) {
            ResultSetFuture future = session.executeAsync(insertStatement.bind(key));
            future.addListener(progressReporter, progressReportExecutor);
        }

        // Wait for all inserts to happen and stop connections and progress tracking
        pendingInserts.await();
        openConnectionsWatcherExecutor.shutdownNow();
        progressReportExecutor.shutdownNow();

        if (negativeOpenConnectionCountSpotted.get()) {
            fail("Negative value spotted for open connection count");
        }
    }

    private String generateJava349InsertStatement() {
        StringBuilder sb = new StringBuilder("INSERT INTO Java349 (mykey");
        for (int i = 0; i < 1000; i++) {
            sb.append(", column").append(i);
        }
        sb.append(") VALUES (?");
        for (int i = 0; i < 1000; i++) {
            sb.append(", ").append(i);
        }
        sb.append(");");

        PrimingRequest preparedStatementPrime = PrimingRequest.preparedStatementBuilder()
                .withQuery(sb.toString())
                .withThen(then().withVariableTypes(PrimitiveType.INT))
                .build();
        primingClient.prime(preparedStatementPrime);
        return sb.toString();
    }

    /**
     * Mock ResponseCallback that simulates the behavior of SpeculativeExecution (in terms of borrowing/releasing connections).
     */
    static class MockRequest implements Connection.ResponseCallback {

        enum State {START, COMPLETED, FAILED, TIMED_OUT}

        final Connection connection;
        private Connection.ResponseHandler responseHandler;

        private final AtomicReference<State> state = new AtomicReference<State>(State.START);

        static MockRequest send(HostConnectionPool pool) throws ConnectionException, BusyConnectionException, TimeoutException {
            // Create a MockRequest and spy on it.  Create a response handler and add it to the connection's dispatcher.
            MockRequest request = spy(new MockRequest(pool));
            request.responseHandler = new Connection.ResponseHandler(request.connection, request);
            request.connection.dispatcher.add(request.responseHandler);
            return request;
        }

        private MockRequest(HostConnectionPool pool) throws ConnectionException, TimeoutException, BusyConnectionException {
            connection = pool.borrowConnection(500, TimeUnit.MILLISECONDS);
        }

        void simulateSuccessResponse() {
            onSet(connection, null, 0, 0);
        }

        @SuppressWarnings("unused")
        void simulateErrorResponse() {
            onException(connection, null, 0, 0);
        }

        @SuppressWarnings("unused")
        void simulateTimeout() {
            if (onTimeout(connection, 0, 0))
                responseHandler.cancelHandler();
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
            if (state.compareAndSet(State.START, State.COMPLETED)) {
                connection.release();
                connection.dispatcher.removeHandler(responseHandler, true);
            }
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency, int retryCount) {
            if (state.compareAndSet(State.START, State.FAILED)) {
                connection.release();
                connection.dispatcher.removeHandler(responseHandler, true);
            }
        }

        @Override
        public boolean onTimeout(Connection connection, long latency, int retryCount) {
            return state.compareAndSet(State.START, State.TIMED_OUT);
        }

        @Override
        public Message.Request request() {
            return null; // not needed for this test class
        }

        @Override
        public int retryCount() {
            return 0; // value not important for this test class
        }
    }
}
