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

import com.codahale.metrics.Gauge;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.*;
import org.scassandra.cql.PrimitiveType;
import org.scassandra.http.client.PrimingRequest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.ConditionChecker.check;
import static com.datastax.driver.core.PoolingOptions.NEW_CONNECTION_THRESHOLD_LOCAL_KEY;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.*;
import static org.scassandra.http.client.ClosedConnectionReport.CloseType.CLOSE;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.Result.server_error;
import static org.testng.Assert.fail;

public class HostConnectionPoolTest extends ScassandraTestBase.PerClassCluster {

    static final Integer NEW_CONNECTION_THRESHOLD = PoolingOptions.DEFAULTS.get(ProtocolVersion.V1)
            .get(NEW_CONNECTION_THRESHOLD_LOCAL_KEY);

    @BeforeClass(groups = {"short", "long"})
    public void reinitializeCluster() {
        // Don't use the provided cluster, each test will create its own instead.
        cluster.close();
    }

    /**
     * Ensure the given pool has the given size within 5 seconds.
     *
     * @param pool         Pool to check.
     * @param expectedSize Expected size of pool.
     */
    private void assertPoolSize(HostConnectionPool pool, final int expectedSize) {
        check().before(5, TimeUnit.SECONDS)
                .that(pool, new Predicate<HostConnectionPool>() {
                    @Override
                    public boolean apply(HostConnectionPool input) {
                        return input.connections.size() == expectedSize;
                    }
                }).becomesTrue();
    }

    private void assertBorrowedConnections(Iterable<MockRequest> requests, List<Connection> expectedConnections) {
        for (MockRequest request : requests) {
            assertThat(expectedConnections).contains(request.getConnection());
        }
    }

    private void assertBorrowedConnection(Iterable<MockRequest> requests, Connection expectedConnection) {
        assertBorrowedConnections(requests, Collections.singletonList(expectedConnection));
    }

    /**
     * Ensures that if a fixed-sized pool has filled its core connections and reached its maximum number of enqueued
     * requests, then borrowConnection will fail instead of creating a new connection.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void fixed_size_pool_should_fill_its_core_connections_and_queue_and_then_reject() {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> allRequests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 2, 2);
            int maxQueueSize = 256;

            assertThat(pool.connections.size()).isEqualTo(2);
            List<Connection> coreConnections = newArrayList(pool.connections);
            // fill connections
            List<MockRequest> requests = MockRequest.sendMany(2 * 128, pool);
            assertBorrowedConnections(requests, coreConnections);
            allRequests.addAll(requests);
            // fill queue
            allRequests.addAll(MockRequest.sendMany(maxQueueSize, pool, maxQueueSize));

            // add one more request, it should be rejected because the queue is full
            MockRequest failedBorrow = MockRequest.send(pool, maxQueueSize);
            try {
                failedBorrow.getConnection();
                fail("Expected a BusyPoolException");
            } catch (BusyPoolException e) {
                assertThat(e).hasMessageContaining("reached its max size");
            }
        } finally {
            MockRequest.completeAll(allRequests);
            cluster.close();
        }
    }

    /**
     * Ensures that if a fixed-sized pool has filled its core connections and reached a number of requests to cause
     * it to be enqueued, that if the request is not serviced within 100ms, a BusyPoolException is raised with a timeout.
     *
     * @jira_ticket JAVA-1371
     * @test_category connection:connection_pool
     * @since 3.0.7 3.1.4 3.2.0
     */
    @Test(groups = "short")
    public void should_reject_if_enqueued_and_timeout_reached() {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> allRequests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 1);
            List<MockRequest> requests = MockRequest.sendMany(128, pool);
            allRequests.addAll(requests);

            // pool is now full, this request will be enqueued
            MockRequest failedBorrow = MockRequest.send(pool, 100, 128);
            try {
                failedBorrow.getConnection();
                fail("Expected a BusyPoolException");
            } catch (BusyPoolException e) {
                assertThat(e).hasMessageContaining("timed out");
            }
        } finally {
            MockRequest.completeAll(allRequests);
            cluster.close();
        }
    }

    /**
     * Validates that if a borrow request is enqueued into a pool for a Host that is currently
     * within the window of reconnecting after an error that the future tied to that query times out
     * after {@link PoolingOptions#setPoolTimeoutMillis(int)}.
     * <p>
     * This primarily serves to demonstrate that the use case described in JAVA-1371 is fixed by using
     * {@link Session#execute(Statement)} shortly after a server error on a connection.
     *
     * @jira_ticket JAVA-1371
     * @test_category connection:connection_pool
     * @since 3.0.7 3.1.4 3.2.0
     */
    @Test(groups = "short")
    public void should_not_hang_when_executing_sync_queries() {
        primingClient.prime(
                queryBuilder()
                        .withQuery("server_error query")
                        .withThen(then().withResult(server_error))
                        .build()
        );

        Cluster cluster = createClusterBuilder()
                .withReconnectionPolicy(new ConstantReconnectionPolicy(10000)).build();
        // reduce timeout so test runs faster.
        cluster.getConfiguration().getPoolingOptions().setPoolTimeoutMillis(500);
        try {
            Session session = cluster.connect();
            try {
                session.execute("server_error query");
                fail("Exception expected");
            } catch (Exception e) {
                // error is expected in this case.
            }

            try {
                session.execute("this should not block indefinitely");
            } catch (NoHostAvailableException nhae) {
                // should raise a NHAE with a BusyPoolException.
                Collection<Throwable> errors = nhae.getErrors().values();
                assertThat(errors).hasSize(1);
                Throwable e = errors.iterator().next();
                assertThat(e).isInstanceOf(BusyPoolException.class);
                assertThat(e).hasMessageContaining("timed out");
            }
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that any enqueued connection borrow requests are failed when their associated connection pool closes.
     *
     * @jira_ticket JAVA-839
     * @test_category connection:connection_pool
     * @since 3.0.4, 3.1.1
     */
    @Test(groups = "short")
    public void requests_with_enqueued_borrow_requests_should_be_failed_when_pool_closes() {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 2, 2);
            int maxQueueSize = 256;

            assertThat(pool.connections.size()).isEqualTo(2);
            List<Connection> coreConnections = newArrayList(pool.connections);
            // fill connections
            requests = MockRequest.sendMany(2 * 128, pool);
            assertBorrowedConnections(requests, coreConnections);
            // fill queue
            List<MockRequest> queuedRequests = MockRequest.sendMany(maxQueueSize, pool, maxQueueSize);

            // Closing the pool should fail all queued connections.
            pool.closeAsync();

            for (MockRequest queuedRequest : queuedRequests) {
                // Future should be completed.
                assertThat(queuedRequest.connectionFuture.isDone()).isTrue();
                try {
                    // Future should fail as result of the pool closing.
                    queuedRequest.getConnection();
                    fail("Expected a ConnectionException");
                } catch (ConnectionException e) {/*expected*/}
            }
        } finally {
            MockRequest.completeAll(requests);
            cluster.close();
        }
    }

    /**
     * Validates that if the keyspace tied to the Session's pool state is different than the keyspace on the connection
     * being used in dequeue that {@link Connection#setKeyspaceAsync(String)} is set on that connection and that
     * "USE keyspace" is only called once since setKeyspaceAsync should not attempt setting the keyspace if there is
     * already a request inflight that is doing this.
     *
     * @jira_ticket JAVA-839
     * @test_category connection:connection_pool
     * @since 3.0.4, 3.1.1
     */
    @Test(groups = "short")
    public void should_adjust_connection_keyspace_on_dequeue_if_pool_state_is_different() throws TimeoutException, ExecutionException {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 1);
            int maxQueueSize = 256;

            assertThat(pool.connections.size()).isEqualTo(1);
            List<Connection> coreConnections = newArrayList(pool.connections);
            // fill connections
            requests = MockRequest.sendMany(128, pool);
            assertBorrowedConnections(requests, coreConnections);
            // fill queue
            List<MockRequest> queuedRequests = MockRequest.sendMany(maxQueueSize, pool, maxQueueSize);

            // Wait for connections to be borrowed before changing keyspace.
            for (MockRequest request : requests) {
                Uninterruptibles.getUninterruptibly(request.connectionFuture, 5, TimeUnit.SECONDS);
            }
            // Simulate change of keyspace on pool.  Prime a delay so existing requests can complete beforehand.
            primingClient.prime(PrimingRequest.queryBuilder().withQuery("USE \"newkeyspace\"").withThen(PrimingRequest.then().withFixedDelay(2000L)));
            pool.manager.poolsState.setKeyspace("newkeyspace");

            // Complete all requests, this should cause dequeue on connection.
            MockRequest.completeAll(requests);

            // Check the status on queued request's connection futures.  We expect that dequeue should
            // be called when connection is released by previous requests completing, and that one set
            // keyspace attempt should be tried.

            int count = 0;
            for (MockRequest queuedRequest : queuedRequests) {
                try {
                    Uninterruptibles.getUninterruptibly(queuedRequest.connectionFuture, 10, TimeUnit.SECONDS);
                    count++;
                } catch (ExecutionException e) {
                    // 128th request should timeout since all in flight requests are used.
                    assertThat(e.getCause())
                            .isInstanceOf(BusyPoolException.class)
                            .hasMessageContaining("timed out after");
                    assertThat(count).isEqualTo(128);
                    break;
                }
            }

            // We should only have gotten one 'USE newkeyspace' query since Connection#setKeyspaceAsync should only do
            // this once if there is already a request in flight.
            assertThat(activityClient.retrieveQueries()).extractingResultOf("getQuery").containsOnlyOnce("USE \"newkeyspace\"");
        } finally {
            MockRequest.completeAll(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that on borrowConnection if a set keyspace attempt is in progress on that connection for a different keyspace than the
     * pool state that the borrowConnection future returned is failed.
     *
     * @jira_ticket JAVA-839
     * @test_category connection:connection_pool
     * @since 3.0.4, 3.1.1
     */
    @Test(groups = "short")
    public void should_fail_in_borrowConnection_when_setting_keyspace_and_another_set_keyspace_attempt_is_in_flight() throws TimeoutException {
        Cluster cluster = createClusterBuilder().build();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 1);

            // Respond to setting as slowks very slowly.
            primingClient.prime(PrimingRequest.queryBuilder().withQuery("USE \"slowks\"").withThen(PrimingRequest.then().withFixedDelay(5000L)));

            Connection connection = pool.connections.get(0);

            connection.setKeyspaceAsync("slowks");

            // Simulate change of keyspace on pool.
            pool.manager.poolsState.setKeyspace("newks");

            MockRequest request = MockRequest.send(pool);

            try {
                Uninterruptibles.getUninterruptibly(request.connectionFuture, 5, TimeUnit.SECONDS);
                fail("Should have thrown exception");
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(DriverException.class);
                assertThat(e.getCause().getMessage()).contains("Aborting attempt to set keyspace to 'newks' since there is already an in flight attempt to set keyspace to 'slowks'.");
            }
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that while dequeuing borrow connection requests that if a set keyspace attempt is in progress on that connection for a difference
     * keyspace than the pool state that the future for that borrow attempt is failed.
     *
     * @jira_ticket JAVA-839
     * @test_category connection:connection_pool
     * @since 3.0.4, 3.1.1
     */
    @Test(groups = "short")
    public void should_fail_in_dequeue_when_setting_keyspace_and_another_set_keyspace_attempt_is_in_flight() throws ExecutionException, TimeoutException {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> requests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 1);
            // Limit requests per connection to 100 so we don't exhaust stream ids.
            cluster.getConfiguration().getPoolingOptions().setMaxRequestsPerConnection(HostDistance.LOCAL, 100);
            int maxQueueSize = 256;

            assertThat(pool.connections.size()).isEqualTo(1);
            List<Connection> coreConnections = newArrayList(pool.connections);

            // fill connections
            requests = MockRequest.sendMany(100, pool);
            assertBorrowedConnections(requests, coreConnections);

            // send a request that will be queued.
            MockRequest queuedRequest = MockRequest.send(pool, maxQueueSize);

            // Wait for connections to be borrowed before changing keyspace.
            for (MockRequest request : requests) {
                Uninterruptibles.getUninterruptibly(request.connectionFuture, 5, TimeUnit.SECONDS);
            }

            // Respond to setting as slowks very slowly.
            primingClient.prime(PrimingRequest.queryBuilder().withQuery("USE \"slowks\"").withThen(PrimingRequest.then().withFixedDelay(5000L)));
            Connection connection = pool.connections.get(0);
            connection.setKeyspaceAsync("slowks");

            // Simulate change of keyspace on pool.
            pool.manager.poolsState.setKeyspace("newkeyspace");

            // Complete all requests, this should cause dequeue on connection.
            MockRequest.completeAll(requests);

            try {
                Uninterruptibles.getUninterruptibly(queuedRequest.connectionFuture, 5, TimeUnit.SECONDS);
                fail("Should have thrown exception");
            } catch (ExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(DriverException.class);
                assertThat(e.getCause().getMessage()).contains("Aborting attempt to set keyspace to 'newkeyspace' since there is already an in flight attempt to set keyspace to 'slowks'.");
            }
        } finally {
            MockRequest.completeAll(requests);
            cluster.close();
        }
    }

    /**
     * Ensures that if a variable-sized pool has filled up to its maximum connections that borrowConnection will
     * return a failed future instead of creating a new connection.
     *
     * @jira_ticket JAVA-419
     * @test_category connection:connection_pool
     * @since 2.0.10, 2.1.6
     */
    @Test(groups = "short")
    public void variable_size_pool_should_fill_its_connections_and_then_reject() throws Exception {
        Cluster cluster = createClusterBuilder().build();
        List<MockRequest> allRequests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            assertThat(pool.connections.size()).isEqualTo(1);
            Connection coreConnection = pool.connections.get(0);

            // Fill enough connections to hit the threshold.
            List<MockRequest> requests = MockRequest.sendMany(NEW_CONNECTION_THRESHOLD, pool);
            assertBorrowedConnection(requests, coreConnection);
            allRequests.addAll(requests);
            allRequests.add(MockRequest.send(pool));

            // Allow time for new connection to be spawned.
            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 2);

            // Borrow more and ensure the connection returned is a non-core connection.
            for (int i = 0; i < NEW_CONNECTION_THRESHOLD; i++) {
                MockRequest request = MockRequest.send(pool);
                assertThat(request.getConnection()).isNotEqualTo(coreConnection);
                allRequests.add(request);
            }

            // Fill remaining connections (28 + 28) - 1
            allRequests.addAll(MockRequest.sendMany(55, pool));

            MockRequest failedBorrow = MockRequest.send(pool);
            try {
                failedBorrow.getConnection();
                fail("Expected a BusyPoolException");
            } catch (BusyPoolException e) { /*expected*/}
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection core = pool.connections.get(0);

            // Fill core connection + 1
            List<MockRequest> requests = MockRequest.sendMany(NEW_CONNECTION_THRESHOLD, pool);
            assertBorrowedConnection(requests, core);
            allRequests.addAll(requests);
            allRequests.add(MockRequest.send(pool));

            // Reaching the threshold should have triggered the creation of an extra one
            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 2);
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection connection1 = pool.connections.get(0);

            List<MockRequest> requests = MockRequest.sendMany(NEW_CONNECTION_THRESHOLD, pool);
            assertBorrowedConnections(requests, Collections.singletonList(connection1));
            allRequests.addAll(requests);
            allRequests.add(MockRequest.send(pool));

            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 2);
            Connection connection2 = pool.connections.get(1);

            assertThat(connection1.inFlight.get()).isEqualTo(101);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Go back under the capacity of 1 connection
            MockRequest.completeMany(51, allRequests);

            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Given enough time, one connection gets trashed (and the implementation picks the first one)
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).containsExactly(connection1);

            // Now borrow enough to go just under the 1 connection threshold
            allRequests.addAll(MockRequest.sendMany(50, pool));

            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).containsExactly(connection1);
            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(50);

            // Borrowing one more time should resurrect the trashed connection
            allRequests.addAll(MockRequest.sendMany(1, pool));
            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 2);

            assertThat(pool.connections).containsExactly(connection2, connection1);
            assertThat(pool.trash).isEmpty();
            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(51);
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection connection1 = pool.connections.get(0);

            List<MockRequest> requests = MockRequest.sendMany(NEW_CONNECTION_THRESHOLD, pool);
            assertBorrowedConnection(requests, connection1);
            allRequests.addAll(requests);
            allRequests.add(MockRequest.send(pool));

            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 2);
            reset(factory);
            Connection connection2 = pool.connections.get(1);

            assertThat(connection1.inFlight.get()).isEqualTo(101);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Go back under the capacity of 1 connection
            MockRequest.completeMany(51, allRequests);

            assertThat(connection1.inFlight.get()).isEqualTo(50);
            assertThat(connection2.inFlight.get()).isEqualTo(0);

            // Given enough time, one connection gets trashed (and the implementation picks the first one)
            Uninterruptibles.sleepUninterruptibly(20, TimeUnit.SECONDS);
            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).containsExactly(connection1);

            // Return trashed connection down to 0 inFlight
            MockRequest.completeMany(50, allRequests);
            assertThat(connection1.inFlight.get()).isEqualTo(0);

            // Give enough time for trashed connection to be cleaned up from the trash:
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
            assertThat(pool.connections).containsExactly(connection2);
            assertThat(pool.trash).isEmpty();
            assertThat(connection1.isClosed()).isTrue();

            // Fill the live connection to go over the threshold where a second one is needed
            requests = MockRequest.sendMany(NEW_CONNECTION_THRESHOLD, pool);
            assertBorrowedConnection(requests, connection2);
            allRequests.addAll(requests);
            allRequests.add(MockRequest.send(pool));
            assertThat(connection2.inFlight.get()).isEqualTo(101);
            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 2);

            // Borrow again to get the new connection
            MockRequest request = MockRequest.send(pool);
            allRequests.add(request);
            assertThat(request.getConnection())
                    .isNotEqualTo(connection2) // should not be the full connection
                    .isNotEqualTo(connection1); // should not be the previously trashed one
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();

        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection connection1 = pool.connections.get(0);

            // Fill core connection enough to trigger creation of another one
            List<MockRequest> requests = MockRequest.sendMany(NEW_CONNECTION_THRESHOLD, pool);
            assertBorrowedConnections(requests, Collections.singletonList(connection1));
            allRequests.addAll(requests);
            allRequests.add(MockRequest.send(pool));

            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(2);

            // Return enough times to get back under the threshold where one connection is enough
            MockRequest.completeMany(50, allRequests);

            // Give enough time for one connection to be trashed. Due to the implementation, this will be the first one.
            // It still has in-flight requests so should not get closed.
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);
            assertThat(pool.trash).containsExactly(connection1);
            assertThat(connection1.inFlight.get()).isEqualTo(51);
            assertThat(connection1.isClosed()).isFalse();

            // Consume all inFlight requests on the trashed connection.
            MockRequest.completeMany(51, allRequests);

            // Sleep enough time for the connection to be consider idled and closed.
            Uninterruptibles.sleepUninterruptibly(30, TimeUnit.SECONDS);

            // The connection should be now closed.
            // The trashed connection should be closed and not in the pool or trash.
            assertThat(connection1.isClosed()).isTrue();
            assertThat(pool.connections).doesNotContain(connection1);
            assertThat(pool.trash).doesNotContain(connection1);
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();
        try {
            HostConnectionPool pool = createPool(cluster, 1, 2);
            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;
            Connection core = pool.connections.get(0);

            List<MockRequest> requests = MockRequest.sendMany(NEW_CONNECTION_THRESHOLD, pool);
            assertBorrowedConnections(requests, Collections.singletonList(core));
            allRequests.addAll(requests);
            allRequests.add(MockRequest.send(pool));

            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
            assertThat(pool.connections).hasSize(2);

            // Grab the new non-core connection and replace it with a spy.
            Connection extra1 = spy(pool.connections.get(1));
            pool.connections.set(1, extra1);

            // Borrow 10 times to ensure pool is utilized.
            allRequests.addAll(MockRequest.sendMany(10, pool));
            assertThat(pool.connections).hasSize(2);

            // stub the maxAvailableStreams method to return 0, indicating there are no remaining streams.
            // this should cause the connection to be replaced and trashed on returnConnection.
            doReturn(0).when(extra1).maxAvailableStreams();

            // On returning of the connection, should detect that there are no available streams and trash it.
            assertThat(pool.trash).hasSize(0);
            pool.returnConnection(extra1);
            assertThat(pool.trash).hasSize(1);
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();
        try {
            cluster.init();

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            TestExecutorService blockingExecutor = new TestExecutorService(cluster.manager.blockingExecutor);
            cluster.manager.blockingExecutor = blockingExecutor;

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
            allRequests.add(request);
            assertThat(request.getConnection()).isEqualTo(core1);

            // Should not have tried to create a new core connection since reconnection time had not elapsed.
            verify(factory, never()).open(any(HostConnectionPool.class));

            // Sleep to elapse the Reconnection Policy.
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Attempt to borrow connection, this should trigger ensureCoreConnections thus spawning a new connection.
            blockingExecutor.reset();
            request = MockRequest.send(pool);
            allRequests.add(request);
            assertThat(request.getConnection()).isEqualTo(core1);

            // Should have tried to open up to core connections as result of borrowing a connection past reconnect time and not being at core.
            blockingExecutor.blockUntilNextTaskCompleted();
            verify(factory).open(any(HostConnectionPool.class));
            reset(factory);

            // Sleep for reconnect interval to allow reconnection time to elapse.
            Uninterruptibles.sleepUninterruptibly((readTimeout + reconnectInterval) * 2, TimeUnit.MILLISECONDS);

            // Enable listening so new connections succeed.
            currentClient.enableListener();
            // Sleep to elapse the Reconnection Policy.
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Try to borrow a connection, the pool should grow.
            blockingExecutor.reset();
            allRequests.add(MockRequest.send(pool));
            blockingExecutor.blockUntilNextTaskCompleted();
            verify(factory).open(any(HostConnectionPool.class));
            reset(factory);

            // Another core connection should be opened as result of another request to get us up to core connections.
            blockingExecutor.reset();
            allRequests.add(MockRequest.send(pool));
            blockingExecutor.blockUntilNextTaskCompleted();
            verify(factory).open(any(HostConnectionPool.class));
            reset(factory);

            // Sending another request should not grow the pool any more, since we are now at core connections.
            allRequests.add(MockRequest.send(pool));
            verify(factory, after((reconnectInterval + readTimeout) * 2).never()).open(any(HostConnectionPool.class));
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();
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
                assertThat(request.getConnection()).isEqualTo(core0);
                core0requests.add(request);
            }

            // Pool should grow by 1.
            verify(factory, after(2000).times(1)).open(any(HostConnectionPool.class));
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
                assertThat(request.state.get()).isEqualTo(MockRequest.State.FAILED);
            }

            assertThat(cluster).host(1).hasState(Host.State.UP);

            // Create enough inFlight requests to fill connection.
            List<MockRequest> requests = MockRequest.sendMany(100, pool);
            assertBorrowedConnections(requests, Collections.singletonList(extra1));
            allRequests.addAll(requests);
            assertThat(pool.connections).hasSize(1);

            // A new connection should never have been spawned since we didn't max out core.
            verify(factory, after(readTimeout).never()).open(any(HostConnectionPool.class));

            // Borrow another connection, since we exceed max another connection should be opened.
            MockRequest request = MockRequest.send(pool);
            allRequests.add(request);
            assertThat(request.getConnection()).isEqualTo(extra1);

            // After some time the a connection should attempt to be opened (but will fail).
            verify(factory, timeout(readTimeout)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 1);
            assertThat(pool.connections).hasSize(1);

            // Wait some reasonable amount of time for connection to reestablish then check pool size.
            Uninterruptibles.sleepUninterruptibly(readTimeout * 2, TimeUnit.MILLISECONDS);
            // Reconnecting failed since listening was enabled.
            assertPoolSize(pool, 1);

            // Re enable listening then wait for reconnect.
            currentClient.enableListener();
            Uninterruptibles.sleepUninterruptibly(reconnectInterval, TimeUnit.MILLISECONDS);

            // Borrow another connection, since we exceed max another connection should be opened.
            request = MockRequest.send(pool);
            allRequests.add(request);
            assertThat(request.getConnection()).isEqualTo(extra1);

            // Wait some reasonable amount of time for connection to reestablish then check pool size.
            Uninterruptibles.sleepUninterruptibly(readTimeout, TimeUnit.MILLISECONDS);
            // Reconnecting should have exceeded and pool will have grown.
            assertThat(pool.connections).hasSize(2);

            // Borrowed connection should be the newly spawned connection since the other one has some inflight requests.
            request = MockRequest.send(pool);
            allRequests.add(request);
            assertThat(request.getConnection()).isNotEqualTo(core0).isNotEqualTo(extra1);
        } finally {
            MockRequest.completeAll(allRequests);
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
        List<MockRequest> allRequests = newArrayList();
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
                allRequests.add(MockRequest.send(pool));
                verify(factory, timeout(readTimeout)).open(any(HostConnectionPool.class));
                reset(factory);
                assertPoolSize(pool, i);
            }
        } finally {
            MockRequest.completeAll(allRequests);
            cluster.close();
        }
    }

    /**
     * Ensures that if all connections fail on pool init that the host and subsequently the
     * control connection is not marked down since the control connection is still active.
     * The test also ensures that borrow attempts on the pool fail if we are still in the reconnection window
     * according to the ConvictionPolicy.
     *
     * @jira_ticket JAVA-544
     * @test_category connection:connection_pool
     * @since 2.0.11
     */
    @Test(groups = "short")
    public void should_throw_exception_if_convicted_and_no_connections_available() {
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

            MockRequest failedBorrow = MockRequest.send(pool);
            try {
                failedBorrow.getConnection();
                fail("Expected a BusyPoolException");
            } catch (BusyPoolException e) { /*expected*/}
        } finally {
            cluster.close();
        }
    }

    /**
     * Ensures that if all connections fail on pool init that the host and subsequently the
     * control connection is not marked down. The test also ensures that when making requests
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
            MockRequest request = MockRequest.send(pool, 1);

            // Should create up to core connections.
            verify(factory, timeout(readTimeout * 8).times(8)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 8);

            request.simulateSuccessResponse();
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

            Connection.Factory factory = spy(cluster.manager.connectionFactory);
            cluster.manager.connectionFactory = factory;

            HostConnectionPool pool = createPool(cluster, 0, 2);

            // Pool should be empty.
            assertThat(pool.connections).hasSize(0);

            // Control connection should stay up with the host.
            assertThat(cluster).host(1).hasState(Host.State.UP);
            assertThat(cluster).hasOpenControlConnection();

            reset(factory);
            MockRequest request = MockRequest.send(pool, 1);

            // Should create up to core connections.
            verify(factory, timeout(readTimeout).times(1)).open(any(HostConnectionPool.class));
            assertPoolSize(pool, 1);
            Uninterruptibles.getUninterruptibly(request.requestInitialized, 10, TimeUnit.SECONDS);
            request.simulateSuccessResponse();
        } finally {
            cluster.close();
        }
    }

    private HostConnectionPool createPool(Cluster cluster, int coreConnections, int maxConnections) {
        cluster.getConfiguration().getPoolingOptions()
                .setNewConnectionThreshold(HostDistance.LOCAL, 100)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 128)
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

        final ListenableFuture<Connection> connectionFuture;

        final ListenableFuture<Connection.ResponseHandler> requestInitialized;

        private volatile Connection.ResponseHandler responseHandler;

        final AtomicReference<State> state = new AtomicReference<State>(State.START);

        static MockRequest send(HostConnectionPool pool) {
            return send(pool, 0);
        }

        static MockRequest send(HostConnectionPool pool, int maxQueueSize) throws ConnectionException, BusyConnectionException {
            return send(pool, 5000, maxQueueSize);
        }

        static MockRequest send(HostConnectionPool pool, int timeoutMillis, int maxQueueSize) throws ConnectionException, BusyConnectionException {
            return new MockRequest(pool, timeoutMillis, maxQueueSize);
        }

        private static List<MockRequest> sendMany(int count, HostConnectionPool pool) throws ConnectionException {
            return sendMany(count, pool, 0);
        }

        private static List<MockRequest> sendMany(int count, HostConnectionPool pool, int maxQueueSize) throws ConnectionException {
            List<MockRequest> requests = newArrayList();
            for (int i = 0; i < count; i++) {
                MockRequest request = send(pool, maxQueueSize);
                requests.add(request);
            }
            return requests;
        }

        /**
         * Completes count requests by simulating a successful response.
         */
        private static void completeMany(int count, List<MockRequest> requests) {
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
        private static void completeAll(List<MockRequest> requests) {
            for (MockRequest request : requests) {
                request.simulateSuccessResponse();
            }
        }

        private MockRequest(HostConnectionPool pool, int timeoutMillis, int maxQueueSize) throws ConnectionException {
            this.connectionFuture = pool.borrowConnection(timeoutMillis, MILLISECONDS, maxQueueSize);
            requestInitialized = Futures.transform(this.connectionFuture, new Function<Connection, Connection.ResponseHandler>() {
                @Override
                public Connection.ResponseHandler apply(Connection connection) {
                    MockRequest thisRequest = MockRequest.this;
                    thisRequest.responseHandler = new Connection.ResponseHandler(connection, -1, thisRequest);
                    connection.dispatcher.add(thisRequest.responseHandler);
                    return responseHandler;
                }
            });
        }

        void simulateSuccessResponse() {
            onSet(getConnection(), null, 0, 0);
        }

        @SuppressWarnings("unused")
        void simulateErrorResponse() {
            onException(getConnection(), null, 0, 0);
        }

        @SuppressWarnings("unused")
        void simulateTimeout() {
            if (onTimeout(getConnection(), 0, 0))
                responseHandler.cancelHandler();
        }

        Connection getConnection() {
            try {
                return Uninterruptibles.getUninterruptibly(connectionFuture, 500, MILLISECONDS);
            } catch (ExecutionException e) {
                throw Throwables.propagate(e.getCause());
            } catch (TimeoutException e) {
                fail("Timed out getting connection");
                return null; // never reached
            }
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
            assertThat(connectionFuture.isDone()).isTrue();
            try {
                assertThat(Uninterruptibles.getUninterruptibly(connectionFuture)).isNotNull();
            } catch (ExecutionException e) {
                Throwables.propagate(e.getCause());
            }
            if (state.compareAndSet(State.START, State.COMPLETED)) {
                connection.dispatcher.removeHandler(responseHandler, true);
                connection.release();
            }
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency, int retryCount) {
            if (state.compareAndSet(State.START, State.FAILED)) {
                connection.dispatcher.removeHandler(responseHandler, true);
                connection.release();
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

    static class TestExecutorService extends ForwardingListeningExecutorService {

        private final ListeningExecutorService delegate;

        private final Semaphore semaphore = new Semaphore(0);

        TestExecutorService(ListeningExecutorService delegate) {
            this.delegate = delegate;
        }

        @Override
        protected ListeningExecutorService delegate() {
            return delegate;
        }

        public void reset() {
            semaphore.drainPermits();
        }

        public void blockUntilNextTaskCompleted() throws InterruptedException {
            semaphore.tryAcquire(1, 1, TimeUnit.MINUTES);
        }

        @Override
        public ListenableFuture<?> submit(Runnable task) {
            ListenableFuture<?> future = super.submit(task);
            Futures.addCallback(future, new FutureCallback<Object>() {
                @Override
                public void onSuccess(Object result) {
                    semaphore.release(1);
                }

                @Override
                public void onFailure(Throwable t) {
                    semaphore.release(1);
                }
            });
            return future;
        }
    }
}
