package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.PrimingRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class HostConnectionPoolTimeoutTest {

    private static final Logger logger = LoggerFactory.getLogger(HostConnectionPoolTimeoutTest.class);

    @Test(groups = "long")
    public void should_wait_for_idle_timeout_before_trashing_connections() throws Exception {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test");
            ccm.populate(1);
            ccm.updateConfig("authenticator", "PasswordAuthenticator");
            ccm.start(1, "-Dcassandra.superuser_setup_delay_ms=0");

            // Hijack the auth provider to be notified each time a connection instance is built
            CountingAuthProvider authProvider = new CountingAuthProvider("cassandra", "cassandra");
            AtomicInteger createdConnections = authProvider.newAuthenticatorCount;

            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withAuthProvider(authProvider)
                .build();

            PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
            poolingOptions.setMinSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 1);
            // Set to the max, which makes it slightly easier to reason about when new connections will be created:
            poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 128);
            int idleTimeoutSeconds = 10;
            poolingOptions.setIdleTimeoutSeconds(idleTimeoutSeconds);

            Gauge<Integer> openConnections = cluster.getMetrics().getOpenConnections();

            Session session = cluster.connect();

            Host host1 = TestUtils.findHost(cluster, 1);
            HostConnectionPool pool = ((SessionManager)session).pools.get(host1);

            Set<PooledConnection> coreConnections = new HashSet<PooledConnection>();

            // Idle pool: 2 core connections + control connection
            assertThat(openConnections.getValue()).isEqualTo(3);
            assertThat(createdConnections.get()).isEqualTo(3);

            // Max out the two core connections
            for (int i = 0; i < 2 * 128; i++) {
                PooledConnection connection = pool.borrowConnection(1, TimeUnit.SECONDS);
                coreConnections.add(connection);
            }

            assertThat(coreConnections).hasSize(2);

            assertThat(openConnections.getValue()).isEqualTo(3);
            assertThat(createdConnections.get()).isEqualTo(3);

            // Borrow one more stream, which should spawn a new connection
            PooledConnection nonCoreConnection = pool.borrowConnection(1, TimeUnit.SECONDS);
            assertThat(coreConnections).doesNotContain(nonCoreConnection);
            assertThat(openConnections.getValue()).isEqualTo(4);
            assertThat(createdConnections.get()).isEqualTo(4);

            // Return the connection, it should not be trashed immediately
            pool.returnConnection(nonCoreConnection);
            TimeUnit.SECONDS.sleep(idleTimeoutSeconds / 2);
            assertThat(openConnections.getValue()).isEqualTo(4);
            assertThat(createdConnections.get()).isEqualTo(4);

            // If we borrow again at this point, it should return the same connection
            PooledConnection nonCoreConnection2 = pool.borrowConnection(1, TimeUnit.SECONDS);
            assertThat(nonCoreConnection2).isSameAs(nonCoreConnection);
            assertThat(openConnections.getValue()).isEqualTo(4);
            assertThat(createdConnections.get()).isEqualTo(4);

            // Borrow two more and return 1 to reach inFlight = 2 (above the min). The connection should not be trashed.
            assertThat(pool.borrowConnection(1, TimeUnit.SECONDS)).isSameAs(nonCoreConnection2);
            assertThat(pool.borrowConnection(1, TimeUnit.SECONDS)).isSameAs(nonCoreConnection2);
            pool.returnConnection(nonCoreConnection2); // (we need to return one because we cancel the timeout on return)
            TimeUnit.SECONDS.sleep(idleTimeoutSeconds * 2);
            assertThat(openConnections.getValue()).isEqualTo(4);
            assertThat(createdConnections.get()).isEqualTo(4);

            // Return everything for the non-core connection and wait long enough, it should eventually get trashed
            pool.returnConnection(nonCoreConnection);
            pool.returnConnection(nonCoreConnection);
            TimeUnit.SECONDS.sleep(idleTimeoutSeconds * 2);
            assertThat(openConnections.getValue()).isEqualTo(3);
            assertThat(createdConnections.get()).isEqualTo(4);

            // Return all the streams of the core connections, they should not get trashed
            for (PooledConnection coreConnection : coreConnections)
                for (int i = 0; i < 128; i++)
                    pool.returnConnection(coreConnection);
            assertThat(session.getState().getInFlightQueries(host1)).isEqualTo(0);

            TimeUnit.SECONDS.sleep(idleTimeoutSeconds * 2);
            assertThat(openConnections.getValue()).isEqualTo(3);
            assertThat(createdConnections.get()).isEqualTo(4);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    @Test(groups = "short")
    public void should_timeout_immediately_if_pool_timeout_is_zero() throws Exception {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 1);
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .build();

            cluster.getConfiguration().getPoolingOptions()
                .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                .setMaxConnectionsPerHost(HostDistance.LOCAL, 1)
                .setPoolTimeoutMillis(0);

            Session session = cluster.connect();

            Host host1 = TestUtils.findHost(cluster, 1);

            // Borrow all streams to simulate a busy pool (the timeout doesn't matter here)
            HostConnectionPool pool = ((SessionManager)session).pools.get(host1);
            for (int i = 0; i < 128; i++)
                pool.borrowConnection(1, TimeUnit.SECONDS);

            boolean threw = false;
            long beforeQuery = System.nanoTime();
            try {
                session.execute("select release_version from system.local");
            } catch (NoHostAvailableException e) {
                long afterQuery = System.nanoTime();
                // This is a bit arbitrary, but we can expect that the time to find out there is no available connection
                // is less than 20 ms
                assertThat(afterQuery - beforeQuery).isLessThan(20 * 1000 * 1000);
                Throwable host1Error = e.getErrors().get(host1.getSocketAddress());
                assertThat(host1Error).isInstanceOf(DriverException.class);
                assertThat(host1Error.getMessage()).contains("Timeout while trying to acquire available connection");
                threw = true;
            }
            assertThat(threw).isTrue();
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * Test that an unused connection is properly cleaned up with the configured idle timeout.
     *
     * Sends queries that are cancelled on a single connection which causes the connection to be replaced
     * with a new one that is never used as no more queries are sent.  Validates that the unused connection is cleaned
     * up after the idle timeout.
     */
    @Test(groups="short")
    public void should_timeout_unused_connection() throws Exception {
        int idleTimeoutSeconds = 10;
        int requestThreshold = 128;

        // Create a simulated cassandra instance that responds to a particular query slowly.  This provides
        // headroom to cancel queries, in addition to allowing us to fill up maxSimultaneousRequestsPerConnection
        // on a connection, creating more connections in a HostConnectionPool.
        String slowQuery = "select key from table";
        int slowQuerySeconds = 15;
        Scassandra scassandra = TestUtils.createScassandraServer();
        Cluster cluster = null;

        try {
            scassandra.start();
            scassandra.primingClient().prime(
                    PrimingRequest.queryBuilder()
                            .withQuery(slowQuery)
                            .withRows(ImmutableMap.of("key", 1))
                            .withFixedDelay(slowQuerySeconds * 1000)
                            .build()
            );

            // Set the read timeout to the query time * 2 to allow slow queries.
            SocketOptions socketOptions = new SocketOptions().setReadTimeoutMillis(slowQuerySeconds*1000*2);

            // Configure a core pool size of 1 to reduce the complexity of the test.
            PoolingOptions poolOptions = new PoolingOptions()
                    .setCoreConnectionsPerHost(HostDistance.LOCAL, 1)
                    .setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, requestThreshold)
                    .setIdleTimeoutSeconds(idleTimeoutSeconds);

            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .withPort(scassandra.getBinaryPort())
                    .withPoolingOptions(poolOptions)
                    .withSocketOptions(socketOptions)
                    .build();

            Session session = cluster.connect();

            // Fill up the core connection.
            for(int i = 0; i < requestThreshold; i++) {
                session.executeAsync(slowQuery);
            }

            // Sleep to ensure queries are submitted.
            TimeUnit.MILLISECONDS.sleep(100);

            // Grab the single host's connection pool.
            HostConnectionPool pool = ((SessionManager)session).pools.values().iterator().next();

            // The pool should be at one since we did not submit beyond the threshold.
            assertThat(pool.connections.size()).isEqualTo(1);

            // Submit 75 requests and cancel them near immediately, this creates enough friction to cause the stream ids
            // on the connection to be exhausted.
            List<ResultSetFuture> toCancel = Lists.newArrayList();
            for(int i = 0; i < 75; i++) {
                toCancel.add(session.executeAsync(slowQuery));
            }

            // Sleep to ensure queries are submitted.
            TimeUnit.MILLISECONDS.sleep(100);

            // The pool size should grow by 1 after enqueuing requests beyond the threshold.
            assertThat(pool.connections.size()).isEqualTo(2);

            // Cancel these requests, this should trigger the connection that was assigned these queries to exhaust it's
            // stream ids and become replaced with a new connection.
            for(ResultSetFuture future : toCancel) {
                future.cancel(true);
            }

            // Sleep for idleTimeout * 2 seconds to anticipate idle connection cleanup.
            TimeUnit.SECONDS.sleep(idleTimeoutSeconds * 2);

            // Log the remaining connections for debugging.
            for(PooledConnection c : pool.connections) {
                logger.info("Remaining connection: {} [trashTime={}, now={}]", c, c.getTrashTime(),
                        System.currentTimeMillis());
            }

            // Ensure that the new unused connection was trashed after idleTimeout of never being used.
            // If not there will be 2 connections here.
            assertThat(pool.connections.size()).isEqualTo(1);

        } finally {
            if(cluster != null) {
                cluster.close();
            }
            scassandra.stop();
        }

    }

    /**
     * An auth provider that counts how many authenticators it has returned.
     */
    static class CountingAuthProvider extends PlainTextAuthProvider {
        final AtomicInteger newAuthenticatorCount = new AtomicInteger();

        public CountingAuthProvider(String username, String password) {
            super(username, password);
        }

        @Override public Authenticator newAuthenticator(InetSocketAddress host) {
            newAuthenticatorCount.incrementAndGet();
            return super.newAuthenticator(host);
        }
    }
}