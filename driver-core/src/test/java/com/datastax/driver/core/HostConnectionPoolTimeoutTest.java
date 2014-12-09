package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

public class HostConnectionPoolTimeoutTest {
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