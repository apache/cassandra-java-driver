package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.codahale.metrics.Gauge;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class HostConnectionPoolTimeoutTest {
    @Test(groups = "long")
    public void should_wait_for_timeout_before_trashing_connections() throws Exception {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test");
            ccm.populate(1);
            ccm.updateConfig("authenticator", "PasswordAuthenticator");
            ccm.start(1, "-Dcassandra.superuser_setup_delay_ms=0");

            // Hijack the auth provider to be notified each time a connection instance is built
            CountingAuthProvider authProvider = new CountingAuthProvider("cassandra", "cassandra");
            AtomicInteger createdConnections = authProvider.count;

            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withAuthProvider(authProvider)
                .build();

            PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
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

            // Borrow one more time, which should spawn a new connection
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

            // If we return and wait long enough, it should eventually get trashed
            pool.returnConnection(nonCoreConnection);
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

    /**
     * An auth provider that counts how many authenticators it has returned.
     */
    static class CountingAuthProvider extends PlainTextAuthProvider {
        final AtomicInteger count = new AtomicInteger();

        public CountingAuthProvider(String username, String password) {
            super(username, password);
        }

        @Override public Authenticator newAuthenticator(InetSocketAddress host) {
            count.incrementAndGet();
            return super.newAuthenticator(host);
        }
    }
}