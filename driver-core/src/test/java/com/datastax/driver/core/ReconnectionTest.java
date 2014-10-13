package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * Scenarios where a Cluster loses connection to a host and reconnects.
 */
public class ReconnectionTest {

    @Test(groups = "short")
    public void should_reconnect_after_full_connectivity_lost() {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 2);
            cluster = Cluster.builder()
                             .addContactPoint(CCMBridge.ipOfNode(1))
                             .withReconnectionPolicy(new ConstantReconnectionPolicy(1000))
                             .build();
            cluster.connect();

            assertThat(cluster).usesControlHost(1);

            // Stop all nodes. We won't get notifications anymore, so the only mechanism to
            // reconnect is the background reconnection attempts.
            ccm.stop(2);
            ccm.stop(1);

            ccm.waitForDown(2);
            ccm.start(2);
            ccm.waitForUp(2);

            TestUtils.waitFor(CCMBridge.ipOfNode(2), cluster);
            assertThat(cluster).usesControlHost(2);
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    @Test(groups = "short")
    public void should_keep_reconnecting_on_authentication_error() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test");
            ccm.populate(1);
            // Configure password authentication on the server (default: cassandra / cassandra)
            ccm.updateConfig("authenticator", "PasswordAuthenticator");
            ccm.start(1, "-Dcassandra.superuser_setup_delay_ms=0");

            DynamicPlainTextAuthProvider authProvider = new DynamicPlainTextAuthProvider("cassandra", "cassandra");
            int reconnectionDelayMs = 1000;
            CountingReconnectionPolicy reconnectionPolicy = new CountingReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelayMs));

            cluster = Cluster.builder()
                            .addContactPoint(CCMBridge.ipOfNode(1))
                            // Start with the correct auth so that we can initialize the server
                            .withAuthProvider(authProvider)
                            .withReconnectionPolicy(reconnectionPolicy)
                            .build();

            cluster.init();
            assertThat(cluster).usesControlHost(1);

            // Stop the server, set wrong credentials and restart
            ccm.stop(1);
            ccm.waitForDown(1);
            authProvider.password = "wrongPassword";
            ccm.start(1);
            ccm.waitForUp(1);

            // Wait a few iterations to ensure that our authProvider has returned the wrong credentials at least once
            // NB: authentication errors show up in the logs
            int initialCount = authProvider.count.get();
            int iterations = 0, maxIterations = 12; // make sure we don't wait indefinitely
            do {
                iterations += 1;
                TimeUnit.SECONDS.sleep(5);
            } while (iterations < maxIterations && authProvider.count.get() <= initialCount);
            assertThat(iterations).isLessThan(maxIterations);

            // Fix the credentials
            authProvider.password = "cassandra";

            // The driver should eventually reconnect to the node
            TestUtils.waitFor(CCMBridge.ipOfNode(1), cluster);
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * An authentication provider that allows changing the credentials at runtime, and tracks how many times they have been requested.
     */
    static class DynamicPlainTextAuthProvider implements AuthProvider {
        volatile String username;
        volatile String password;

        final AtomicInteger count = new AtomicInteger();

        public DynamicPlainTextAuthProvider(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public Authenticator newAuthenticator(InetSocketAddress host) {
            return new PlainTextAuthenticator();
        }

        private class PlainTextAuthenticator extends ProtocolV1Authenticator implements Authenticator {

            @Override
            public byte[] initialResponse() {
                count.incrementAndGet();

                byte[] usernameBytes = username.getBytes(Charsets.UTF_8);
                byte[] passwordBytes = password.getBytes(Charsets.UTF_8);
                byte[] initialToken = new byte[usernameBytes.length + passwordBytes.length + 2];
                initialToken[0] = 0;
                System.arraycopy(usernameBytes, 0, initialToken, 1, usernameBytes.length);
                initialToken[usernameBytes.length + 1] = 0;
                System.arraycopy(passwordBytes, 0, initialToken, usernameBytes.length + 2, passwordBytes.length);
                return initialToken;
            }

            @Override
            public byte[] evaluateChallenge(byte[] challenge) {
                return null;
            }

            @Override
            public void onAuthenticationSuccess(byte[] token) {
                // no-op, the server should send nothing anyway
            }

            @Override
            Map<String, String> getCredentials() {
                return ImmutableMap.of("username", username,
                                       "password", password);
            }
        }
    }

    /**
     * A reconnection policy that tracks how many times its schedule has been invoked.
     */
    public static class CountingReconnectionPolicy implements ReconnectionPolicy {
        public final AtomicInteger count = new AtomicInteger();
        private final ReconnectionPolicy childPolicy;

        public CountingReconnectionPolicy(ReconnectionPolicy childPolicy) {
            this.childPolicy = childPolicy;
        }

        @Override
        public ReconnectionSchedule newSchedule() {
            return new CountingSchedule(childPolicy.newSchedule());
        }

        class CountingSchedule implements ReconnectionSchedule {
            private final ReconnectionSchedule childSchedule;

            public CountingSchedule(ReconnectionSchedule childSchedule) {
                this.childSchedule = childSchedule;
            }

            @Override
            public long nextDelayMs() {
                count.incrementAndGet();
                return childSchedule.nextDelayMs();
            }
        }
    }
}
