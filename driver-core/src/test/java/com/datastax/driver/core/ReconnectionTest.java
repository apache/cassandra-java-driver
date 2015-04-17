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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.Host.State;
import com.datastax.driver.core.policies.*;

import static com.datastax.driver.core.Assertions.assertThat;

/**
 * Scenarios where a Cluster loses connection to a host and reconnects.
 */
public class ReconnectionTest {

    @Test(groups = "long")
    public void should_reconnect_after_full_connectivity_loss() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 2);
            int reconnectionDelay = 1000;
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelay))
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

            assertThat(cluster).host(2).comesUpWithin(120, SECONDS);

            // Give the control connection a few moments to reconnect
            TimeUnit.MILLISECONDS.sleep(reconnectionDelay * 2);
            assertThat(cluster).usesControlHost(2);
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    @Test(groups = "long")
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
            assertThat(cluster).host(1).comesUpWithin(120, SECONDS);
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    @Test(groups = "long")
    public void should_cancel_reconnection_attempts() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;

        long reconnectionDelayMillis = 1000;
        CountingReconnectionPolicy reconnectionPolicy = new CountingReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelayMillis));

        try {
            ccm = CCMBridge.create("test", 2);
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withReconnectionPolicy(reconnectionPolicy)
                .build();
            cluster.connect();

            // Stop a node and cancel the reconnection attempts to it
            ccm.stop(2);
            Host host2 = TestUtils.findHost(cluster, 2);
            host2.getReconnectionAttemptFuture().cancel(false);

            // The reconnection count should not vary over time anymore
            int initialCount = reconnectionPolicy.count.get();
            TimeUnit.MILLISECONDS.sleep(reconnectionDelayMillis * 2);
            assertThat(reconnectionPolicy.count.get()).isEqualTo(initialCount);

            // Restart the node, which will trigger an UP notification
            ccm.start(2);
            ccm.waitForUp(2);

            // The driver should now see the node as UP again
            assertThat(cluster).host(2).comesUpWithin(120, SECONDS);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    @Test(groups = "long")
    public void should_trigger_one_time_reconnect() throws InterruptedException, IOException {
        CCMBridge ccm = null;
        Cluster cluster = null;

        long reconnectionDelayMillis = 1000;
        TogglabePolicy loadBalancingPolicy = new TogglabePolicy(new RoundRobinPolicy());

        try {
            ccm = CCMBridge.create("test", 1);
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelayMillis))
                .build();
            cluster.connect();

            // Tweak the LBP so that the control connection never reconnects, otherwise
            // it would interfere with the rest of the test (this is a bit of a hack)
            loadBalancingPolicy.returnEmptyQueryPlan = true;

            // Stop the node, ignore it and cancel reconnection attempts to it
            ccm.stop(1);
            ccm.waitForDown(1);
            assertThat(cluster).host(1).goesDownWithin(20, SECONDS);
            Host host1 = TestUtils.findHost(cluster, 1);
            loadBalancingPolicy.setDistance(TestUtils.findHost(cluster, 1), HostDistance.IGNORED);
            ListenableFuture<?> reconnectionAttemptFuture = host1.getReconnectionAttemptFuture();
            if (reconnectionAttemptFuture != null)
                reconnectionAttemptFuture.cancel(false);

            // Trigger a one-time reconnection attempt (this will fail)
            host1.tryReconnectOnce();

            // Wait for a few reconnection cycles before checking
            TimeUnit.MILLISECONDS.sleep(reconnectionDelayMillis * 2);
            assertThat(cluster).host(1).hasState(State.DOWN);

            // Restart the node (this will not trigger an UP notification thanks to our
            // hack to disable the control connection reconnects). The host should stay
            // down for the driver.
            ccm.start(1);
            ccm.waitForUp(1);
            assertThat(cluster).host(1).hasState(State.DOWN);

            TimeUnit.SECONDS.sleep(Cluster.NEW_NODE_DELAY_SECONDS);
            assertThat(cluster).host(1).hasState(State.DOWN);

            // Trigger another one-time reconnection attempt (this will succeed). The
            // host should be back up.
            host1.tryReconnectOnce();
            assertThat(cluster).host(1).comesUpWithin(120, SECONDS);
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * The connection established by a successful reconnection attempt should be reused in one of the
     * connection pools (JAVA-505).
     */
    @Test(groups = "long")
    public void should_use_connection_from_reconnection_in_pool() {
        CCMBridge ccm = null;
        Cluster cluster = null;

        TogglabePolicy loadBalancingPolicy = new TogglabePolicy(new RoundRobinPolicy());

        // Spy SocketOptions.getKeepAlive to count how many connections were instantiated.
        SocketOptions socketOptions = spy(new SocketOptions());

        try {
            ccm = CCMBridge.create("test", 1);
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(1))
                .withReconnectionPolicy(new ConstantReconnectionPolicy(5000))
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withSocketOptions(socketOptions)
                .build();
            // Create two sessions to have multiple pools
            cluster.connect();
            cluster.connect();

            // Right after init, 1 connection has been opened by the control connection, and 2 (core size) for each pool.
            verify(socketOptions, times(5)).getKeepAlive();

            // Tweak the LBP so that the control connection never reconnects. This makes it easier
            // to reason about the number of connection attempts.
            loadBalancingPolicy.returnEmptyQueryPlan = true;

            // Stop the node and cancel the reconnection attempts to it
            ccm.stop(1);
            ccm.waitForDown(1);
            assertThat(cluster).host(1).goesDownWithin(20, SECONDS);
            Host host1 = TestUtils.findHost(cluster, 1);
            host1.getReconnectionAttemptFuture().cancel(false);

            ccm.start(1);
            ccm.waitForUp(1);

            // Reset the spy and count the number of connections attempts for 1 reconnect
            reset(socketOptions);
            host1.tryReconnectOnce();
            assertThat(cluster).host(1).comesUpWithin(120, SECONDS);
            // Expect 1 connection from the reconnection attempt  3 for the pools (we need 4
            // but the one from the reconnection attempt gets reused).
            verify(socketOptions, times(4)).getKeepAlive();
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
                count.incrementAndGet();
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

    /**
     * A load balancing policy that:
     * - can be "disabled" by having its query plan return no hosts.
     * - can be instructed to return a specific distance for some hosts.
     */
    public static class TogglabePolicy extends DelegatingLoadBalancingPolicy {

        volatile boolean returnEmptyQueryPlan;
        final ConcurrentMap<Host, HostDistance> distances = new ConcurrentHashMap<Host, HostDistance>();

        public TogglabePolicy(LoadBalancingPolicy delegate) {
            super(delegate);
        }

        @Override
        public HostDistance distance(Host host) {
            HostDistance distance = distances.get(host);
            return (distance != null)
                ? distance
                : super.distance(host);
        }

        public void setDistance(Host host, HostDistance distance) {
            distances.put(host, distance);
        }

        @Override
        public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
            if (returnEmptyQueryPlan)
                return Collections.<Host>emptyList().iterator();
            else
                return super.newQueryPlan(loggedKeyspace, statement);
        }
    }
}
