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

import com.datastax.driver.core.Host.State;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.*;

/**
 * Scenarios where a Cluster loses connection to a host and reconnects.
 */
@CreateCCM(PER_METHOD)
public class ReconnectionTest extends CCMTestsSupport {

    private final int reconnectionDelayMillis = 1000;

    @CCMConfig(dirtiesContext = true, numberOfNodes = 2, createCluster = false)
    @Test(groups = "long")
    public void should_reconnect_after_full_connectivity_loss() throws InterruptedException {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelayMillis))
                .build());
        cluster.connect();

        assertThat(cluster).usesControlHost(1);

        // Stop all nodes. We won't get notifications anymore, so the only mechanism to
        // reconnect is the background reconnection attempts.
        ccm().stop(2);
        ccm().stop(1);

        ccm().waitForDown(2);
        ccm().start(2);
        ccm().waitForUp(2);

        assertThat(cluster).host(2).comesUpWithin(Cluster.NEW_NODE_DELAY_SECONDS * 2, SECONDS);

        // Give the control connection a few moments to reconnect
        TimeUnit.MILLISECONDS.sleep(reconnectionDelayMillis * 2);
        assertThat(cluster).usesControlHost(2);
    }

    @CCMConfig(
            dirtiesContext = true,
            config = "authenticator:PasswordAuthenticator",
            jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0",
            createCluster = false)
    @Test(groups = "long")
    public void should_keep_reconnecting_on_authentication_error() throws InterruptedException {
        // For C* 1.2, sleep before attempting to connect as there is a small delay between
        // user being created.
        if (ccm().getCassandraVersion().getMajor() < 2) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
        CountingReconnectionPolicy reconnectionPolicy = new CountingReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelayMillis));
        CountingAuthProvider authProvider = new CountingAuthProvider("cassandra", "cassandra");
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                // Start with the correct auth so that we can initialize the server
                .withAuthProvider(authProvider)
                .withReconnectionPolicy(reconnectionPolicy)
                .build());

        cluster.init();
        assertThat(cluster).usesControlHost(1);

        // Stop the server, set wrong credentials and restart
        ccm().stop(1);
        ccm().waitForDown(1);
        authProvider.setPassword("wrongPassword");
        ccm().start(1);
        ccm().waitForUp(1);

        // Wait a few iterations to ensure that our authProvider has returned the wrong credentials at least twice
        // NB: authentication errors show up in the logs
        int initialCount = authProvider.count.get();
        long initialMetricCount = cluster.getMetrics().getErrorMetrics().getAuthenticationErrors().getCount();
        int iterations = 0, maxIterations = 12; // make sure we don't wait indefinitely
        do {
            iterations += 1;
            TimeUnit.SECONDS.sleep(5);
        } while (iterations < maxIterations && authProvider.count.get() <= initialCount + 1);
        assertThat(iterations).isLessThan(maxIterations);
        // Number of authentication errors should have increased.
        assertThat(cluster.getMetrics().getErrorMetrics().getAuthenticationErrors().getCount())
                .isGreaterThan(initialMetricCount);

        // Fix the credentials
        authProvider.setPassword("cassandra");

        // The driver should eventually reconnect to the node
        assertThat(cluster).host(1).comesUpWithin(Cluster.NEW_NODE_DELAY_SECONDS * 2, SECONDS);
    }

    @CCMConfig(dirtiesContext = true, numberOfNodes = 2, createCluster = false)
    @Test(groups = "long")
    public void should_cancel_reconnection_attempts() throws InterruptedException {
        // Stop a node and cancel the reconnection attempts to it
        CountingReconnectionPolicy reconnectionPolicy = new CountingReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelayMillis));

        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withReconnectionPolicy(reconnectionPolicy).build());
        cluster.connect();

        // Stop a node and cancel the reconnection attempts to it
        ccm().stop(2);
        Host host2 = TestUtils.findHost(cluster, 2);
        host2.getReconnectionAttemptFuture().cancel(false);

        // The reconnection count should not vary over time anymore
        int initialCount = reconnectionPolicy.count.get();
        TimeUnit.MILLISECONDS.sleep(reconnectionDelayMillis * 2);
        assertThat(reconnectionPolicy.count.get()).isEqualTo(initialCount);

        // Restart the node, which will trigger an UP notification
        ccm().start(2);
        ccm().waitForUp(2);

        // The driver should now see the node as UP again
        assertThat(cluster).host(2).comesUpWithin(Cluster.NEW_NODE_DELAY_SECONDS * 2, SECONDS);
    }

    @CCMConfig(dirtiesContext = true, createCluster = false)
    @Test(groups = "long")
    public void should_trigger_one_time_reconnect() throws InterruptedException, IOException {
        TogglabePolicy loadBalancingPolicy = new TogglabePolicy(new RoundRobinPolicy());
        Cluster cluster = register(Cluster.builder()
                .addContactPointsWithPorts(ccm().addressOfNode(1))
                .withPort(ccm().getBinaryPort())
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withReconnectionPolicy(new ConstantReconnectionPolicy(reconnectionDelayMillis))
                .build());
        cluster.connect();

        // Tweak the LBP so that the control connection never reconnects, otherwise
        // it would interfere with the rest of the test (this is a bit of a hack)
        loadBalancingPolicy.returnEmptyQueryPlan = true;

        // Stop the node, ignore it and cancel reconnection attempts to it
        ccm().stop(1);
        ccm().waitForDown(1);
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
        ccm().start(1);
        ccm().waitForUp(1);
        assertThat(cluster).host(1).hasState(State.DOWN);

        TimeUnit.SECONDS.sleep(Cluster.NEW_NODE_DELAY_SECONDS);
        assertThat(cluster).host(1).hasState(State.DOWN);

        // Trigger another one-time reconnection attempt (this will succeed). The
        // host should be back up.
        host1.tryReconnectOnce();
        assertThat(cluster).host(1).comesUpWithin(Cluster.NEW_NODE_DELAY_SECONDS * 2, SECONDS);
    }

    /**
     * The connection established by a successful reconnection attempt should be reused in one of the
     * connection pools (JAVA-505).
     */
    @CCMConfig(dirtiesContext = true, createCluster = false)
    @Test(groups = "long")
    public void should_use_connection_from_reconnection_in_pool() {
        TogglabePolicy loadBalancingPolicy = new TogglabePolicy(new RoundRobinPolicy());

        // Spy SocketOptions.getKeepAlive to count how many connections were instantiated.
        SocketOptions socketOptions = spy(new SocketOptions());
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withReconnectionPolicy(new ConstantReconnectionPolicy(5000))
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .withSocketOptions(socketOptions)
                .withProtocolVersion(ccm().getProtocolVersion())
                .build());
        // Create two sessions to have multiple pools
        cluster.connect();
        cluster.connect();

        int corePoolSize = TestUtils.numberOfLocalCoreConnections(cluster);

        // Right after init, 1 connection has been opened by the control connection, and the core size for each pool.
        verify(socketOptions, times(1 + corePoolSize * 2)).getKeepAlive();

        // Tweak the LBP so that the control connection never reconnects. This makes it easier
        // to reason about the number of connection attempts.
        loadBalancingPolicy.returnEmptyQueryPlan = true;

        // Stop the node and cancel the reconnection attempts to it
        ccm().stop(1);
        ccm().waitForDown(1);
        assertThat(cluster).host(1).goesDownWithin(20, SECONDS);
        Host host1 = TestUtils.findHost(cluster, 1);
        host1.getReconnectionAttemptFuture().cancel(false);

        ccm().start(1);
        ccm().waitForUp(1);

        // Reset the spy and count the number of connections attempts for 1 reconnect
        reset(socketOptions);
        host1.tryReconnectOnce();
        assertThat(cluster).host(1).comesUpWithin(Cluster.NEW_NODE_DELAY_SECONDS * 2, SECONDS);
        // Expect 1 connection from the reconnection attempt  3 for the pools (we need 4
        // but the one from the reconnection attempt gets reused).
        verify(socketOptions, times(corePoolSize * 2)).getKeepAlive();
    }

    /**
     * Extends the plain text auth provider to track how many times the credentials have been requested
     */
    static class CountingAuthProvider extends PlainTextAuthProvider {
        final AtomicInteger count = new AtomicInteger();

        CountingAuthProvider(String username, String password) {
            super(username, password);
        }

        @Override
        public Authenticator newAuthenticator(InetSocketAddress host, String authenticator) {
            count.incrementAndGet();
            return super.newAuthenticator(host, authenticator);
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
