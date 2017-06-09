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

import com.datastax.driver.core.exceptions.AuthenticationException;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.log4j.Level;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.findHost;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

/**
 * Tests for authenticated cluster access
 */
@CreateCCM(PER_METHOD)
@CCMConfig(
        config = "authenticator:PasswordAuthenticator",
        jvmArgs = "-Dcassandra.superuser_setup_delay_ms=0",
        createCluster = false)
public class AuthenticationTest extends CCMTestsSupport {

    @BeforeMethod(groups = "short")
    public void sleepIf12() {
        // For C* 1.2, sleep before attempting to connect as there is a small delay between
        // user being created.
        if (ccm().getCassandraVersion().getMajor() < 2) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }
    }

    @Test(groups = "short")
    public void should_connect_with_credentials() throws InterruptedException {
        PlainTextAuthProvider authProvider = spy(new PlainTextAuthProvider("cassandra", "cassandra"));
        Cluster cluster = Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withAuthProvider(authProvider)
                .build();
        cluster.connect();
        verify(authProvider, atLeastOnce()).newAuthenticator(findHost(cluster, 1).getSocketAddress(), "org.apache.cassandra.auth.PasswordAuthenticator");
        assertThat(cluster.getMetrics().getErrorMetrics().getAuthenticationErrors().getCount()).isEqualTo(0);
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_with_wrong_credentials() throws InterruptedException {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withCredentials("bogus", "bogus")
                .build());
        try {
            cluster.connect();
        } finally {
            assertThat(cluster.getMetrics().getErrorMetrics().getAuthenticationErrors().getCount()).isEqualTo(1);
        }
    }

    @Test(groups = "short", expectedExceptions = AuthenticationException.class)
    public void should_fail_to_connect_without_credentials() throws InterruptedException {
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .build());
        try {
            cluster.connect();
        } finally {
            assertThat(cluster.getMetrics().getErrorMetrics().getAuthenticationErrors().getCount()).isEqualTo(1);
        }
    }

    /**
     * Ensures that authentication is possible even if the server is busy during
     * SASL handshake.
     *
     * @jira_ticket JAVA-1429
     */
    @Test(groups = "short")
    @CCMConfig(dirtiesContext = true)
    public void should_connect_with_slow_server() throws InterruptedException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withAuthProvider(new SlowAuthProvider())
                .withPoolingOptions(new PoolingOptions()
                        .setHeartbeatIntervalSeconds(1))
                .build();
        cluster.connect();
    }

    private class SlowAuthProvider extends PlainTextAuthProvider {

        public SlowAuthProvider() {
            super("cassandra", "cassandra");
        }

        @Override
        public Authenticator newAuthenticator(InetSocketAddress host, String authenticator) throws AuthenticationException {
            simulateBusyServer();
            return super.newAuthenticator(host, authenticator);
        }

    }

    private void simulateBusyServer() {
        ccm().pause(1);
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                ccm().resume(1);
            }
        }, 2000);
    }

    /**
     * Ensures that when a host replies with AuthenticationException
     * during connection pool initialization the pool creation is aborted.
     *
     * @jira_ticket JAVA-1431
     */
    @Test(groups = "short")
    public void should_not_create_pool_with_wrong_credentials() throws InterruptedException {
        PlainTextAuthProvider authProvider = new PlainTextAuthProvider("cassandra", "cassandra");
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints())
                .withPort(ccm().getBinaryPort())
                .withAuthProvider(authProvider)
                .build());
        cluster.init();
        authProvider.setPassword("wrong");
        Level previous = TestUtils.setLogLevel(Session.class, Level.WARN);
        MemoryAppender logs = new MemoryAppender().enableFor(Session.class);
        Session session;
        try {
            session = cluster.connect();
        } finally {
            TestUtils.setLogLevel(Session.class, previous);
            logs.disableFor(Session.class);
        }
        assertThat(session.getState().getConnectedHosts()).isEmpty();
        InetSocketAddress host = ccm().addressOfNode(1);
        assertThat(logs.get())
                .contains(
                        "Error creating pool to " + host,
                        "Authentication error on host " + host,
                        AuthenticationException.class.getSimpleName());
    }

}
