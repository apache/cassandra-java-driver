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

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.datastax.driver.core.Assertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.fail;
import static org.scassandra.http.client.PrimingRequest.queryBuilder;
import static org.scassandra.http.client.PrimingRequest.then;

public class HeartbeatTest extends ScassandraTestBase {

    static org.slf4j.Logger logger = LoggerFactory.getLogger(HeartbeatTest.class);
    Logger connectionLogger = Logger.getLogger(Connection.class);
    MemoryAppender logs;
    Level originalLevel;

    @BeforeMethod(groups = "long")
    public void startCapturingLogs() {
        originalLevel = connectionLogger.getLevel();
        connectionLogger.setLevel(Level.DEBUG);
        logs = new MemoryAppender();
        connectionLogger.addAppender(logs);
    }

    @AfterMethod(groups = "long", alwaysRun = true)
    public void stopCapturingLogs() {
        connectionLogger.setLevel(originalLevel);
        connectionLogger.removeAppender(logs);
    }

    /**
     * Ensures that a heartbeat message is sent after the configured heartbeat interval of idle time and succeeds and
     * continues to be sent as long as the connection remains idle.
     *
     * @test_category connection:heartbeat
     * @expected_result heartbeat is sent after heartbeat interval (3) seconds of idle time.
     * @jira_ticket JAVA-533
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "long")
    public void should_send_heartbeat_when_connection_is_inactive() throws InterruptedException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(hostAddress.getAddress())
                .withPort(scassandra.getBinaryPort())
                .withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(3))
                .build();

        try {
            // Don't create any session, only the control connection will be established
            cluster.init();

            for (int i = 0; i < 5; i++) {
                triggerRequestOnControlConnection(cluster);
                SECONDS.sleep(1);
            }
            assertThat(logs.getNext()).doesNotContain("sending heartbeat");

            // Ensure heartbeat is sent after no activity.
            SECONDS.sleep(4);
            assertThat(logs.getNext())
                    .contains("sending heartbeat")
                    .contains("heartbeat query succeeded");

            // Ensure heartbeat is sent after continued inactivity.
            SECONDS.sleep(4);
            assertThat(logs.getNext())
                    .contains("sending heartbeat")
                    .contains("heartbeat query succeeded");

            // Ensure heartbeat is not sent after activity.
            logs.getNext();
            for (int i = 0; i < 5; i++) {
                triggerRequestOnControlConnection(cluster);
                SECONDS.sleep(1);
            }
            assertThat(logs.getNext()).doesNotContain("sending heartbeat");

            // Finally, ensure heartbeat is sent after inactivity.
            SECONDS.sleep(4);
            assertThat(logs.getNext())
                    .contains("sending heartbeat")
                    .contains("heartbeat query succeeded");
        } finally {
            cluster.close();
        }
    }

    /**
     * Verifies that there exists a line in logs that matches pattern.
     *
     * @param logs    Captured log entries.
     * @param pattern Pattern to match on individual lines.
     * @return
     */
    private void assertLineMatches(String logs, Pattern pattern) {
        String lines[] = logs.split("\\r?\\n");
        for (String line : lines) {
            if (pattern.matcher(line).matches()) {
                return;
            }
        }
        fail("Expecting: [" + logs + "] to contain " + pattern);
    }

    /**
     * Verifies that no line in logs matches pattern.
     *
     * @param logs    Captured log entries.
     * @param pattern Pattern to match on individual lines.
     */
    private void assertNoLineMatches(String logs, Pattern pattern) {
        String lines[] = logs.split("\\r?\\n");
        for (String line : lines) {
            if (pattern.matcher(line).matches()) {
                fail("Expecting: [" + logs + "] not to contain " + pattern);
            }
        }
    }

    /**
     * Ensures that a heartbeat message is sent after the configured heartbeat interval of idle time when no data is
     * received on a connection even though are successful writes on the socket.
     *
     * @test_category connection:heartbeat
     * @expected_result heartbeat is sent after heartbeat interval (3) seconds of idle time.
     * @jira_ticket JAVA-1346
     * @since 3.0.6, 3.1.3
     */
    @Test(groups = "long")
    public void should_send_heartbeat_when_requests_being_written_but_nothing_received() throws Exception {
        Cluster cluster = Cluster.builder()
                .addContactPoints(hostAddress.getAddress())
                .withPort(scassandra.getBinaryPort())
                .withPoolingOptions(new PoolingOptions().setHeartbeatIntervalSeconds(3).setConnectionsPerHost(HostDistance.LOCAL, 1, 1))
                .build();

        // Prime 'ping' to never return a response this is a way to create outgoing traffic
        // without receiving anything inbound.
        scassandra.primingClient()
                .prime(queryBuilder().withQuery("ping").withThen(then().withFixedDelay(8675309999L)));

        // Thread that will submit queries that get no response repeatedly.
        Thread submitter = null;
        try {
            // Don't create any session, only the control connection will be established
            cluster.init();

            // Find the connection in the connection pool.
            SessionManager session = (SessionManager) cluster.connect();
            Host host = TestUtils.findHost(cluster, 1);
            Connection connection = session.pools.get(host).connections.get(0);

            // Extract connection name from toString implementation.
            String connectionName = connection.toString()
                    .replaceAll("\\-", "\\\\-") // Replace - with \- so its properly escaped as a regex.
                    .replaceAll("Connection\\[\\/", "") // Replace first part of toString (Connection[
                    .replaceAll("\\, inFlight.*", ""); // Replace everything after ',inFlight'

            // Define patterns that check for whether or not heartbeats are sent / received on a given connection.
            Pattern heartbeatSentPattern = Pattern.compile(".*" + connectionName + ".*sending heartbeat");
            Pattern heartbeatReceivedPattern = Pattern.compile(".*" + connectionName + ".*heartbeat query succeeded");
            logger.debug("Heartbeat pattern is {}", heartbeatSentPattern);

            // Start query submission thread.
            submitter = new Thread(new QuerySubmitter(session));
            submitter.start();

            for (int i = 0; i < 5; i++) {
                session.execute("bar");
                SECONDS.sleep(1);
            }

            // Should be no heartbeats sent on pooled connection since we had successful requests.
            String log = logs.getNext();
            assertNoLineMatches(log, heartbeatSentPattern);

            int inFlight = connection.inFlight.get();
            assertThat(inFlight).isGreaterThan(0);

            // Ensure heartbeat is sent after no received data, even though we have inflight requests (JAVA-1346).
            SECONDS.sleep(4);
            // Verify more requests were sent over this time period.
            assertThat(connection.inFlight.get()).isGreaterThan(inFlight);
            log = logs.getNext();
            // Heartbeat should have been sent and received.
            assertLineMatches(log, heartbeatSentPattern);
            assertLineMatches(log, heartbeatReceivedPattern);
        } finally {
            // interrupt thread so it stops submitting queries.
            if (submitter != null) {
                submitter.interrupt();
            }
            cluster.close();
        }
    }

    private static class QuerySubmitter implements Runnable {

        private final Session session;

        QuerySubmitter(Session session) {
            this.session = session;
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                logger.debug("Sending ping, for which we expect no response");
                session.executeAsync("ping");
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Ensures that a heartbeat message is not sent if the configured heartbeat interval is 0.
     * <p/>
     * While difficult to prove the absence of evidence, the test will wait up to the default heartbeat interval
     * (30 seconds + 1) and check to see if the heartbeat was sent.
     *
     * @test_category connection:heartbeat
     * @expected_result heartbeat is not sent after default heartbeat interval (60) seconds of idle time.
     * @jira_ticket JAVA-533
     * @since 2.0.10, 2.1.5
     */
    @Test(groups = "long")
    public void should_not_send_heartbeat_when_disabled() throws InterruptedException {
        Cluster cluster = Cluster.builder()
                .addContactPoints(hostAddress.getAddress())
                .withPort(scassandra.getBinaryPort())
                .withPoolingOptions(new PoolingOptions()
                        .setHeartbeatIntervalSeconds(0))
                .build();

        try {
            // Don't create any session, only the control connection will be established
            cluster.init();

            for (int i = 0; i < 5; i++) {
                triggerRequestOnControlConnection(cluster);
                SECONDS.sleep(1);
            }
            assertThat(logs.get()).doesNotContain("sending heartbeat");

            // Sleep for a while and ensure no heartbeat is sent.
            SECONDS.sleep(32);
            assertThat(logs.get()).doesNotContain("sending heartbeat");
        } finally {
            cluster.close();
        }
    }

    // Simulates activity on the control connection via the internal API
    private void triggerRequestOnControlConnection(Cluster cluster) {
        cluster.manager.controlConnection.refreshNodeInfo(TestUtils.findHost(cluster, 1));
    }
}
