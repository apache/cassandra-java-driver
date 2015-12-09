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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static java.util.concurrent.TimeUnit.SECONDS;

public class HeartbeatTest {

    Logger connectionLogger = Logger.getLogger(Connection.class);
    MemoryAppender logs;

    @BeforeMethod(groups = "long")
    public void startCapturingLogs() {
        connectionLogger.setLevel(Level.DEBUG);
        logs = new MemoryAppender();
        connectionLogger.addAppender(logs);
    }

    @AfterMethod(groups = "long")
    public void stopCapturingLogs() {
        connectionLogger.setLevel(null);
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
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.builder("test").withNodes(1).build();
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1))
                    .withPoolingOptions(new PoolingOptions()
                            .setHeartbeatIntervalSeconds(3))
                    .build();

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
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
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
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.builder("test").withNodes(1).build();
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1))
                    .withPoolingOptions(new PoolingOptions()
                            .setHeartbeatIntervalSeconds(0))
                    .build();

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
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    // Simulates activity on the control connection via the internal API
    private void triggerRequestOnControlConnection(Cluster cluster) {
        cluster.manager.controlConnection.refreshNodeInfo(TestUtils.findHost(cluster, 1));
    }
}
