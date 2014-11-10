package com.datastax.driver.core;

import static java.util.concurrent.TimeUnit.SECONDS;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;

public class HeartbeatTest {

    Logger connectionLogger = Logger.getLogger(Connection.class);
    MemoryAppender logs;

    @BeforeMethod(groups = "long")
    public void startCapturingLogs() {
        connectionLogger.setLevel(Level.DEBUG);
        logs = new MemoryAppender();
        connectionLogger.addAppender(logs);
    }

    @AfterMethod(groups="long")
    public void stopCapturingLogs() {
        connectionLogger.setLevel(null);
        connectionLogger.removeAppender(logs);
    }

    @Test(groups = "long")
    public void should_send_heartbeat_when_connection_is_inactive() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.create("test", 1);
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1))
                .withPoolingOptions(new PoolingOptions()
                    .setHeartbeatIntervalSeconds(3))
                .build();

            // Don't create any session, only the control connection will be established
            cluster.init();

            for (int i = 0; i < 5; i++) {
                // Simulate activity
                cluster.manager.controlConnection.refreshNodeInfo(TestUtils.findHost(cluster, 1));
                SECONDS.sleep(1);
            }
            assertThat(logs.get()).doesNotContain("sending heartbeat");

            SECONDS.sleep(5);
            assertThat(logs.get())
                .contains("sending heartbeat")
                .contains("heartbeat query succeeded");
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
