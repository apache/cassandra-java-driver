package com.datastax.driver.core;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * Due to C* gossip bugs, system.peers may report nodes that are gone from the cluster.
 *
 * This class tests scenarios where these nodes have been recommissionned to another cluster and
 * come back up. The driver must detect that they are not part of the cluster anymore, and ignore them.
 */
public class RecommissionnedNodeTest {
    private static final Logger logger = LoggerFactory.getLogger(RecommissionnedNodeTest.class);

    CCMBridge mainCcm, otherCcm;
    Cluster mainCluster;

    /**
     * Tests the case when trying to reconnect to the foreign node results from a scheduled reconnection attempt.
     */
    @Test(groups = "long")
    public void testReconnectionToRegularNode() throws Exception {
        mainCcm = CCMBridge.create("main", 3);
        // node1 will be our "recommissionned" node, for now we just stop it so that it stays in the peers table.
        mainCcm.stop(1);
        mainCcm.waitForDown(1);

        // Now start the driver that will connect to node2 and node3, and consider node1 down
        Cluster mainCluster = Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + "2").build();
        mainCluster.connect();
        waitForCountUpHosts(mainCluster, 2);
        // From that point, reconnections to node1 have been scheduled.

        // Start another ccm that will reuse node1's address
        otherCcm = CCMBridge.create("other", 1);
        otherCcm.waitForUp(1);

        // Give the driver the time to notice the node is back up and try to connect to it.
        TimeUnit.SECONDS.sleep(32);

        assertEquals(countUpHosts(mainCluster), 2);
    }

    /**
     * Tests the case when trying to reconnect to the foreign node results from a scheduled reconnection attempt.
     */
    @Test(groups = "long")
    public void testControlConnection() throws Exception {
        mainCcm = CCMBridge.create("main", 2);
        mainCcm.stop(1);
        mainCcm.waitForDown(1);

        // Start the driver, the control connection will be on node2
        Cluster mainCluster = Cluster.builder().addContactPoint(CCMBridge.IP_PREFIX + "2").build();
        mainCluster.connect();
        waitForCountUpHosts(mainCluster, 1);

        // Start another ccm that will reuse node1's address
        otherCcm = CCMBridge.create("other", 1);
        otherCcm.waitForUp(1);

        // Stop node2, the control connection gets defunct
        mainCcm.stop(2);
        TimeUnit.SECONDS.sleep(32);

        // The driver should not try to reconnect the control connection to node1
        assertFalse(mainCluster.manager.controlConnection.isOpen());
    }

    @AfterMethod(groups = "long")
    public void teardown() {
        if (mainCluster != null)
            mainCluster.close();

        if (mainCcm != null)
            mainCcm.remove();
        if (otherCcm != null)
            otherCcm.remove();
    }

    private static int countUpHosts(Cluster cluster) {
        int ups = 0;
        for (Host host : cluster.getMetadata().getAllHosts()) {
            if (host.isUp())
                ups += 1;
        }
        return ups;
    }

    private static void waitForCountUpHosts(Cluster cluster, int expectedCount) throws InterruptedException {
        int maxRetries = 30;
        int interval = 10;

        for (int i = 0; i <= maxRetries; i++) {
            int actualCount = countUpHosts(cluster);
            if (actualCount == expectedCount)
                return;

            if (i == maxRetries)
                fail(String.format("Up host count didn't reach %d after %d seconds",
                                   expectedCount, i * interval));
            else
                logger.debug("Counted {} up hosts after {} seconds", actualCount, i * interval);

            TimeUnit.SECONDS.sleep(interval);
        }
    }
}