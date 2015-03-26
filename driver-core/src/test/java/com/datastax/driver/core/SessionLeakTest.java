package com.datastax.driver.core;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class SessionLeakTest {

    @Test(groups = "short")
    public void connectionLeakTest() throws Exception {
        CCMBridge ccm = null;
        Cluster cluster = null;

        try {
            ccm = CCMBridge.create("test", 1);

            // create a new cluster object and ensure 0 sessions and connections
            cluster = Cluster.builder().addContactPoints(CCMBridge.IP_PREFIX + '1').build();

            int corePoolSize = cluster.getConfiguration()
                .getPoolingOptions()
                .getCoreConnectionsPerHost(HostDistance.LOCAL);

            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 0);

            // ensure sessions.size() returns with 1 control connection + core pool size.
            Session session = cluster.connect();
            assertEquals(cluster.manager.sessions.size(), 1);
            assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 1 + corePoolSize);

            // ensure sessions.size() returns to 0 with only 1 active connection (the control connection)
            session.close();
            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 1);

            Session thisSession;

            // ensure bootstrapping a node does not create additional connections

            ccm.bootstrapNode(2);
            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 1);

            // ensure a new session gets registered and core connections are established
            // there should be corePoolSize more connections to accommodate for the new host.
            thisSession = cluster.connect();
            assertEquals(cluster.manager.sessions.size(), 1);

            assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 1 + (corePoolSize * 2));

            // ensure bootstrapping a node does not create additional connections that won't get cleaned up
            thisSession.close();

            assertEquals(cluster.manager.sessions.size(), 0);

            assertEquals((int)cluster.getMetrics().getOpenConnections().getValue(), 1);

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
