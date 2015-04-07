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

import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.assertEquals;


public class SessionLeakTest {


    @Test(groups = "short")
    public void connectionLeakTest() throws Exception {
        // Checking for JAVA-342
        CCMBridge ccmBridge;
        ccmBridge = CCMBridge.create("test", 1);
        Cluster cluster = null;
        try {
            // create a new cluster object and ensure 0 sessions and connections
            cluster = Cluster.builder().addContactPoints(CCMBridge.IP_PREFIX + '1').build();
            cluster.init();

            int corePoolSize = cluster.getConfiguration()
                    .getPoolingOptions()
                    .getCoreConnectionsPerHost(HostDistance.LOCAL);

            assertEquals(cluster.manager.sessions.size(), 0);
            // Should be 1 control connection after initialization.
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);

            // ensure sessions.size() returns with 1 control connection + core pool size.
            Session session = cluster.connect();
            assertEquals(cluster.manager.sessions.size(), 1);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1 + corePoolSize);

            // ensure sessions.size() returns to 0 with only 1 active connection (the control connection)
            session.close();
            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);

            // ensure bootstrapping a node does not create additional connections
            ccmBridge.bootstrapNode(2);
            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);

            // ensure a new session gets registered and core connections are established
            // there should be corePoolSize more connections to accommodate for the new host.
            Session thisSession = cluster.connect();
            assertEquals(cluster.manager.sessions.size(), 1);

            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1 + (corePoolSize * 2));

            // ensure bootstrapping a node does not create additional connections that won't get cleaned up
            thisSession.close();

            assertEquals(cluster.manager.sessions.size(), 0);
            assertEquals((int) cluster.getMetrics().getOpenConnections().getValue(), 1);
        } finally {
            if(cluster != null){
                cluster.close();
            }
            if (ccmBridge != null) {
                ccmBridge.remove();
            }
        }
    }
}
