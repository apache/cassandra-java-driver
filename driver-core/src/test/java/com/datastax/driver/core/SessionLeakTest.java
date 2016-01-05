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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.SocketChannelMonitor;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.fail;

public class SessionLeakTest {

    Cluster cluster;
    List<InetSocketAddress> nodes = Lists.newArrayList(
            new InetSocketAddress(CCMBridge.IP_PREFIX + '1', 9042),
            new InetSocketAddress(CCMBridge.IP_PREFIX + '2', 9042));
    SocketChannelMonitor channelMonitor;

    @Test(groups = "short")
    public void connectionLeakTest() throws Exception {
        // Checking for JAVA-342
        CCMBridge ccmBridge;
        ccmBridge = CCMBridge.builder().withNodes(1).build();
        channelMonitor = new SocketChannelMonitor();
        channelMonitor.reportAtFixedInterval(1, TimeUnit.SECONDS);
        try {
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(Collections.singletonList(
                            new InetSocketAddress(CCMBridge.IP_PREFIX + '1', 9042)))
                    .withNettyOptions(channelMonitor.nettyOptions())
                    .withQueryOptions(nonDebouncingQueryOptions())
                    .build();

            cluster.init();

            assertThat(cluster.manager.sessions.size()).isEqualTo(0);
            // Should be 1 control connection after initialization.
            assertOpenConnections(1);

            // ensure sessions.size() returns with 1 control connection + core pool size.
            int corePoolSize = TestUtils.numberOfLocalCoreConnections(cluster);
            Session session = cluster.connect();

            assertThat(cluster.manager.sessions.size()).isEqualTo(1);
            assertOpenConnections(1 + corePoolSize);

            // ensure sessions.size() returns to 0 with only 1 active connection (the control connection)
            session.close();
            assertThat(cluster.manager.sessions.size()).isEqualTo(0);
            assertOpenConnections(1);

            // ensure bootstrapping a node does not create additional connections
            ccmBridge.bootstrapNode(2);
            assertThat(cluster).host(2).comesUpWithin(2, MINUTES);

            assertThat(cluster.manager.sessions.size()).isEqualTo(0);
            assertOpenConnections(1);

            // ensure a new session gets registered and core connections are established
            // there should be corePoolSize more connections to accommodate for the new host.
            Session thisSession = cluster.connect();
            assertThat(cluster.manager.sessions.size()).isEqualTo(1);
            assertOpenConnections(1 + (corePoolSize * 2));

            // ensure bootstrapping a node does not create additional connections that won't get cleaned up
            thisSession.close();

            assertThat(cluster.manager.sessions.size()).isEqualTo(0);
            assertOpenConnections(1);
        } finally {
            if (cluster != null) {
                cluster.close();
            }
            if (ccmBridge != null) {
                ccmBridge.remove();
            }
            // Ensure no channels remain open.
            channelMonitor.stop();
            channelMonitor.report();
            assertThat(channelMonitor.openChannels(nodes).size()).isEqualTo(0);
        }
    }

    @Test(groups = "short")
    public void should_not_leak_session_when_wrong_keyspace() throws Exception {
        // Checking for JAVA-806
        CCMBridge ccmBridge;
        ccmBridge = CCMBridge.builder().withNodes(1).build();
        channelMonitor = new SocketChannelMonitor();
        channelMonitor.reportAtFixedInterval(1, TimeUnit.SECONDS);
        try {
            cluster = Cluster.builder()
                    .addContactPointsWithPorts(Collections.singletonList(
                            new InetSocketAddress(CCMBridge.IP_PREFIX + '1', 9042)))
                    .withNettyOptions(channelMonitor.nettyOptions()).build();

            cluster.init();

            assertThat(cluster.manager.sessions.size()).isEqualTo(0);
            // Should be 1 control connection after initialization.
            assertOpenConnections(1);

            cluster.connect("wrong_keyspace");

            fail("Should not have connected to a wrong keyspace");

        } catch (InvalidQueryException e) {

            // ok

        } finally {

            assertThat(cluster.manager.sessions.size()).isEqualTo(0);

            if (cluster != null) {
                cluster.close();
            }
            if (ccmBridge != null) {
                ccmBridge.remove();
            }
            // Ensure no channels remain open.
            channelMonitor.stop();
            channelMonitor.report();
            assertThat(channelMonitor.openChannels(nodes).size()).isEqualTo(0);
        }
    }

    private void assertOpenConnections(int expected) {
        assertThat((cluster.getMetrics().getOpenConnections().getValue())).isEqualTo(expected);
        assertThat(channelMonitor.openChannels(nodes).size()).isEqualTo(expected);
    }
}
