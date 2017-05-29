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

import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.utils.SocketChannelMonitor;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.TestUtils.nonDebouncingQueryOptions;
import static com.google.common.collect.Lists.newArrayList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.fail;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true, createCluster = false)
public class SessionLeakTest extends CCMTestsSupport {

    SocketChannelMonitor channelMonitor;

    @Test(groups = "long")
    public void connectionLeakTest() throws Exception {
        // Checking for JAVA-342
        channelMonitor = new SocketChannelMonitor();
        channelMonitor.reportAtFixedInterval(1, TimeUnit.SECONDS);
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withNettyOptions(channelMonitor.nettyOptions())
                .withQueryOptions(nonDebouncingQueryOptions())
                .build());

        cluster.init();

        assertThat(cluster.manager.sessions.size()).isEqualTo(0);
        // Should be 1 control connection after initialization.
        assertOpenConnections(1, cluster);

        // ensure sessions.size() returns with 1 control connection + core pool size.
        int corePoolSize = TestUtils.numberOfLocalCoreConnections(cluster);
        Session session = cluster.connect();

        assertThat(cluster.manager.sessions.size()).isEqualTo(1);
        assertOpenConnections(1 + corePoolSize, cluster);

        // ensure sessions.size() returns to 0 with only 1 active connection (the control connection)
        session.close();
        assertThat(cluster.manager.sessions.size()).isEqualTo(0);
        assertOpenConnections(1, cluster);

        // ensure bootstrapping a node does not create additional connections
        ccm().add(2);
        ccm().start(2);
        ccm().waitForUp(2);
        assertThat(cluster).host(2).comesUpWithin(2, MINUTES);

        assertThat(cluster.manager.sessions.size()).isEqualTo(0);
        assertOpenConnections(1, cluster);

        // ensure a new session gets registered and core connections are established
        // there should be corePoolSize more connections to accommodate for the new host.
        Session thisSession = cluster.connect();
        assertThat(cluster.manager.sessions.size()).isEqualTo(1);
        assertOpenConnections(1 + (corePoolSize * 2), cluster);

        // ensure bootstrapping a node does not create additional connections that won't get cleaned up
        thisSession.close();

        assertThat(cluster.manager.sessions.size()).isEqualTo(0);
        assertOpenConnections(1, cluster);
        cluster.close();
        // Ensure no channels remain open.
        channelMonitor.stop();
        channelMonitor.report();
        assertThat(channelMonitor.openChannels(newArrayList(ccm().addressOfNode(1), ccm().addressOfNode(2))).size()).isEqualTo(0);
    }

    @Test(groups = "short")
    public void should_not_leak_session_when_wrong_keyspace() throws Exception {
        // Checking for JAVA-806
        channelMonitor = new SocketChannelMonitor();
        channelMonitor.reportAtFixedInterval(1, TimeUnit.SECONDS);
        Cluster cluster = register(Cluster.builder()
                .addContactPoints(getContactPoints().get(0))
                .withPort(ccm().getBinaryPort())
                .withNettyOptions(channelMonitor.nettyOptions())
                .build());
        cluster.init();
        assertThat(cluster.manager.sessions.size()).isEqualTo(0);
        try {
            // Should be 1 control connection after initialization.
            assertOpenConnections(1, cluster);
            cluster.connect("wrong_keyspace");
            fail("Should not have connected to a wrong keyspace");
        } catch (InvalidQueryException e) {
            // ok
        }
        assertThat(cluster.manager.sessions.size()).isEqualTo(0);
        cluster.close();
        // Ensure no channels remain open.
        channelMonitor.stop();
        channelMonitor.report();
        assertThat(channelMonitor.openChannels(ccm().addressOfNode(1), ccm().addressOfNode(2)).size()).isEqualTo(0);
    }

    private void assertOpenConnections(int expected, Cluster cluster) {
        assertThat(cluster.getMetrics().getOpenConnections().getValue()).isEqualTo(expected);
        assertThat(channelMonitor.openChannels(ccm().addressOfNode(1), ccm().addressOfNode(2)).size()).isEqualTo(expected);
    }
}
