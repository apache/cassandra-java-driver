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

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.datastax.driver.core.FakeHost.Behavior.THROWING_CONNECT_TIMEOUTS;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.*;

public class ClusterInitTest {
    private static final Logger logger = LoggerFactory.getLogger(ClusterInitTest.class);

    /**
     * Test for JAVA-522: when the cluster and session initialize, if some contact points are behaving badly and
     * causing timeouts, we want to ensure that the driver does not wait multiple times on the same host.
     */

    @Test(groups = "short")
    public void should_wait_for_each_contact_point_at_most_once() {
        CCMBridge ccm = null;
        Cluster cluster = null;
        List<FakeHost> fakeHosts = Lists.newArrayList();
        try {
            // Obtaining connect timeouts is not trivial: we create a 6-host cluster but only start one of them,
            // then simulate the other 5.
            ccm = CCMBridge.create("test");
            ccm.populate(6);
            ccm.start(1);

            for (int i = 0; i < 5; i++) {
                FakeHost fakeHost = new FakeHost(CCMBridge.ipOfNode(i + 2), 9042, THROWING_CONNECT_TIMEOUTS);
                fakeHosts.add(fakeHost);
                fakeHost.start();
            }

            // Our real instance has no rows in its system.peers table. That would cause the driver to ignore our fake
            // hosts when the control connection refreshes the host list.
            // So we also insert fake rows in system.peers:
            fakePeerRowsInNode1();

            logger.info("Environment is set up, starting test");
            long start = System.nanoTime();

            // We want to count how many connections were attempted. For that, we rely on the fact that SocketOptions.getKeepAlive is called in Connection.Factory.newBoostrap()
            // each time we prepare to open a new connection. This is a bit of a hack, but this is what we have.
            SocketOptions socketOptions = spy(new SocketOptions());

            // Set an enormous delay so that reconnection attempts don't pollute our observations
            ConstantReconnectionPolicy reconnectionPolicy = new ConstantReconnectionPolicy(3600 * 1000);

            cluster = Cluster.builder().addContactPoints(
                CCMBridge.ipOfNode(1), CCMBridge.ipOfNode(2), CCMBridge.ipOfNode(3),
                CCMBridge.ipOfNode(4), CCMBridge.ipOfNode(5), CCMBridge.ipOfNode(6))
                .withSocketOptions(socketOptions)
                .withReconnectionPolicy(reconnectionPolicy)
                .withProtocolVersion(TestUtils.getDesiredProtocolVersion())
                .build();
            cluster.connect();

            // For information only:
            long initTimeMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);
            logger.info("Cluster and session initialized in {} ms", initTimeMs);

            // We have one live host so 2 successful connections (1 control connection and 1 core connection in the pool).
            // The other 5 hosts are unreachable, we should attempt to connect to each of them only once.
            verify(socketOptions, times(1 + TestUtils.numberOfLocalCoreConnections(cluster) + 5)).getKeepAlive();
        } finally {
            if (cluster != null)
                cluster.close();
            for (FakeHost fakeHost : fakeHosts)
                fakeHost.stop();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * <p>
     * Validates that a Cluster that was never able to successfully establish connection a session can be closed
     * properly.
     *
     * @test_category connection
     * @expected_result Cluster closes within 1 second.
     */
    @Test(groups="unit")
    public void should_be_able_to_close_cluster_that_never_successfully_connected() throws Exception {
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(Collections.singleton(new InetSocketAddress("127.0.0.1", 65534)))
                .build();
        try {
            cluster.connect();
            fail("Should not have been able to connect.");
        } catch(NoHostAvailableException e) {} // Expected.
        CloseFuture closeFuture = cluster.closeAsync();
        try {
            closeFuture.get(1, TimeUnit.SECONDS);
        } catch(TimeoutException e) {
            fail("Close Future did not complete quickly.");
        }
    }

    private void fakePeerRowsInNode1() {
        Cluster cluster = null;
        try {
            cluster = Cluster.builder().addContactPoint(CCMBridge.ipOfNode(1)).build();
            Session session = cluster.connect("system");

            String releaseVersion = session.execute("SELECT release_version FROM local")
                .one().getString("release_version");

            for (int i = 2; i <= 6; i++) {
                Insert insertStmt = insertInto("peers")
                    .value("peer", InetAddress.getByName(CCMBridge.ipOfNode(i)))
                    .value("data_center", "datacenter1")
                    .value("host_id", UUIDs.random())
                    .value("rack", "rack1")
                    .value("release_version", releaseVersion)
                    .value("rpc_address", InetAddress.getByName(CCMBridge.ipOfNode(i)))
                    .value("schema_version", UUIDs.random());
                session.execute(insertStmt);
            }
        } catch (Exception e) {
            fail("Error while inserting fake peer rows", e);
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }
}
