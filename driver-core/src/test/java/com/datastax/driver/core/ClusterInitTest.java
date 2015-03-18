package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.utils.UUIDs;

import static com.datastax.driver.core.FakeHost.Behavior.THROWING_CONNECT_TIMEOUTS;

import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

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
            int coreConnections = cluster.getConfiguration()
                    .getPoolingOptions()
                    .getCoreConnectionsPerHost(HostDistance.LOCAL);
            verify(socketOptions, times(1 + coreConnections + 5)).getKeepAlive();
        } finally {
            if (cluster != null)
                cluster.close();
            for (FakeHost fakeHost : fakeHosts)
                fakeHost.stop();
            if (ccm != null)
                ccm.remove();
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
