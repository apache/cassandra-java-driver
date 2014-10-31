package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.utils.UUIDs;

import static com.datastax.driver.core.FakeHost.Behavior.THROWING_CONNECT_TIMEOUTS;

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

            cluster = Cluster.builder().addContactPoints(
                CCMBridge.ipOfNode(1), CCMBridge.ipOfNode(2), CCMBridge.ipOfNode(3),
                CCMBridge.ipOfNode(4), CCMBridge.ipOfNode(5), CCMBridge.ipOfNode(6)
            ).build();
            cluster.connect();

            long initTimeMs = TimeUnit.MILLISECONDS.convert(System.nanoTime() - start, TimeUnit.NANOSECONDS);

            // It's hard to estimate how much time init should take. But at the very least, we should wait at most
            // once for each of the 5 failing hosts, so 6 times the connect timeout is a reasonable upper bound.
            int expected = cluster.getConfiguration().getSocketOptions().getConnectTimeoutMillis() * 6;

            logger.info("Cluster and session initialized in {} ms", initTimeMs);
            assertThat(initTimeMs).isLessThan(expected);
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

            for (int i = 2; i <= 6; i++)
                session.execute("INSERT INTO peers (peer, data_center, host_id, rack, release_version, rpc_address, schema_version) " +
                        "VALUES (?, 'datacenter1', ?, 'rack1', ?, ?, ?)",
                    InetAddress.getByName(CCMBridge.ipOfNode(i)),
                    UUIDs.random(),
                    releaseVersion,
                    InetAddress.getByName(CCMBridge.ipOfNode(i)),
                    UUIDs.random()
                );
        } catch (Exception e) {
            fail("Error while inserting fake peer rows", e);
        } finally {
            if (cluster != null)
                cluster.close();
        }
    }
}
