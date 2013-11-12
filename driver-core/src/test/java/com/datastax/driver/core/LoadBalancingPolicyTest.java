/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.net.InetAddress;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import static com.datastax.driver.core.TestUtils.*;

public class LoadBalancingPolicyTest extends AbstractPoliciesTest {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancingPolicyTest.class);
    private static final boolean DEBUG = false;

    private PreparedStatement prepared;

    @Test(groups = "long")
    public void roundRobinTest() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy());
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        try {

            createSchema(c.session);
            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 6);
            assertQueried(CCMBridge.IP_PREFIX + "2", 6);

            resetCoordinators();
            c.cassandraCluster.bootstrapNode(3);
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 4);
            assertQueried(CCMBridge.IP_PREFIX + "2", 4);
            assertQueried(CCMBridge.IP_PREFIX + "3", 4);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(1);
            waitForDecommission(CCMBridge.IP_PREFIX + "1", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "2", 6);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void roundRobinWith2DCsTest() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy());
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, 2, builder);
        try {

            createSchema(c.session);
            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 3);
            assertQueried(CCMBridge.IP_PREFIX + "2", 3);
            assertQueried(CCMBridge.IP_PREFIX + "3", 3);
            assertQueried(CCMBridge.IP_PREFIX + "4", 3);

            resetCoordinators();
            c.cassandraCluster.bootstrapNode(5, "dc2");
            c.cassandraCluster.decommissionNode(1);
            waitFor(CCMBridge.IP_PREFIX + "5", c.cluster);
            waitForDecommission(CCMBridge.IP_PREFIX + "1", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 3);
            assertQueried(CCMBridge.IP_PREFIX + "3", 3);
            assertQueried(CCMBridge.IP_PREFIX + "4", 3);
            assertQueried(CCMBridge.IP_PREFIX + "5", 3);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void DCAwareRoundRobinTest() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, 2, builder);
        try {

            createMultiDCSchema(c.session);
            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);
            assertQueried(CCMBridge.IP_PREFIX + "4", 6);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void dcAwareRoundRobinTestWithOneRemoteHost() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2", 1));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, 2, builder);
        try {

            createMultiDCSchema(c.session);
            init(c, 12);
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);
            assertQueried(CCMBridge.IP_PREFIX + "4", 6);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.bootstrapNode(5, "dc3");
            waitFor(CCMBridge.IP_PREFIX + "5", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);
            assertQueried(CCMBridge.IP_PREFIX + "4", 6);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(3);
            c.cassandraCluster.decommissionNode(4);
            waitForDecommission(CCMBridge.IP_PREFIX + "3", c.cluster);
            waitForDecommission(CCMBridge.IP_PREFIX + "4", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 12);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(5);
            waitForDecommission(CCMBridge.IP_PREFIX + "5", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 12);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(1);
            waitForDecommission(CCMBridge.IP_PREFIX + "1", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);

            try {
                query(c, 12);
                fail();
            } catch (NoHostAvailableException e) {
                // No more nodes so ...
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void tokenAwareTest() throws Throwable {
        tokenAwareTest(false);
    }

    @Test(groups = "long")
    public void tokenAwarePreparedTest() throws Throwable {
        tokenAwareTest(true);
    }

    /**
     * Check for JAVA-123 bug. Doesn't really test token awareness, but rather
     * that we do not destroy the keys.
     */
    @Test(groups = "long")
    public void tokenAwareCompositeKeyTest() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);

        Session session = c.session;

        try {
            String COMPOSITE_TABLE = "composite";
            session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, SIMPLE_KEYSPACE, 2));
            session.execute("USE " + SIMPLE_KEYSPACE);
            session.execute(String.format("CREATE TABLE %s (k1 int, k2 int, i int, PRIMARY KEY ((k1, k2)))", COMPOSITE_TABLE));

            PreparedStatement ps = session.prepare("INSERT INTO " + COMPOSITE_TABLE + "(k1, k2, i) VALUES (?, ?, ?)");
            session.execute(ps.bind(1, 2, 3));

            ResultSet rs = session.execute("SELECT * FROM " + COMPOSITE_TABLE + " WHERE k1 = 1 AND k2 = 2");
            assertTrue(!rs.isExhausted());
            Row r = rs.one();
            assertTrue(rs.isExhausted());

            assertEquals(r.getInt("i"), 3);
        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            c.discard();
        }
    }

    public void tokenAwareTest(boolean usePrepared) throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        try {

            createSchema(c.session);
            init(c, 12);
            query(c, 12);

            // Not the best test ever, we should use OPP and check we do it the
            // right nodes. But since M3P is hard-coded for now, let just check
            // we just hit only one node.
            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);

            resetCoordinators();
            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);

            resetCoordinators();
            c.cassandraCluster.forceStop(2);
            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);

            try {
                query(c, 12, usePrepared);
                fail();
            } catch (UnavailableException e) {
                assertEquals("Not enough replica available for query at consistency ONE (1 required but only 0 alive)",
                             e.getMessage());
            }

            resetCoordinators();
            c.cassandraCluster.start(2);
            waitFor(CCMBridge.IP_PREFIX + "2", c.cluster);

            // FIXME: remove sleep once waitFor() is fixed
            Thread.sleep(2000);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(2);
            waitForDecommission(CCMBridge.IP_PREFIX + "2", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 12);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void tokenAwareWithRF2Test() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        try {

            createSchema(c.session, 2);
            init(c, 12);
            query(c, 12);

            // Not the best test ever, we should use OPP and check we do hit the
            // right nodes. But since M3P is hard-coded for now, let just check
            // we just hit only one node.
            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

            resetCoordinators();
            c.cassandraCluster.bootstrapNode(3);
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster);

            query(c, 12);

            // We should still be hitting only one node
            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

            resetCoordinators();
            c.cassandraCluster.stop(2);
            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);

            query(c, 12);

            // Still only one node since RF=2
            assertQueried(CCMBridge.IP_PREFIX + "1", 12);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    @Test(groups = "long")
    public void latencyAwareTest() throws Throwable {
        //Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy());
        LatencyAwarePolicy latencyAwarePolicyInstance = new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build();
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(latencyAwarePolicyInstance);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        try {

            createSchema(c.session, 3);
            int longTest = 500000;
            int shortTest = 100000;
            init(c, longTest);

            query(c, longTest);
            showLatencyStats(latencyAwarePolicyInstance);

            resetCoordinators();
            c.cassandraCluster.bootstrapNode(3);
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster);

            for (int i=0; i < 10; ++i) {

                query(c, longTest);
                showLatencyStats(latencyAwarePolicyInstance);

            }

            resetCoordinators();
            c.cassandraCluster.stop(2);
            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);

            for (int i=0; i < 10; ++i) {

                query(c, shortTest);
                showLatencyStats(latencyAwarePolicyInstance);

            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    private void showLatencyStats(LatencyAwarePolicy latencyAwarePolicyInstance) {
        Map<Host, LatencyAwarePolicy.TimestampedAverage> currentLatencies = latencyAwarePolicyInstance.latencyTracker.currentLatencies();

        // create a sorted list for easy printing
        ArrayList<Host> hosts = new ArrayList<Host>();
        for (int i=0; i < 3; ++i) {
            try {
                hosts.add(new Host(InetAddress.getByName(CCMBridge.IP_PREFIX + (i + 1)), new ConvictionPolicy.Simple.Factory()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        long minLatency = Long.MAX_VALUE;
        int totalQueried = 0;
        for (Host host : hosts) {
            LatencyAwarePolicy.TimestampedAverage latency = currentLatencies.get(host);
            if (latency != null) {
                if (latency.average < minLatency)
                    minLatency = latency.average;
                totalQueried += getQueried(host.toString().substring(1));
            }
        }

        // print headers
        logger.info(String.format("%20s %20s %20s %20s %20s %20s",
                "host", "latency.average", "latency.nbMeasure", "queried.count",
                "latency", "queried %"));

        // print found metrics
        for (Host host : hosts) {
            LatencyAwarePolicy.TimestampedAverage latency = currentLatencies.get(host);
            int queriedCount = getQueried(host.toString().substring(1));
            if (latency != null)
                logger.info(String.format("%20s %20s %20s %20s %19sx %20s",
                        host, latency.average, latency.nbMeasure,
                        queriedCount,
                        (float) latency.average / minLatency,
                        Math.round((float) queriedCount / totalQueried * 100)));
        }

        // generate a space during printing for easy readability
        logger.info("");
    }
}
