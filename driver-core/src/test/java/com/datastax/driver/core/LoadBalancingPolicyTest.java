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

    /**
     * Ensure that latencyAwarePolicy does not get lop-sided when adding or removing nodes.
     *
     * @throws Throwable
     */
    @Test(groups = "long")
    public void latencyAwareTest() throws Throwable {
        int failures = 0;

        // prints stats throughout the test to help debug issues
        boolean debug = false;

        LatencyAwarePolicy latencyAwarePolicyInstance = new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build();
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(latencyAwarePolicyInstance);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);

        // create a sorted list for easy printing
        ArrayList<Host> hosts = new ArrayList<Host>();
        for (int i=0; i < 3; ++i) {
            try {
                hosts.add(new Host(InetAddress.getByName(CCMBridge.IP_PREFIX + (i + 1)), new ConvictionPolicy.Simple.Factory()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {

            createSchema(c.session, 3);
            int testSize = 5000;
            init(c, testSize);

            // run a benchmark test
            query(c, testSize);
            failures += assertRanges(latencyAwarePolicyInstance, hosts, debug);

            resetCoordinators();
            c.cassandraCluster.bootstrapNode(3);
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster);

            // ensure adding a node does not lop-side results
            for (int i=0; i < 10; ++i) {
                query(c, testSize);
                failures += assertRanges(latencyAwarePolicyInstance, hosts, debug);
            }

            resetCoordinators();
            c.cassandraCluster.stop(2);
            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);

            // ensure removing a node does not lop-side results
            for (int i=0; i < 10; ++i) {
                query(c, testSize);
                failures += assertRanges(latencyAwarePolicyInstance, hosts, debug);
            }

            // make the final jUnit test fail if more than 2 failures were seen
            if (failures > 2) {
                fail(String.format("%s failures seen. Unless this happens frequently, you should assume this is statistical noise.", failures));
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    /**
     * Ensure nodes will be within acceptable ranges when using the LAP.
     *
     * @param latencyAwarePolicyInstance
     * @param hosts
     */
    private int assertRanges(LatencyAwarePolicy latencyAwarePolicyInstance, ArrayList<Host> hosts, boolean debug) {
        int failures = 0;

        float totalLatency = 0;
        float totalCount = 0;
        float totalNodes = 0;

        // grab totals and average
        for (Host host : hosts) {
            LatencyAwarePolicy.Snapshot.Stats latency = latencyAwarePolicyInstance.getScoresSnapshot().getStats(host);
            if (latency != null) {
                totalLatency += latency.getLatencyScore();
                totalCount += latency.getMeasurementsCount();
                ++totalNodes;
            }
        }
        float averageLatency = totalLatency / totalNodes;

        // check each host...
        for (Host host : hosts) {
            LatencyAwarePolicy.Snapshot.Stats latency = latencyAwarePolicyInstance.getScoresSnapshot().getStats(host);

            // check if host was active for this round of stats...
            if (latency != null) {
                // ensure a single node is never 3x more latent than the average latency
                double limit = 3;
                if (latency.getLatencyScore() > averageLatency * limit) {
                    if (debug) logger.warn(latency.getLatencyScore() / averageLatency + ">" + limit);
                    if (debug) logger.warn(String.format("Node %s has a latency of %s, which is %sx, higher than %sx, the average latency of %s",
                        host, latency.getLatencyScore(), latency.getLatencyScore() / averageLatency, limit, averageLatency));
                    ++failures;
                }

                // ensure a single node is never less than 0.04x the average latency
                limit = 0.04;
                if (latency.getLatencyScore() < averageLatency * limit) {
                    if (debug) logger.warn(latency.getLatencyScore() / averageLatency + "<" + limit);
                    if (debug) logger.warn("Node %s has a latency of %s, which is %sx, less than %sx, the average latency of %s",
                        host, latency.getLatencyScore(), latency.getLatencyScore() / averageLatency, limit, averageLatency);
                    ++failures;
                }

                // ensure a single node never carries more than 90% of the queries
                limit = 0.9;
                if (latency.getMeasurementsCount() / totalCount > limit) {
                    if (debug) logger.warn(latency.getMeasurementsCount() / totalCount + ">" + limit);
                    if (debug) logger.warn("Node %s was pinged %s times, or %s%%..., which is greater than %s%%",
                        host, latency.getMeasurementsCount(), latency.getMeasurementsCount() / totalCount,
                        limit * 100);
                    ++failures;
                }

                // ensure a single node never handles less than 5% of the queries (it's set so low to make room for rampup time)
                limit = 0.04;
                if (latency.getMeasurementsCount() / totalCount < limit) {
                    if (debug) logger.warn(latency.getMeasurementsCount() / totalCount + "<" + limit);
                    if (debug) logger.warn("Node %s was pinged %s times, or %s%%..., which is less than %s%%",
                        host, latency.getMeasurementsCount(), latency.getMeasurementsCount() / totalCount,
                        limit * 100);
                    ++failures;

                }
            }
        }

        return failures;
    }



    @Test(groups = "long")
    public void latencyAwareForcedTest() throws Throwable {
        int failures = 0;

        // prints stats throughout the test to help debug issues
        boolean debug = false;

        LatencyAwarePolicy latencyAwarePolicyInstance = new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build();
        LatencyAwarePolicy.Tracker latencyTracker = latencyAwarePolicyInstance.latencyTracker;
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(latencyAwarePolicyInstance);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);

        // create a sorted list for easy cycling of hosts
        ArrayList<Host> hosts = new ArrayList<Host>();
        for (int i=0; i < 3; ++i) {
            try {
                hosts.add(new Host(InetAddress.getByName(CCMBridge.IP_PREFIX + (i + 1)), new ConvictionPolicy.Simple.Factory()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {
            createSchema(c.session, 3);
            int testSize = 5000;
            init(c, testSize);



            // ensure the policy has enough datapoints to use the tracked stats
            query(c, testSize * 10);
            if (debug) showLatencyStats(latencyAwarePolicyInstance, hosts);
            resetCoordinators();
            if (debug) logger.info("==================================================");



            // make node 0 incredibly latent
            for (int i = 0; i < 50000; ++i) {
                latencyTracker.update(hosts.get(0), 99999999);
            }

            // query 5 times
            for (int i = 0; i < 5; ++i) {
                query(c, testSize);
                if (debug) showLatencyStats(latencyAwarePolicyInstance, hosts);
            }

            // ensure node 0, the highly latent node, does not handle more than 200 queries
            if (getQueried(hosts.get(0).toString().substring(1)) > 200) {
                logger.warn(String.format("%s queries sent to %s, which was greater than the expected %s",
                        getQueried(hosts.get(0).toString().substring(1)), hosts.get(0), 200));
                ++failures;
            }
            resetCoordinators();
            if (debug) logger.info("==================================================");



            // bring node 0's latency down, but increase node 1's latency
            for (int i = 0; i < 50000 * 2; ++i) {
                latencyTracker.update(hosts.get(0), 0);
                latencyTracker.update(hosts.get(1), 999999999);
            }

            // query 5 times
            for (int i = 0; i < 5; ++i) {
                query(c, testSize);
                if (debug) showLatencyStats(latencyAwarePolicyInstance, hosts);
            }

            // ensure node 1, the newly latent node, does not handle more than 5000 queries
            // do note that sometimes the LAP will recalculate the totals causing the averages to get updated with the
            // real data via a weighted average
            if (getQueried(hosts.get(1).toString().substring(1)) > 5000) {
                logger.warn(String.format("%s queries sent to %s, which was greater than the expected %s",
                        getQueried(hosts.get(1).toString().substring(1)), hosts.get(1), 5000));
                ++failures;
            }
            resetCoordinators();
            if (debug) logger.info("==================================================");



            // query 5 times
            for (int i = 0; i < 5; ++i) {
                query(c, testSize);
                if (debug) showLatencyStats(latencyAwarePolicyInstance, hosts);
            }

            // ensure each node returns to taking up at least 25% of the queries since the stats should have reset to
            // include the new averages by this point
            if (getQueried(hosts.get(0).toString().substring(1)) < 5000) {
                logger.warn(String.format("%s queries sent to %s, which was less than the expected %s",
                        getQueried(hosts.get(0).toString().substring(1)), hosts.get(0), 5000));
                ++failures;
            }
            if (getQueried(hosts.get(1).toString().substring(1)) < 5000) {
                logger.warn(String.format("%s queries sent to %s, which was less than the expected %s",
                        getQueried(hosts.get(1).toString().substring(1)), hosts.get(1), 5000));
                ++failures;
            }
            resetCoordinators();
            if (debug) logger.info("==================================================");



            // start a 3rd node and repeat the same concepts
            resetCoordinators();
            c.cassandraCluster.bootstrapNode(3);
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster);

            // ensure 3rd node has enough stats data
            query(c, testSize * 10);
            if (debug) showLatencyStats(latencyAwarePolicyInstance, hosts);
            resetCoordinators();


            // run this test for longer to ensure ramp-up time doesn't become a factor
            for (int i = 0; i < 20; ++i) {
                query(c, testSize);
                if (debug) showLatencyStats(latencyAwarePolicyInstance, hosts);
            }

            // ensure that each node handles a fair amount of requests, even if it just joined the ring
            for (int i = 0; i < 3; ++i) {
                if (getQueried(hosts.get(i).toString().substring(1)) < 15000) {
                    logger.warn(String.format("%s queries sent to %s, which was less than the expected %s",
                            getQueried(hosts.get(i).toString().substring(1)), hosts.get(i), 15000));
                    ++failures;
                }
            }
            if (debug) logger.info("==================================================");



            resetCoordinators();
            c.cassandraCluster.stop(2);
            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);



            // query 5 times
            for (int i = 0; i < 6; ++i) {
                query(c, testSize);
                if (debug) showLatencyStats(latencyAwarePolicyInstance, hosts);
            }



            // ensure that each node gets a noticable chunk of requests, even after 1 node left the ring
            if (getQueried(hosts.get(0).toString().substring(1)) < 10000) {
                logger.warn(String.format("%s queries sent to %s, which was less than the expected %s",
                        getQueried(hosts.get(0).toString().substring(1)), hosts.get(0), 10000));
                ++failures;
            }
            if (getQueried(hosts.get(2).toString().substring(1)) < 10000) {
                logger.warn(String.format("%s queries sent to %s, which was less than the expected %s",
                        getQueried(hosts.get(2).toString().substring(1)), hosts.get(2), 10000));
                ++failures;
            }




            // make the final jUnit test fail if more than 2 failures were seen
            if (failures > 2) {
                fail(String.format("%s failures seen. Unless this happens frequently, you should assume this is statistical noise.", failures));
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    /**
     * Debugger test that outputs the stats held within the LatencyAwarePolicy.
     * Not set to run with jUnit, but included for future manual debugging use.
     * @throws Throwable
     */
    public void latencyAwarePrintTest() throws Throwable {
        LatencyAwarePolicy latencyAwarePolicyInstance = new LatencyAwarePolicy.Builder(new RoundRobinPolicy()).build();
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(latencyAwarePolicyInstance);
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);

        // create a sorted list for easy printing
        ArrayList<Host> hosts = new ArrayList<Host>();
        for (int i=0; i < 3; ++i) {
            try {
                hosts.add(new Host(InetAddress.getByName(CCMBridge.IP_PREFIX + (i + 1)), new ConvictionPolicy.Simple.Factory()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try {

            createSchema(c.session, 3);
            int longTest = 500000;
            int shortTest = 100000;
            init(c, longTest);

            // run a long test to fill up the stats
            query(c, longTest);
            showLatencyStats(latencyAwarePolicyInstance, hosts);

            // add a new node
            resetCoordinators();
            c.cassandraCluster.bootstrapNode(3);
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster);

            // run the long test 10 times and manually ensure the LAP directs
            // traffic to nodes accordingly
            for (int i=0; i < 10; ++i) {
                query(c, longTest);
                showLatencyStats(latencyAwarePolicyInstance, hosts);
            }

            // stop node 2
            resetCoordinators();
            c.cassandraCluster.stop(2);
            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);

            // manually ensure node 1 and node 3 are contacted and
            // that the LAP handles node 2's loss well
            for (int i=0; i < 10; ++i) {
                query(c, shortTest);
                showLatencyStats(latencyAwarePolicyInstance, hosts);
            }

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }

    /**
     * Helper for latencyAwarePrintTest().
     * @param latencyAwarePolicyInstance
     * @param hosts
     */
    private void showLatencyStats(LatencyAwarePolicy latencyAwarePolicyInstance, ArrayList<Host> hosts) {
        LatencyAwarePolicy.Snapshot currentLatencies = latencyAwarePolicyInstance.getScoresSnapshot();

        long minLatency = Long.MAX_VALUE;
        int totalQueried = 0;

        // calculate minLatency and totalQueried counts
        for (Host host : hosts) {
            LatencyAwarePolicy.Snapshot.Stats latency = currentLatencies.getStats(host);
            if (latency != null) {
                if (latency.getLatencyScore() < minLatency)
                    minLatency = latency.getLatencyScore();
                totalQueried += getQueried(host.toString().substring(1));
            }
        }

        // print headers
        logger.info(String.format("%20s %20s %20s %20s %20s %20s",
                "host", "latency.average", "latency.nbMeasure", "queried.count",
                "latency", "queried %"));

        // print found metrics
        for (Host host : hosts) {
            LatencyAwarePolicy.Snapshot.Stats latency = currentLatencies.getStats(host);
            int queriedCount = getQueried(host.toString().substring(1));
            if (latency != null)
                logger.info(String.format("%20s %20s %20s %20s %19sx %20s",
                        host, latency.getLatencyScore(), latency.getMeasurementsCount(),
                        queriedCount,
                        (float) latency.getLatencyScore() / minLatency,
                        Math.round((float) queriedCount / totalQueried * 100)));
        }

        // generate a space during printing for easy readability
        logger.info("");
    }
}
