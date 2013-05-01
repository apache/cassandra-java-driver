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

import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.UnavailableException;
import static com.datastax.driver.core.TestUtils.*;

public class LoadBalancingPolicyTest extends AbstractPoliciesTest {
    private static final boolean DEBUG = false;

    private Map<InetAddress, Integer> coordinators = new HashMap<InetAddress, Integer>();
    private PreparedStatement prepared;

    @Test(groups = "integration")
    public void roundRobinTest() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy());
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        createSchema(c.session);
        try {

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

    @Test(groups = "integration")
    public void roundRobinWith2DCsTest() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new RoundRobinPolicy());
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, 2, builder);
        createSchema(c.session);
        try {

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

    @Test(groups = "integration")
    public void DCAwareRoundRobinTest() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2"));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, 2, builder);
        createMultiDCSchema(c.session);
        try {

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

    @Test(groups = "integration")
    public void dcAwareRoundRobinTestWithOneRemoteHost() throws Throwable {

        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new DCAwareRoundRobinPolicy("dc2", 1));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, 2, builder);
        createMultiDCSchema(c.session);
        try {

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

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(2);
            waitForDecommission(CCMBridge.IP_PREFIX + "2", c.cluster);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 12);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(1);
            waitForDown(CCMBridge.IP_PREFIX + "1", c.cluster);

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

    @Test(groups = "integration")
    public void tokenAwareTest() throws Throwable {
        tokenAwareTest(false);
    }

    @Test(groups = "integration")
    public void tokenAwarePreparedTest() throws Throwable {
        tokenAwareTest(true);
    }

    public void tokenAwareTest(boolean usePrepared) throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        createSchema(c.session);
        try {

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

    @Test(groups = "integration")
    public void tokenAwareWithRF2Test() throws Throwable {
        Cluster.Builder builder = Cluster.builder().withLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()));
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(2, builder);
        createSchema(c.session, 2);
        try {

            init(c, 12);
            query(c, 12);

            // Not the best test ever, we should use OPP and check we do it the
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

            assertQueried(CCMBridge.IP_PREFIX + "1", 6);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }
}
