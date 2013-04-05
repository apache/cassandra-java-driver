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
import java.nio.ByteBuffer;
import java.util.*;

import org.testng.annotations.Test;

import static org.testng.Assert.*;

import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import static com.datastax.driver.core.TestUtils.*;

public class LoadBalancingPolicyTest {

    private static final String TABLE = "test";

    private Map<InetAddress, Integer> coordinators = new HashMap<InetAddress, Integer>();
    private PreparedStatement prepared;

    private void createSchema(Session session) {
        session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, SIMPLE_KEYSPACE, 1));
        session.execute("USE " + SIMPLE_KEYSPACE);
        session.execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, i int)", TABLE));
    }

    private void createMultiDCSchema(Session session) {

        session.execute(String.format(CREATE_KEYSPACE_GENERIC_FORMAT, SIMPLE_KEYSPACE, "NetworkTopologyStrategy", "'dc1' : 1, 'dc2' : 1"));
        session.execute("USE " + SIMPLE_KEYSPACE);
        session.execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, i int)", TABLE));
    }

    private void addCoordinator(ResultSet rs) {
        InetAddress coordinator = rs.getExecutionInfo().getQueriedHost().getAddress();
        Integer n = coordinators.get(coordinator);
        coordinators.put(coordinator, n == null ? 1 : n + 1);
    }

    private void assertQueried(String host, int n) {
        try {
            Integer queried = coordinators.get(InetAddress.getByName(host));
            assertEquals(queried == null ? 0 : queried, n, "For " + host);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void resetCoordinators() {
        coordinators = new HashMap<InetAddress, Integer>();
    }

    private void init(CCMBridge.CCMCluster c, int n) {
        // We don't use insert for our test because the resultSet don't ship the queriedHost
        // Also note that we don't use tracing because this would trigger requests that screw up the test
        for (int i = 0; i < n; ++i)
            c.session.execute(String.format("INSERT INTO %s(k, i) VALUES (0, 0)", TABLE));

        prepared = c.session.prepare("SELECT * FROM " + TABLE + " WHERE k = ?");
    }

    private void query(CCMBridge.CCMCluster c, int n) {
        query(c, n, false);
    }

    private void query(CCMBridge.CCMCluster c, int n, boolean usePrepared) {
        if (usePrepared) {
            BoundStatement bs = prepared.bind(0);
            for (int i = 0; i < n; ++i)
                addCoordinator(c.session.execute(bs));
        } else {
            ByteBuffer routingKey = ByteBuffer.allocate(4);
            routingKey.putInt(0, 0);
            for (int i = 0; i < n; ++i)
                addCoordinator(c.session.execute(new SimpleStatement(String.format("SELECT * FROM %s WHERE k = 0", TABLE)).setRoutingKey(routingKey)));
        }
    }

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
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster, 20);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 4);
            assertQueried(CCMBridge.IP_PREFIX + "2", 4);
            assertQueried(CCMBridge.IP_PREFIX + "3", 4);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(1);
            waitForDecommission(CCMBridge.IP_PREFIX + "1", c.cluster, 20);

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
    public void tokenAwareTest() throws Throwable {
        tokenAwareTest(false);
    }

    @Test(groups = "integration")
    public void tokenAwarePreparedTest() throws Throwable {
        tokenAwareTest(true);
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
            waitFor(CCMBridge.IP_PREFIX + "5", c.cluster, 20);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 6);
            assertQueried(CCMBridge.IP_PREFIX + "4", 6);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(3);
            c.cassandraCluster.decommissionNode(4);
            waitForDecommission(CCMBridge.IP_PREFIX + "3", c.cluster, 20);
            waitForDecommission(CCMBridge.IP_PREFIX + "4", c.cluster, 20);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 12);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(5);
            waitForDecommission(CCMBridge.IP_PREFIX + "5", c.cluster, 20);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.decommissionNode(2);
            waitForDecommission(CCMBridge.IP_PREFIX + "2", c.cluster, 20);

            query(c, 12);

            assertQueried(CCMBridge.IP_PREFIX + "1", 12);
            assertQueried(CCMBridge.IP_PREFIX + "2", 0);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);
            assertQueried(CCMBridge.IP_PREFIX + "4", 0);
            assertQueried(CCMBridge.IP_PREFIX + "5", 0);

            resetCoordinators();
            c.cassandraCluster.forceStop(1);

            try {
                query(c, 12);
                assertTrue(false);
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
            c.cassandraCluster.bootstrapNode(3);
            waitFor(CCMBridge.IP_PREFIX + "3", c.cluster, 20);

            query(c, 12, usePrepared);

            // We should still be hitting only one node
            assertQueried(CCMBridge.IP_PREFIX + "1", 0);
            assertQueried(CCMBridge.IP_PREFIX + "2", 12);
            assertQueried(CCMBridge.IP_PREFIX + "3", 0);

        } catch (Throwable e) {
            c.errorOut();
            throw e;
        } finally {
            resetCoordinators();
            c.discard();
        }
    }
}
