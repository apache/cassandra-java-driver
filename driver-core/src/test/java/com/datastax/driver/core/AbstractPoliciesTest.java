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
import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

import static com.datastax.driver.core.TestUtils.*;
import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;

public abstract class AbstractPoliciesTest {
    private static final boolean DEBUG = false;

    protected Map<InetAddress, Integer> coordinators = new HashMap<InetAddress, Integer>();
    protected PreparedStatement prepared;


    /**
     * Create schemas for the policy tests, depending on replication factors/strategies.
     */
    public static void createSchema(Session session) {
        createSchema(session, 1);
    }

    public static void createSchema(Session session, int replicationFactor) {
        session.execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, SIMPLE_KEYSPACE, replicationFactor));
        session.execute("USE " + SIMPLE_KEYSPACE);
        session.execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, i int)", SIMPLE_TABLE));
    }

    public static void createMultiDCSchema(Session session) {
        createMultiDCSchema(session, 1, 1);
    }

    public static void createMultiDCSchema(Session session, int dc1RF, int dc2RF) {
        session.execute(String.format(CREATE_KEYSPACE_GENERIC_FORMAT, SIMPLE_KEYSPACE, "NetworkTopologyStrategy", String.format("'dc1' : %d, 'dc2' : %d", dc1RF, dc2RF)));
        session.execute("USE " + SIMPLE_KEYSPACE);
        session.execute(String.format("CREATE TABLE %s (k int PRIMARY KEY, i int)", SIMPLE_TABLE));
    }


    /**
     * Coordinator management/count
     */
    protected void addCoordinator(ResultSet rs) {
        InetAddress coordinator = rs.getExecutionInfo().getQueriedHost().getAddress();
        Integer n = coordinators.get(coordinator);
        coordinators.put(coordinator, n == null ? 1 : n + 1);
    }

    protected void resetCoordinators() {
        coordinators = new HashMap<InetAddress, Integer>();
    }

    private String queriedMapString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        for (Map.Entry<InetAddress, Integer> entry : coordinators.entrySet())
            sb.append(entry.getKey()).append(" : ").append(entry.getValue()).append(", ");
        return sb.append("}").toString();
    }

    /**
     * Helper test methods
     */
    protected void assertQueried(String host, int n) {
        try {
            Integer queried = coordinators.get(InetAddress.getByName(host));
            if (DEBUG)
                System.out.println(String.format("Expected: %s\tReceived: %s", n, queried));
            else {
                assertEquals(queried == null ? 0 : queried, n, queriedMapString());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void assertQueriedAtLeast(String host, int n) {
        try {
            Integer queried = coordinators.get(InetAddress.getByName(host));
            queried = queried == null ? 0 : queried;
            if (DEBUG)
                System.out.println(String.format("Expected > %s\tReceived: %s", n, queried));
            else
                assertTrue(queried >= n, "For " + host);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /** Assert that one of the nodes in the list was queried with n, no matter which one */
    protected void assertOneNodeQueried(int n, String... hosts) {
        try {
            boolean found = false;
            for (String host : hosts) {
                InetAddress addr = InetAddress.getByName(host);
                int queried = coordinators.containsKey(addr) ? coordinators.get(addr) : 0;
                if (DEBUG)
                    System.out.println(String.format("Expected: %s\tReceived: %s", n, queried));
                else {

                    if (n == queried) {
                        if (found == true)
                            throw new AssertionError(String.format("Found 2 nodes with " + n + " queries in " + queriedMapString()));
                        found = true;
                    } else {
                        if (queried != 0)
                            throw new AssertionError(String.format("Host " + addr + " should have be queried: " + queriedMapString()));
                    }
                }
            }
            if (!found)
                throw new AssertionError("Found no host queried exactly " + n + " times in " + queriedMapString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void failDebug(String message) {
        if (DEBUG)
            System.out.println(message);
        else
            fail(message);
    }


    /**
     * Init methods that handle writes using batch and consistency options.
     */
    protected void init(CCMBridge.CCMCluster c, int n) {
        write(c, n, false, ConsistencyLevel.ONE);
        prepared = c.session.prepare("SELECT * FROM " + SIMPLE_TABLE + " WHERE k = ?").setConsistencyLevel(ConsistencyLevel.ONE);
    }

    protected void init(CCMBridge.CCMCluster c, int n, boolean batch) {
        write(c, n, batch, ConsistencyLevel.ONE);
        prepared = c.session.prepare("SELECT * FROM " + SIMPLE_TABLE + " WHERE k = ?").setConsistencyLevel(ConsistencyLevel.ONE);
    }

    protected void init(CCMBridge.CCMCluster c, int n, ConsistencyLevel cl) {
        write(c, n, false, cl);
        prepared = c.session.prepare("SELECT * FROM " + SIMPLE_TABLE + " WHERE k = ?").setConsistencyLevel(cl);
    }

    protected void write(CCMBridge.CCMCluster c, int n) {
        write(c, n, false, ConsistencyLevel.ONE);
    }

    protected void write(CCMBridge.CCMCluster c, int n, boolean batch) {
        write(c, n, batch, ConsistencyLevel.ONE);
    }

    protected void write(CCMBridge.CCMCluster c, int n, ConsistencyLevel cl) {
        write(c, n, false, cl);
    }

    protected void write(CCMBridge.CCMCluster c, int n, boolean batch, ConsistencyLevel cl) {
        // We don't use insert for our test because the resultSet don't ship the queriedHost
        // Also note that we don't use tracing because this would trigger requests that screw up the test
        for (int i = 0; i < n; ++i)
            if (batch)
                // BUG: WriteType == SIMPLE
                c.session.execute(batch()
                        .add(insertInto(SIMPLE_TABLE).values(new String[]{ "k", "i"}, new Object[]{ 0, 0 }))
                        .setConsistencyLevel(cl));
            else
                c.session.execute(new SimpleStatement(String.format("INSERT INTO %s(k, i) VALUES (0, 0)", SIMPLE_TABLE)).setConsistencyLevel(cl));
    }


    /**
     * Query methods that handle reads based on PreparedStatements and/or ConsistencyLevels.
     */
    protected void query(CCMBridge.CCMCluster c, int n) {
        query(c, n, false, ConsistencyLevel.ONE);
    }

    protected void query(CCMBridge.CCMCluster c, int n, boolean usePrepared) {
        query(c, n, usePrepared, ConsistencyLevel.ONE);
    }

    protected void query(CCMBridge.CCMCluster c, int n, ConsistencyLevel cl) {
        query(c, n, false, cl);
    }

    protected void query(CCMBridge.CCMCluster c, int n, boolean usePrepared, ConsistencyLevel cl) {
        if (usePrepared) {
            BoundStatement bs = prepared.bind(0);
            for (int i = 0; i < n; ++i)
                addCoordinator(c.session.execute(bs));
        } else {
            ByteBuffer routingKey = ByteBuffer.allocate(4);
            routingKey.putInt(0, 0);
            for (int i = 0; i < n; ++i)
                addCoordinator(c.session.execute(new SimpleStatement(String.format("SELECT * FROM %s WHERE k = 0", SIMPLE_TABLE)).setRoutingKey(routingKey).setConsistencyLevel(cl)));
        }
    }
}
