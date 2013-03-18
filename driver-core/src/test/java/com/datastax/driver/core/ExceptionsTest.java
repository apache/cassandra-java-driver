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
import com.datastax.driver.core.exceptions.*;

import java.util.*;
import java.net.InetAddress;
import java.util.HashMap;

import org.junit.Test;
import static org.junit.Assert.*;


/**
 * Simple test of the Exception classes against a one node cluster.
 */
public class ExceptionsTest{

    @Test
    public void alreadyExistsException() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster cluster = CCMBridge.buildCluster(1, builder);
        try {
            Session session = cluster.session;
            String keyspace = "TestKeyspace";
            String table = "TestTable";

            String[] cqlCommands = new String[]{
                String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1),
                "USE " + keyspace,
                String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table)
            };

            // Create the schema once
            session.execute(cqlCommands[0]);
            session.execute(cqlCommands[1]);
            session.execute(cqlCommands[2]);

            // Try creating the keyspace again
            try {
                session.execute(cqlCommands[0]);
            } catch (AlreadyExistsException e) {
                String expected = String.format("Keyspace %s already exists", keyspace.toLowerCase());
                assertEquals(expected, e.getMessage());
                assertEquals(keyspace.toLowerCase(), e.getKeyspace());
                assertEquals(null, e.getTable());
                assertEquals(false, e.wasTableCreation());
            }

            session.execute(cqlCommands[1]);

            // Try creating the table again
            try {
                session.execute(cqlCommands[2]);
            } catch (AlreadyExistsException e) {
                String expected = String.format("Table %s.%s already exists", keyspace.toLowerCase(), table.toLowerCase());
                assertEquals(expected, e.getMessage());
                assertEquals(keyspace.toLowerCase(), e.getKeyspace());
                assertEquals(table.toLowerCase(), e.getTable());
                assertEquals(true, e.wasTableCreation());
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            cluster.discard();
        }
    }

    public void authenticationException() throws Exception {
        // TODO: Modify CCM to accept authenticated sessions
    }

    @Test
    public void noHostAvailableException() throws Exception {
        String ipAddress = "255.255.255.255";

        try {
            Cluster cluster = Cluster.builder().addContactPoints("255.255.255.255").build();
        } catch (NoHostAvailableException e) {
            assertEquals(String.format("All host(s) tried for query failed (tried: [/%s])", ipAddress), e.getMessage());

            HashMap<InetAddress, String> hm = new HashMap<InetAddress, String>();
            hm.put(InetAddress.getByName(ipAddress), "[/255.255.255.255] Cannot connect");
            assertEquals(hm, e.getErrors());
        }
    }

    @Test
    public void readTimeoutException() throws Exception {
        // TODO: Launch three nodes, send an async ALL query, kill a node, catch exception
    }

    @Test
    public void writeTimeoutException() throws Exception {
        // TODO: Launch three nodes, send an async ALL query, kill a node, catch exception
    }

    @Test
    public void unavailableException() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster cluster = CCMBridge.buildCluster(3, builder);
        try {
            Session session = cluster.session;
            CCMBridge bridge = cluster.cassandraCluster;

            String keyspace = "TestKeyspace";
            String table = "TestTable";
            int replicationFactor = 3;
            String key = "1";

            session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
            session.execute("USE " + keyspace);
            session.execute(String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

            session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));

            bridge.stop(2);
            try{
                session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (UnavailableException e) {
                String expectedError = String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", "ALL", 3, 2);
                assertEquals(expectedError, e.getMessage());
                assertEquals(ConsistencyLevel.ALL, e.getConsistency());
                assertEquals(replicationFactor, e.getRequiredReplicas());
                assertEquals(replicationFactor - 1, e.getAliveReplicas());
            }

            try{
                session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (UnavailableException e) {
                String expectedError = String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", "ALL", 3, 2);
                assertEquals(expectedError, e.getMessage());
                assertEquals(ConsistencyLevel.ALL, e.getConsistency());
                assertEquals(replicationFactor, e.getRequiredReplicas());
                assertEquals(replicationFactor - 1, e.getAliveReplicas());
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            cluster.discard();
        }
    }
}
