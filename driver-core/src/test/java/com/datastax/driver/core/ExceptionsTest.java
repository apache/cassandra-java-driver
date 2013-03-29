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
import org.junit.matchers.JUnitMatchers;
import static org.junit.Assert.*;

/**
 * Tests Exception classes with seperate clusters per test, when applicable
 */
public class ExceptionsTest{

    /**
     * Tests the AlreadyExistsException.
     * Create a keyspace twice and a table twice.
     * Catch and test all the exception methods.
     */
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
                // TODO: Pending CASSANDRA-5362 this won't work. So let's re-enable this once C* 1.2.4
                // is released
                //assertEquals(keyspace.toLowerCase(), e.getKeyspace());
                //assertEquals(table.toLowerCase(), e.getTable());
                assertEquals(true, e.wasTableCreation());
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            cluster.discard();
        }
    }

    /**
     * Placeholder test for the AuthenticationException.
     * Testing pending CCM authenticated sessions integration.
     */
    public void authenticationException() throws Exception {
        // TODO: Modify CCM to accept authenticated sessions
    }

    /**
     * Tests DriverInternalError.
     * Tests basic message, rethrow, and copy abilities.
     */
    @Test
    public void driverInternalError() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new DriverInternalError(errorMessage);
        } catch (DriverInternalError e1) {
            try {
                throw new DriverInternalError(e1);
            } catch (DriverInternalError e2) {
                assertThat(e2.getMessage(), JUnitMatchers.containsString(errorMessage));

                DriverInternalError copy = (DriverInternalError) e2.copy();
                assertEquals(e2.getMessage(), copy.getMessage());
            }
        }
    }

    /**
     * Tests InvalidConfigurationInQueryException.
     * Tests basic message abilities.
     */
    @Test
    public void invalidConfigurationInQueryException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new InvalidConfigurationInQueryException(errorMessage);
        } catch (InvalidConfigurationInQueryException e) {
            assertEquals(errorMessage, e.getMessage());
        }
    }

    /**
     * Tests InvalidQueryException.
     * Tests basic message and copy abilities.
     */
    @Test
    public void invalidQueryException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new InvalidQueryException(errorMessage);
        } catch (InvalidQueryException e) {
            assertEquals(errorMessage, e.getMessage());

            InvalidQueryException copy = (InvalidQueryException) e.copy();
            assertEquals(e.getMessage(), copy.getMessage());
        }
    }

    /**
     * Tests InvalidTypeException.
     * Tests basic message and copy abilities.
     */
    @Test
    public void invalidTypeException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new InvalidTypeException(errorMessage);
        } catch (InvalidTypeException e) {
            assertEquals(errorMessage, e.getMessage());

            InvalidTypeException copy = (InvalidTypeException) e.copy();
            assertEquals(e.getMessage(), copy.getMessage());
        }
    }

    /**
     * Tests the NoHostAvailableException.
     * by attempting to build a cluster using the IP address "255.255.255.255"
     * and test all available exception methods.
     */
    @Test
    public void noHostAvailableException() throws Exception {
        String ipAddress = "255.255.255.255";
        HashMap<InetAddress, String> errorsHashMap = new HashMap<InetAddress, String>();
        errorsHashMap.put(InetAddress.getByName(ipAddress), "[/255.255.255.255] Cannot connect");

        try {
            Cluster cluster = Cluster.builder().addContactPoints("255.255.255.255").build();
        } catch (NoHostAvailableException e) {
            assertEquals(String.format("All host(s) tried for query failed (tried: [/%s])", ipAddress), e.getMessage());
            assertEquals(errorsHashMap, e.getErrors());

            NoHostAvailableException copy = (NoHostAvailableException) e.copy();
            assertEquals(e.getMessage(), copy.getMessage());
            assertEquals(e.getErrors(), copy.getErrors());
        }
    }

    /**
     * Tests the ReadTimeoutException.
     * Create a 3 node cluster and write out a single key at CL.ALL.
     * Then forcibly kill single node and attempt a read of the key at CL.ALL.
     * Catch and test all available exception methods.
     */
    @Test
    public void readTimeoutException() throws Throwable {
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

            bridge.forceStop(2);
            try{
                session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (ReadTimeoutException e) {
                assertEquals(ConsistencyLevel.ALL, e.getConsistencyLevel());
                assertEquals(2, e.getReceivedAcknowledgements());
                assertEquals(3, e.getRequiredAcknowledgements());
                assertEquals(e.wasDataRetrieved(), true);

                ReadTimeoutException copy = (ReadTimeoutException) e.copy();
                assertEquals(e.getMessage(), copy.getMessage());
                assertEquals(e.wasDataRetrieved(), copy.wasDataRetrieved());
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            cluster.discard();
        }
    }

    /**
     * Tests SyntaxError.
     * Tests basic message and copy abilities.
     */
    @Test
    public void syntaxError() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new SyntaxError(errorMessage);
        } catch (SyntaxError e) {
            assertEquals(errorMessage, e.getMessage());

            SyntaxError copy = (SyntaxError) e.copy();
            assertEquals(e.getMessage(), copy.getMessage());
        }
    }

    /**
     * Tests TraceRetrievalException.
     * Tests basic message and copy abilities.
     */
    @Test
    public void traceRetrievalException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new TraceRetrievalException(errorMessage);
        } catch (TraceRetrievalException e) {
            assertEquals(errorMessage, e.getMessage());

            TraceRetrievalException copy = (TraceRetrievalException) e.copy();
            assertEquals(e.getMessage(), copy.getMessage());
        }
    }

    /**
     * Tests TruncateException.
     * Tests basic message and copy abilities.
     */
    @Test
    public void truncateException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new TruncateException(errorMessage);
        } catch (TruncateException e) {
            assertEquals(errorMessage, e.getMessage());

            TruncateException copy = (TruncateException) e.copy();
            assertEquals(e.getMessage(), copy.getMessage());
        }
    }

    /**
     * Tests UnauthorizedException.
     * Tests basic message and copy abilities.
     */
    @Test
    public void unauthorizedException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new UnauthorizedException(errorMessage);
        } catch (UnauthorizedException e) {
            assertEquals(errorMessage, e.getMessage());

            UnauthorizedException copy = (UnauthorizedException) e.copy();
            assertEquals(e.getMessage(), copy.getMessage());
        }
    }

    /**
     * Tests the UnavailableException.
     * Create a 3 node cluster and write out a single key at CL.ALL.
     * Then kill single node, wait for gossip to propogate the new state,
     * and attempt to read and write the same key at CL.ALL.
     * Catch and test all available exception methods.
     */
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
            // Ensure that gossip has reported the node as down.
            Thread.sleep(1000);

            try{
                session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (UnavailableException e) {
                String expectedError = String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", "ALL", 3, 2);
                assertEquals(expectedError, e.getMessage());
                assertEquals(ConsistencyLevel.ALL, e.getConsistency());
                assertEquals(replicationFactor, e.getRequiredReplicas());
                assertEquals(replicationFactor - 1, e.getAliveReplicas());
            }

            try{
                session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
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

    /**
     * Tests the WriteTimeoutException.
     * Create a 3 node cluster and write out a single key at CL.ALL.
     * Then forcibly kill single node and attempt to write the same key at CL.ALL.
     * Catch and test all available exception methods.
     */
    @Test
    public void writeTimeoutException() throws Throwable {
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

            bridge.forceStop(2);
            try{
                session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (WriteTimeoutException e) {
                assertEquals(ConsistencyLevel.ALL, e.getConsistencyLevel());
                assertEquals(2, e.getReceivedAcknowledgements());
                assertEquals(3, e.getRequiredAcknowledgements());
                assertEquals(WriteType.SIMPLE, e.getWriteType());
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            cluster.discard();
        }
    }
}
