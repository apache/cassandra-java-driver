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
import java.util.HashMap;

import static com.datastax.driver.core.TestUtils.waitForDown;
import org.apache.commons.lang.StringUtils;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

import com.datastax.driver.core.exceptions.*;

/**
 * Tests Exception classes with seperate clusters per test, when applicable
 */
public class ExceptionsTest {

    /**
     * Tests the AlreadyExistsException.
     * Create a keyspace twice and a table twice.
     * Catch and test all the exception methods.
     */
    @Test(groups = "short")
    public void alreadyExistsException() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(1, builder);
        try {
            String keyspace = "TestKeyspace";
            String table = "TestTable";

            String[] cqlCommands = new String[]{
                String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1),
                "USE " + keyspace,
                String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table)
            };

            // Create the schema once
            c.session.execute(cqlCommands[0]);
            c.session.execute(cqlCommands[1]);
            c.session.execute(cqlCommands[2]);

            // Try creating the keyspace again
            try {
                c.session.execute(cqlCommands[0]);
            } catch (AlreadyExistsException e) {
                String expected = String.format("Keyspace %s already exists", keyspace.toLowerCase());
                assertEquals(e.getMessage(), expected);
                assertEquals(e.getKeyspace(), keyspace.toLowerCase());
                assertEquals(e.getTable(), null);
                assertEquals(e.wasTableCreation(), false);
            }

            c.session.execute(cqlCommands[1]);

            // Try creating the table again
            try {
                c.session.execute(cqlCommands[2]);
            } catch (AlreadyExistsException e) {
                // TODO: Pending CASSANDRA-5362 this won't work. So let's re-enable this once C* 1.2.4
                // is released
                //assertEquals(e.getKeyspace(), keyspace.toLowerCase());
                //assertEquals(e.getTable(), table.toLowerCase());
                assertEquals(e.wasTableCreation(), true);
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            c.discard();
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
    @Test(groups = "unit")
    public void driverInternalError() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new DriverInternalError(errorMessage);
        } catch (DriverInternalError e1) {
            try {
                throw new DriverInternalError(e1);
            } catch (DriverInternalError e2) {
                assertTrue(StringUtils.contains(e2.getMessage(), errorMessage));

                DriverInternalError copy = (DriverInternalError) e2.copy();
                assertEquals(copy.getMessage(), e2.getMessage());
            }
        }
    }

    /**
     * Tests InvalidConfigurationInQueryException.
     * Tests basic message abilities.
     */
    @Test(groups = "unit")
    public void invalidConfigurationInQueryException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new InvalidConfigurationInQueryException(errorMessage);
        } catch (InvalidConfigurationInQueryException e) {
            assertEquals(e.getMessage(), errorMessage);
        }
    }

    /**
     * Tests InvalidQueryException.
     * Tests basic message and copy abilities.
     */
    @Test(groups = "unit")
    public void invalidQueryException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new InvalidQueryException(errorMessage);
        } catch (InvalidQueryException e) {
            assertEquals(e.getMessage(), errorMessage);

            InvalidQueryException copy = (InvalidQueryException) e.copy();
            assertEquals(copy.getMessage(), e.getMessage());
        }
    }

    /**
     * Tests InvalidTypeException.
     * Tests basic message and copy abilities.
     */
    @Test(groups = "unit")
    public void invalidTypeException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new InvalidTypeException(errorMessage);
        } catch (InvalidTypeException e) {
            assertEquals(e.getMessage(), errorMessage);

            InvalidTypeException copy = (InvalidTypeException) e.copy();
            assertEquals(copy.getMessage(), e.getMessage());
        }
    }

    /**
     * Tests the NoHostAvailableException.
     * by attempting to build a cluster using the IP address "255.255.255.255"
     * and test all available exception methods.
     */
    @Test(groups = "short")
    public void noHostAvailableException() throws Exception {
        String ipAddress = "255.255.255.255";
        HashMap<InetAddress, String> errorsHashMap = new HashMap<InetAddress, String>();
        errorsHashMap.put(InetAddress.getByName(ipAddress), "[/255.255.255.255] Cannot connect");

        try {
            Cluster.builder().addContactPoints("255.255.255.255").build();
        } catch (NoHostAvailableException e) {
            assertEquals(e.getErrors(), errorsHashMap);

            NoHostAvailableException copy = (NoHostAvailableException) e.copy();
            assertEquals(copy.getMessage(), e.getMessage());
            assertEquals(copy.getErrors(), e.getErrors());
        }
    }

    /**
     * Tests the ReadTimeoutException.
     * Create a 3 node cluster and write out a single key at CL.ALL.
     * Then forcibly kill single node and attempt a read of the key at CL.ALL.
     * Catch and test all available exception methods.
     */
    @Test(groups = "long")
    public void readTimeoutException() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {
            String keyspace = "TestKeyspace";
            String table = "TestTable";
            int replicationFactor = 3;
            String key = "1";

            c.session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
            c.session.execute("USE " + keyspace);
            c.session.execute(String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

            c.session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));

            c.cassandraCluster.forceStop(2);
            try{
                c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (ReadTimeoutException e) {
                assertEquals(e.getConsistencyLevel(), ConsistencyLevel.ALL);
                assertEquals(e.getReceivedAcknowledgements(), 2);
                assertEquals(e.getRequiredAcknowledgements(), 3);
                assertEquals(e.wasDataRetrieved(), true);

                ReadTimeoutException copy = (ReadTimeoutException) e.copy();
                assertEquals(copy.getMessage(), e.getMessage());
                assertEquals(copy.wasDataRetrieved(), e.wasDataRetrieved());
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            c.discard();
        }
    }

    /**
     * Tests SyntaxError.
     * Tests basic message and copy abilities.
     */
    @Test(groups = "unit")
    public void syntaxError() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new SyntaxError(errorMessage);
        } catch (SyntaxError e) {
            assertEquals(e.getMessage(), errorMessage);

            SyntaxError copy = (SyntaxError) e.copy();
            assertEquals(copy.getMessage(), e.getMessage());
        }
    }

    /**
     * Tests TraceRetrievalException.
     * Tests basic message and copy abilities.
     */
    @Test(groups = "unit")
    public void traceRetrievalException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new TraceRetrievalException(errorMessage);
        } catch (TraceRetrievalException e) {
            assertEquals(e.getMessage(), errorMessage);

            TraceRetrievalException copy = (TraceRetrievalException) e.copy();
            assertEquals(copy.getMessage(), e.getMessage());
        }
    }

    /**
     * Tests TruncateException.
     * Tests basic message and copy abilities.
     */
    @Test(groups = "unit")
    public void truncateException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new TruncateException(errorMessage);
        } catch (TruncateException e) {
            assertEquals(e.getMessage(), errorMessage);

            TruncateException copy = (TruncateException) e.copy();
            assertEquals(copy.getMessage(), e.getMessage());
        }
    }

    /**
     * Tests UnauthorizedException.
     * Tests basic message and copy abilities.
     */
    @Test(groups = "unit")
    public void unauthorizedException() throws Exception {
        String errorMessage = "Test Message";

        try {
            throw new UnauthorizedException(errorMessage);
        } catch (UnauthorizedException e) {
            assertEquals(e.getMessage(), errorMessage);

            UnauthorizedException copy = (UnauthorizedException) e.copy();
            assertEquals(copy.getMessage(), e.getMessage());
        }
    }

    /**
     * Tests the UnavailableException.
     * Create a 3 node cluster and write out a single key at CL.ALL.
     * Then kill single node, wait for gossip to propogate the new state,
     * and attempt to read and write the same key at CL.ALL.
     * Catch and test all available exception methods.
     */
    @Test(groups = "long")
    public void unavailableException() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {
            String keyspace = "TestKeyspace";
            String table = "TestTable";
            int replicationFactor = 3;
            String key = "1";

            c.session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
            c.session.execute("USE " + keyspace);
            c.session.execute(String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

            c.session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));

            c.cassandraCluster.stop(2);

            waitForDown(CCMBridge.IP_PREFIX + "2", c.cluster);

            try{
                c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (UnavailableException e) {
                String expectedError = String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", "ALL", 3, 2);
                assertEquals(e.getMessage(), expectedError);
                assertEquals(e.getConsistency(), ConsistencyLevel.ALL);
                assertEquals(e.getRequiredReplicas(), replicationFactor);
                assertEquals(e.getAliveReplicas(), replicationFactor - 1);
            }

            try{
                c.session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (UnavailableException e) {
                String expectedError = String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", "ALL", 3, 2);
                assertEquals(e.getMessage(), expectedError);
                assertEquals(e.getConsistency(), ConsistencyLevel.ALL);
                assertEquals(e.getRequiredReplicas(), replicationFactor);
                assertEquals(e.getAliveReplicas(), replicationFactor - 1);
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            c.discard();
        }
    }

    /**
     * Tests the WriteTimeoutException.
     * Create a 3 node cluster and write out a single key at CL.ALL.
     * Then forcibly kill single node and attempt to write the same key at CL.ALL.
     * Catch and test all available exception methods.
     */
    @Test(groups = "long")
    public void writeTimeoutException() throws Throwable {
        Cluster.Builder builder = Cluster.builder();
        CCMBridge.CCMCluster c = CCMBridge.buildCluster(3, builder);
        try {
            String keyspace = "TestKeyspace";
            String table = "TestTable";
            int replicationFactor = 3;
            String key = "1";

            c.session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, replicationFactor));
            c.session.execute("USE " + keyspace);
            c.session.execute(String.format(TestUtils.CREATE_TABLE_SIMPLE_FORMAT, table));

            c.session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));

            c.cassandraCluster.forceStop(2);
            try{
                c.session.execute(new SimpleStatement(String.format(TestUtils.INSERT_FORMAT, table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (WriteTimeoutException e) {
                assertEquals(e.getConsistencyLevel(), ConsistencyLevel.ALL);
                assertEquals(e.getReceivedAcknowledgements(), 2);
                assertEquals(e.getRequiredAcknowledgements(), 3);
                assertEquals(e.getWriteType(), WriteType.SIMPLE);
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            c.discard();
        }
    }
}
