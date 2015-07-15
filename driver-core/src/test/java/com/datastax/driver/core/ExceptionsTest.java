/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import java.util.Locale;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.datastax.driver.core.utils.CassandraVersion;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static com.datastax.driver.core.TestUtils.waitForDownWithWait;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
                String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", table)
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
                // is released
                assertEquals(e.getKeyspace(), keyspace.toLowerCase());
                assertEquals(e.getTable(), table.toLowerCase());
                assertEquals(e.wasTableCreation(), true);
            }
        } catch (Throwable e) {
            throw e;
        } finally {
            c.discard();
        }
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
                assertTrue(e2.getMessage().contains(errorMessage));

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
        try {
            Cluster.builder().addContactPoints("255.255.255.255").build();
        } catch (NoHostAvailableException e) {
            assertEquals(e.getErrors().size(), 1);
            assertTrue(e.getErrors().values().iterator().next().toString().contains("[/255.255.255.255] Cannot connect"));

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
            c.session.execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", table));

            c.session.execute(new SimpleStatement(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
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
            c.session.execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", table));

            c.session.execute(new SimpleStatement(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));

            c.cassandraCluster.stop(2);

            waitForDownWithWait(CCMBridge.IP_PREFIX + '2', c.cluster, 10);

            try{
                c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (UnavailableException e) {
                String expectedError = String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", "ALL", 3, 2);
                assertEquals(e.getMessage(), expectedError);
                assertEquals(e.getConsistencyLevel(), ConsistencyLevel.ALL);
                assertEquals(e.getRequiredReplicas(), replicationFactor);
                assertEquals(e.getAliveReplicas(), replicationFactor - 1);
            }

            try{
                c.session.execute(new SimpleStatement(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            } catch (UnavailableException e) {
                String expectedError = String.format("Not enough replica available for query at consistency %s (%d required but only %d alive)", "ALL", 3, 2);
                assertEquals(e.getMessage(), expectedError);
                assertEquals(e.getConsistencyLevel(), ConsistencyLevel.ALL);
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
            c.session.execute(String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", table));

            c.session.execute(new SimpleStatement(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
            c.session.execute(new SimpleStatement(String.format(TestUtils.SELECT_ALL_FORMAT, table)).setConsistencyLevel(ConsistencyLevel.ALL));

            c.cassandraCluster.forceStop(2);
            try{
                c.session.execute(new SimpleStatement(String.format(Locale.US, "INSERT INTO %s (k, t, i, f) VALUES ('%s', '%s', %d, %f)", table, key, "foo", 42, 24.03f)).setConsistencyLevel(ConsistencyLevel.ALL));
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

    /**
     * Validates that a write_failure emitted from a coordinator is properly surfaced as a WriteFailureException.
     *
     * Uses JVM argument '-Dcassandra.test.fail_writes_ks=keyspace' in C* to generate write failures for a particular
     * keyspace to reliably generate write_failure-based errors.
     *
     * @since 2.2.0
     * @jira_ticket JAVA-749
     * @expected_result WriteFailureException is generated for a failed read.
     * @test_category queries:basic
     */
    @CassandraVersion(major=2.2)
    @Test(groups = "long", expectedExceptions = WriteFailureException.class)
    public void should_rethrow_write_failure_when_encountered_on_replica() throws InterruptedException {
        String keyspace = "writefailureks";

        CCMBridge ccm = null;
        Cluster cluster = null;

        try {
            ccm = CCMBridge.builder("test").withNodes(2).notStarted().build();
            // Configure 1 of the nodes to fail writes to the keyspace.
            ccm.start(1, "-Dcassandra.test.fail_writes_ks=" +keyspace);
            ccm.start(2);

            cluster = Cluster.builder()
                    .addContactPoint(CCMBridge.ipOfNode(2))
                    .withLoadBalancingPolicy(new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(),
                            Lists.newArrayList(new InetSocketAddress(CCMBridge.ipOfNode(2), 9042))))
                    .build();

            Session session = cluster.connect();

            session.execute(String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 2));
            session.execute("USE " + keyspace);
            session.execute("CREATE TABLE testtable (k text PRIMARY KEY, t text, i int, f float)");

            try {
                // Query with a CL of ALL to hit all replicas.
                session.execute(new SimpleStatement(String.format(Locale.US, "INSERT INTO testtable (k, t, i, f) VALUES ('%s', '%s', %d, %f)", "1", "2", 3, 4.0f))
                        .setConsistencyLevel(ConsistencyLevel.ALL));
            } catch(WriteFailureException e) {
                // Expect a failure for node configured to fail writes.
                assertThat(e.getFailures()).isEqualTo(1);
                // The one node not configured to fail writes should succeed if response processed quickly enough.
                assertThat(e.getReceivedAcknowledgements()).isLessThanOrEqualTo(1);
                // There should be 2 required acknowledgements since CL ALL is used.
                assertThat(e.getRequiredAcknowledgements()).isEqualTo(2);
                // A simple query was executed so that should be the write type.
                assertThat(e.getWriteType()).isEqualTo(WriteType.SIMPLE);
                // Rethrow to trigger validation on expectedExceptions.
                throw e;
            }
        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }

    /**
     * Validates that a read_failure emitted from a coordinator is properly surfaced as a ReadFailureException.
     *
     * Configures C* nodes with a 'tombstone_failure_threshold' of 1000 so that whenever of a read of a row scans more
     * than 1000 tombstones an exception is emitted, which should cause a read_failure.
     *
     * @since 2.2.0
     * @jira_ticket JAVA-749
     * @expected_result ReadFailureException is generated for a failed read caused by the tombstone_failure_threshold
     *   being exceeded.
     * @test_category queries:basic
     */
    @Test(groups = "short", expectedExceptions = ReadFailureException.class)
    @CassandraVersion(major = 2.2)
    public void should_rethrow_read_failure_when_tombstone_overwhelm_on_replica() throws InterruptedException {
        CCMBridge ccm = null;
        Cluster cluster = null;
        try {
            ccm = CCMBridge.builder("test")
                .withNodes(2)
                .withCassandraConfiguration("tombstone_failure_threshold", "1000")
                .build();

            // The rest of the test relies on the fact that the PK '1' will be placed on node1
            // (hard-coding it is reasonable considering that C* shouldn't change its default partitioner too often)
            cluster = Cluster.builder()
                .addContactPoint(CCMBridge.ipOfNode(2))
                .withLoadBalancingPolicy(new WhiteListPolicy(Policies.defaultLoadBalancingPolicy(),
                    Lists.newArrayList(new InetSocketAddress(CCMBridge.ipOfNode(2), 9042))))
                    .build();

            Session session = cluster.connect();
            session.execute("create KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            session.execute("create table test.foo(pk int, cc int, v int, primary key (pk, cc))");

            // Generate 1001 tombstones on node1
            for (int i = 0; i < 1001; i++)
                session.execute("insert into test.foo (pk, cc, v) values (1, ?, null)", i);

            try {
                session.execute("select * from test.foo");
            } catch(ReadFailureException e) {
                assertThat(e.getFailures()).isEqualTo(1);
                assertThat(e.wasDataRetrieved()).isFalse();
                assertThat(e.getReceivedAcknowledgements()).isEqualTo(0);
                assertThat(e.getRequiredAcknowledgements()).isEqualTo(1);
                // Rethrow to trigger validation on expectedExceptions.
                throw e;
            }

        } finally {
            if (cluster != null)
                cluster.close();
            if (ccm != null)
                ccm.remove();
        }
    }
}
