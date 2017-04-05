/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core.exceptions;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.TestUtils;
import com.datastax.driver.core.WriteType;
import org.testng.annotations.Test;

import java.net.InetSocketAddress;

import static com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests Exception classes with separate clusters per test, when applicable
 */
public class ExceptionsTest extends CCMTestsSupport {

    private InetSocketAddress address1 = new InetSocketAddress("127.0.0.1", 9042);
    private InetSocketAddress address2 = new InetSocketAddress("127.0.0.2", 9042);

    /**
     * Tests the AlreadyExistsException.
     * Create a keyspace twice and a table twice.
     * Catch and test all the exception methods.
     */
    @Test(groups = "short")
    public void alreadyExistsException() throws Throwable {
        String keyspace = "TestKeyspace";
        String table = "TestTable";

        String[] cqlCommands = new String[]{
                String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace, 1),
                "USE " + keyspace,
                String.format("CREATE TABLE %s (k text PRIMARY KEY, t text, i int, f float)", table)
        };

        // Create the schema once
        session().execute(cqlCommands[0]);
        session().execute(cqlCommands[1]);
        session().execute(cqlCommands[2]);

        // Try creating the keyspace again
        try {
            session().execute(cqlCommands[0]);
        } catch (AlreadyExistsException e) {
            String expected = String.format("Keyspace %s already exists", keyspace.toLowerCase());
            assertEquals(e.getMessage(), expected);
            assertEquals(e.getKeyspace(), keyspace.toLowerCase());
            assertEquals(e.getTable(), null);
            assertEquals(e.wasTableCreation(), false);
            assertEquals(e.getHost(), ccm().addressOfNode(1).getAddress());
            assertEquals(e.getAddress(), ccm().addressOfNode(1));
        }

        session().execute(cqlCommands[1]);

        // Try creating the table again
        try {
            session().execute(cqlCommands[2]);
        } catch (AlreadyExistsException e) {
            // is released
            assertEquals(e.getKeyspace(), keyspace.toLowerCase());
            assertEquals(e.getTable(), table.toLowerCase());
            assertEquals(e.wasTableCreation(), true);
            assertEquals(e.getHost(), ccm().addressOfNode(1).getAddress());
            assertEquals(e.getAddress(), ccm().addressOfNode(1));
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

    @Test(groups = "unit")
    public void should_create_proper_already_exists_exception_for_keyspaces() {
        AlreadyExistsException e = new AlreadyExistsException(address1, "keyspace1", "");
        assertThat(e.getMessage()).isEqualTo("Keyspace keyspace1 already exists");
        assertThat(e.getKeyspace()).isEqualTo("keyspace1");
        assertThat(e.getTable()).isNull();
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy(address2);
        assertThat(e.getMessage()).isEqualTo("Keyspace keyspace1 already exists");
        assertThat(e.getKeyspace()).isEqualTo("keyspace1");
        assertThat(e.getTable()).isNull();
        assertThat(e.getAddress()).isEqualTo(address2);
        assertThat(e.getHost()).isEqualTo(address2.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_already_exists_exception_for_tables() {
        AlreadyExistsException e = new AlreadyExistsException(address1, "keyspace1", "table1");
        assertThat(e.getMessage()).isEqualTo("Table keyspace1.table1 already exists");
        assertThat(e.getKeyspace()).isEqualTo("keyspace1");
        assertThat(e.getTable()).isEqualTo("table1");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy(address2);
        assertThat(e.getMessage()).isEqualTo("Table keyspace1.table1 already exists");
        assertThat(e.getKeyspace()).isEqualTo("keyspace1");
        assertThat(e.getTable()).isEqualTo("table1");
        assertThat(e.getAddress()).isEqualTo(address2);
        assertThat(e.getHost()).isEqualTo(address2.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_bootstrapping_exception() {
        BootstrappingException e = new BootstrappingException(address1, "Sorry mate");
        assertThat(e.getMessage()).isEqualTo("Queried host (" + address1 + ") was bootstrapping: Sorry mate");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy();
        assertThat(e.getMessage()).isEqualTo("Queried host (" + address1 + ") was bootstrapping: Sorry mate");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_invalid_query_exception() {
        InvalidQueryException e = new InvalidQueryException("Bad, really bad");
        assertThat(e.getMessage()).isEqualTo("Bad, really bad");
        e = (InvalidQueryException) e.copy();
        assertThat(e.getMessage()).isEqualTo("Bad, really bad");
    }

    @Test(groups = "unit")
    public void should_create_proper_trace_retrieval_exception() {
        TraceRetrievalException e = new TraceRetrievalException("Couldn't find any trace of it");
        assertThat(e.getMessage()).isEqualTo("Couldn't find any trace of it");
        e = (TraceRetrievalException) e.copy();
        assertThat(e.getMessage()).isEqualTo("Couldn't find any trace of it");
    }

    @Test(groups = "unit")
    public void should_create_proper_paging_state_exception() {
        PagingStateException e = new PagingStateException("Bad, really bad");
        assertThat(e.getMessage()).isEqualTo("Bad, really bad");
        // no copy method for this exception
    }

    @Test(groups = "unit")
    public void should_create_proper_invalid_configuration_in_query_exception() {
        InvalidConfigurationInQueryException e = new InvalidConfigurationInQueryException(address1, "Bad, really bad");
        assertThat(e.getMessage()).isEqualTo("Bad, really bad");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        InvalidQueryException e1 = (InvalidQueryException) e.copy();
        assertThat(e1.getMessage()).isEqualTo("Bad, really bad");
    }

    @Test(groups = "unit")
    public void should_create_proper_overloaded_exception() {
        OverloadedException e = new OverloadedException(address1, "I'm busy");
        assertThat(e.getMessage()).isEqualTo("Queried host (" + address1 + ") was overloaded: I'm busy");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy();
        assertThat(e.getMessage()).isEqualTo("Queried host (" + address1 + ") was overloaded: I'm busy");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_syntax_error() {
        SyntaxError e = new SyntaxError(address1, "Missing ) at EOF");
        assertThat(e.getMessage()).isEqualTo("Missing ) at EOF");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = (SyntaxError) e.copy();
        assertThat(e.getMessage()).isEqualTo("Missing ) at EOF");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_truncate_exception() {
        TruncateException e = new TruncateException(address1, "I'm running headless now");
        assertThat(e.getMessage()).isEqualTo("I'm running headless now");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = (TruncateException) e.copy();
        assertThat(e.getMessage()).isEqualTo("I'm running headless now");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_unauthorized_exception() {
        UnauthorizedException e = new UnauthorizedException(address1, "You talking to me?");
        assertThat(e.getMessage()).isEqualTo("You talking to me?");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = (UnauthorizedException) e.copy();
        assertThat(e.getMessage()).isEqualTo("You talking to me?");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_unavailable_exception() {
        UnavailableException e = new UnavailableException(address1, LOCAL_QUORUM, 3, 2);
        assertThat(e.getMessage()).isEqualTo("Not enough replicas available for query at consistency LOCAL_QUORUM (3 required but only 2 alive)");
        assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
        assertThat(e.getAliveReplicas()).isEqualTo(2);
        assertThat(e.getRequiredReplicas()).isEqualTo(3);
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy(address2);
        assertThat(e.getMessage()).isEqualTo("Not enough replicas available for query at consistency LOCAL_QUORUM (3 required but only 2 alive)");
        assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
        assertThat(e.getAliveReplicas()).isEqualTo(2);
        assertThat(e.getRequiredReplicas()).isEqualTo(3);
        assertThat(e.getAddress()).isEqualTo(address2);
        assertThat(e.getHost()).isEqualTo(address2.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_unprepared_exception() {
        UnpreparedException e = new UnpreparedException(address1, "Caught me unawares");
        assertThat(e.getMessage()).isEqualTo("A prepared query was submitted on " + address1 + " but was not known of that node: Caught me unawares");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy();
        assertThat(e.getMessage()).isEqualTo("A prepared query was submitted on " + address1 + " but was not known of that node: Caught me unawares");
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_read_timeout_exception() {
        ReadTimeoutException e = new ReadTimeoutException(address1, LOCAL_QUORUM, 2, 3, true);
        assertThat(e.getMessage()).isEqualTo("Cassandra timeout during read query at consistency LOCAL_QUORUM (3 responses were required but only 2 replica responded)");
        assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
        assertThat(e.getReceivedAcknowledgements()).isEqualTo(2);
        assertThat(e.getRequiredAcknowledgements()).isEqualTo(3);
        assertThat(e.wasDataRetrieved()).isTrue();
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy(address2);
        assertThat(e.getMessage()).isEqualTo("Cassandra timeout during read query at consistency LOCAL_QUORUM (3 responses were required but only 2 replica responded)");
        assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
        assertThat(e.getReceivedAcknowledgements()).isEqualTo(2);
        assertThat(e.getRequiredAcknowledgements()).isEqualTo(3);
        assertThat(e.wasDataRetrieved()).isTrue();
        assertThat(e.getAddress()).isEqualTo(address2);
        assertThat(e.getHost()).isEqualTo(address2.getAddress());
    }

    @Test(groups = "unit")
    public void should_create_proper_write_timeout_exception() {
        WriteTimeoutException e = new WriteTimeoutException(address1, LOCAL_QUORUM, WriteType.BATCH, 2, 3);
        assertThat(e.getMessage()).isEqualTo("Cassandra timeout during write query at consistency LOCAL_QUORUM (3 replica were required but only 2 acknowledged the write)");
        assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
        assertThat(e.getReceivedAcknowledgements()).isEqualTo(2);
        assertThat(e.getRequiredAcknowledgements()).isEqualTo(3);
        assertThat(e.getWriteType()).isEqualTo(WriteType.BATCH);
        assertThat(e.getAddress()).isEqualTo(address1);
        assertThat(e.getHost()).isEqualTo(address1.getAddress());
        e = e.copy(address2);
        assertThat(e.getMessage()).isEqualTo("Cassandra timeout during write query at consistency LOCAL_QUORUM (3 replica were required but only 2 acknowledged the write)");
        assertThat(e.getConsistencyLevel()).isEqualTo(LOCAL_QUORUM);
        assertThat(e.getReceivedAcknowledgements()).isEqualTo(2);
        assertThat(e.getRequiredAcknowledgements()).isEqualTo(3);
        assertThat(e.getWriteType()).isEqualTo(WriteType.BATCH);
        assertThat(e.getAddress()).isEqualTo(address2);
        assertThat(e.getHost()).isEqualTo(address2.getAddress());
    }
}
