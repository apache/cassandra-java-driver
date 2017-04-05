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

import com.datastax.driver.core.*;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static com.datastax.driver.core.TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@CCMConfig(config = "tombstone_failure_threshold:1000",
        numberOfNodes = 2,
        jvmArgs = "-Dcassandra.test.fail_writes_ks=ks_write_fail")
@CassandraVersion("2.2.0")
public class ReadWriteFailureExceptionTest extends CCMTestsSupport {
    /**
     * Validates that ReadFailureException occurs, and that in the case of protocol v5 the reason map
     * is surfaced appropriately on the exception.
     *
     * @jira_ticket JAVA-1424
     * @test_category error_codes
     */
    @Test(groups = "long")
    public void should_readFailure_on_tombstone_overwelmed() throws Throwable {
        //Create a table and insert 2000 tombstones
        session().execute("CREATE KEYSPACE ks_read_fail WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
        session().execute("CREATE TABLE ks_read_fail.foo(pk int, cc int, v int, primary key (pk, cc))");
        PreparedStatement prepared = session().prepare("INSERT INTO ks_read_fail.foo (pk, cc, v) VALUES (1, ?, null)");

        for (int v = 0; v < 2000; v++) {
            BoundStatement bound = prepared.bind(v);
            session().execute(bound);
        }
        // Attempt a query, since our tombstone failure threshold is set to 1000 this should error
        try {
            ResultSet result = session().execute("SELECT * FROM ks_read_fail.foo WHERE pk = 1");
            fail("A ReadFailureException should have been thrown here");
        } catch (ReadFailureException e) {
            if (cluster().getConfiguration().getProtocolOptions().getProtocolVersion().compareTo(ProtocolVersion.V5) >= 0) {
                assertThat(e.getFailuresMap())
                        .hasSize(1)
                        .containsValue(1);
            } else {
                assertThat(e.getFailuresMap()).isEmpty();
            }
        }
    }

    /**
     * Validates that a WriteFailureException occurs. In the case of protocol > v5 the reason map
     * is surfaced appropriately on the exception.
     *
     * @jira_ticket JAVA-1424
     * @test_category error_codes
     */
    @Test(groups = "long")
    public void should_writeFailure_on_error() throws Throwable {
        // Creates the failure keyspace and a table.
        session().execute(String.format(CREATE_KEYSPACE_SIMPLE_FORMAT, "ks_write_fail", 1));
        session().execute("CREATE TABLE ks_write_fail.foo(pk int, cc int, v int, primary key (pk, cc))");
        try {
            // This should fail because we have a the jvm arg cassandra.test.fail_writes_ks=ks_write_fail set.
            session().execute("INSERT INTO ks_write_fail.foo (pk, cc, v) VALUES (1, 1, null)");
            fail("A WriteFailureException should have been thrown here");
        } catch (WriteFailureException e) {

            if (cluster().getConfiguration().getProtocolOptions().getProtocolVersion().compareTo(ProtocolVersion.V5) >= 0) {
                assertThat(e.getFailuresMap())
                        .hasSize(1)
                        .containsValue(0);
            } else {
                assertThat(e.getFailuresMap()).isEmpty();
            }
        }
    }
}