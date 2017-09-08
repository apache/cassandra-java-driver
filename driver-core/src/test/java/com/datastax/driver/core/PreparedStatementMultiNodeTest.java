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
package com.datastax.driver.core;

import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.utils.CassandraVersion;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.fail;

@CCMConfig(numberOfNodes = 2, dirtiesContext = true)
@CreateCCM(CreateCCM.TestMode.PER_METHOD)
public class PreparedStatementMultiNodeTest extends CCMTestsSupport {

    private static final String keyspace2 = TestUtils.generateIdentifier("ks_");
    private static final String keyspace3 = TestUtils.generateIdentifier("ks_");

    @Override
    public void onTestContextInitialized() {
        execute(
                String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace2, 2),
                String.format("CREATE TABLE %s.users(id int, id2 int, name text, primary key (id, id2))", keyspace2),
                String.format("INSERT INTO %s.users(id, id2, name) VALUES (2, 2, 'test2')", keyspace2),
                String.format(TestUtils.CREATE_KEYSPACE_SIMPLE_FORMAT, keyspace3, 2),
                String.format("CREATE TABLE %s.users(id int, id2 int, name text, primary key (id, id2))", keyspace3),
                String.format("INSERT INTO %s.users(id, id2, name) VALUES (3, 3, 'test3')", keyspace3)
        );
    }

    @Test(groups = "long")
    @CassandraVersion("4.0.0")
    public void should_be_able_to_execute_statement_on_restarted_node_reprepare_on_up_set_keyspace() {
        // validate that if a node is restarted we should be able to execute a bound statement against it
        // as the statement is reprepared when the node comes back up.
        // uses the Protocol V5 per-query keyspace strategy.
        executeAfterNodeBroughtBackUp(true, true);
    }

    @Test(groups = "long")
    @CassandraVersion("4.0.0")
    public void should_be_able_to_execute_statement_on_restarted_node_set_keyspace() {
        // validate that if a node is restarted we should be able to execute a bound statement against it
        // as the statement is reprepared when we receive an 'unprepared' response.
        // uses the Protocol V5 per-query keyspace strategy.
        executeAfterNodeBroughtBackUp(false, true);
    }

    @Test(groups = "long")
    public void should_be_able_to_execute_statement_on_restarted_node_reprepare_on_up_use_keyspace() {
        // validate that if a node is restarted we should be able to execute a bound statement against it
        // as the statement is reprepared when the node comes back up.
        // uses the "USE keyspace" strategy which is an anti-pattern.
        executeAfterNodeBroughtBackUp(true, false);
    }

    @Test(groups = "long")
    public void should_be_able_to_execute_statement_on_restarted_node_use_keyspace() {
        // validate that if a node is restarted we should be able to execute a bound statement against it
        // as the statement is reprepared when we receive an 'unprepared' response.
        // uses the Protocol V5 per-query keyspace strategy.
        executeAfterNodeBroughtBackUp(false, false);
    }

    public void executeAfterNodeBroughtBackUp(boolean reprepareOnUp, boolean setKeyspace) {
        QueryOptions queryOptions = new QueryOptions().setReprepareOnUp(reprepareOnUp).setPrepareOnAllHosts(false);
        Cluster.Builder builder = createClusterBuilderNoDebouncing()
                .withNettyOptions(TestUtils.nonQuietClusterCloseOptions)
                .addContactPointsWithPorts(getContactPointsWithPorts())
                .withPort(ccm().getBinaryPort())
                .withQueryOptions(queryOptions)
                .withLoadBalancingPolicy(new SortingLoadBalancingPolicy());

        // TODO, remove this when V5 is no longer beta.
        if (setKeyspace) {
            builder = builder.allowBetaProtocolVersion();
        }

        Cluster cluster = builder.build();
        try {
            Session session = cluster.connect();

            PreparedStatement prep2;
            PreparedStatement prep3;

            if (setKeyspace) {
                prep2 = session.prepare(new SimpleStatement("SELECT * from users").setKeyspace(keyspace2));
                prep3 = session.prepare(new SimpleStatement("SELECT * from users").setKeyspace(keyspace3));
            } else {
                // Execute use keyspace before preparing.  This is an anti-pattern but works nonetheless.
                session.execute("USE " + keyspace2);
                prep2 = session.prepare(new SimpleStatement("SELECT * from users"));
                session.execute("USE " + keyspace3);
                prep3 = session.prepare(new SimpleStatement("SELECT * from users"));
            }

            Host host1 = TestUtils.findHost(cluster, 1);
            Host host2 = TestUtils.findHost(cluster, 2);

            // Execute queries 10 times, should always hit host1 due to sorting policy.
            for (int i = 0; i < 10; i++) {
                if (!setKeyspace) {
                    session.execute("USE " + keyspace2);
                }
                ResultSet result = session.execute(prep2.bind());
                assertThat(result.getExecutionInfo().getQueriedHost()).isEqualTo(host1);
                assertThat(result.one().getString("name")).isEqualTo("test2");
                if (!setKeyspace) {
                    session.execute("USE " + keyspace3);
                }
                result = session.execute(prep3.bind());
                assertThat(result.getExecutionInfo().getQueriedHost()).isEqualTo(host1);
                assertThat(result.one().getString("name")).isEqualTo("test3");
            }

            // bring node 1 down
            ccm().stop(1);
            TestUtils.waitForDown(TestUtils.ipOfNode(1), cluster);

            // Execute queries 10 times, should always hit host2 due to node1 being down, should also prepare on host2
            // under covers since wasn't previously prepared.
            for (int i = 0; i < 10; i++) {
                if (!setKeyspace) {
                    session.execute("USE " + keyspace2);
                }
                ResultSet result = session.execute(prep2.bind());
                assertThat(result.getExecutionInfo().getQueriedHost()).isEqualTo(host2);
                assertThat(result.one().getString("name")).isEqualTo("test2");
                if (!setKeyspace) {
                    session.execute("USE " + keyspace3);
                }
                result = session.execute(prep3.bind());
                assertThat(result.getExecutionInfo().getQueriedHost()).isEqualTo(host2);
                assertThat(result.one().getString("name")).isEqualTo("test3");
            }

            ccm().start(1);
            TestUtils.waitForUp(TestUtils.ipOfNode(1), cluster);

            // Execute queries 10 times, should always hit host1 due to sorting policy and node1 being up
            // should have been reprepared either on up or when encountering query was not prepared (although
            // newer versions of C* may auto prepare on up).
            for (int i = 0; i < 10; i++) {
                if (!setKeyspace) {
                    session.execute("USE " + keyspace2);
                }
                ResultSet result = session.execute(prep2.bind());
                assertThat(result.getExecutionInfo().getQueriedHost()).isEqualTo(host1);
                assertThat(result.one().getString("name")).isEqualTo("test2");
                if (!setKeyspace) {
                    session.execute("USE " + keyspace3);
                }
                result = session.execute(prep3.bind());
                assertThat(result.getExecutionInfo().getQueriedHost()).isEqualTo(host1);
                assertThat(result.one().getString("name")).isEqualTo("test3");
            }
        } finally {
            cluster.close();
        }
    }

    @Test(groups = "long")
    public void should_fail_on_unprepared_if_keyspace_is_different() {
        // validates that the driver proactively throws an exception if we get an 'UNPREPARED' response and the
        // associated prepared statements keyspace is different than the one set on the session.
        QueryOptions queryOptions = new QueryOptions().setReprepareOnUp(false).setPrepareOnAllHosts(false);
        Cluster.Builder builder = createClusterBuilderNoDebouncing()
                .withNettyOptions(TestUtils.nonQuietClusterCloseOptions)
                .addContactPointsWithPorts(getContactPointsWithPorts())
                .withPort(ccm().getBinaryPort())
                .withQueryOptions(queryOptions)
                .withLoadBalancingPolicy(new SortingLoadBalancingPolicy());

        // Downgrade C* 4.0 to protocol version 4 as v5 is resilient to this issue.
        if (ccm().getCassandraVersion().compareTo(VersionNumber.parse("4.0.0")) >= 0) {
            builder = builder.withProtocolVersion(ProtocolVersion.V4);
        }

        Cluster cluster = builder.build();
        try {
            Session session = cluster.connect();

            // Execute use keyspace before preparing.  This is an anti-pattern but works nonetheless.
            session.execute("USE " + keyspace2);
            PreparedStatement prep = session.prepare(new SimpleStatement("SELECT * from users"));

            Host host1 = TestUtils.findHost(cluster, 1);

            // Execute queries 10 times, should always hit host1 due to sorting policy.
            for (int i = 0; i < 10; i++) {
                ResultSet result = session.execute(prep.bind());
                assertThat(result.getExecutionInfo().getQueriedHost()).isEqualTo(host1);
                assertThat(result.one().getString("name")).isEqualTo("test2");
            }

            // use keyspace3 to make it the active keyspace
            session.execute("USE " + keyspace3);

            // bring node 1 down
            ccm().stop(1);
            TestUtils.waitForDown(TestUtils.ipOfNode(1), cluster);

            // Execute query, should hit node 2 where the query has not been prepared, because the prepared statement
            // was originally done with keyspace2, executing the query should fail as keyspace3 is now active.
            // under covers since wasn't previously prepared.
            try {
                session.execute(prep.bind());
                fail("Expected DriverInternalError");
            } catch (DriverInternalError die) {
                // IllegalStateException expected indication can't prepare as statement was prepared on a different keyspace.
                assertThat(die.getCause()).isInstanceOf(IllegalStateException.class);
            }
        } finally {
            cluster.close();
        }
    }
}
