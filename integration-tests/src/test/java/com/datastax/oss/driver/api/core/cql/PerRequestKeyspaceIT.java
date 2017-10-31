/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Note: at the time of writing, this test exercises features of an unreleased Cassandra version. To
 * test against a local build, run with
 *
 * <pre>
 *   -Dccm.cassandraVersion=4.0.0 -Dccm.cassandraDirectory=/path/to/cassandra -Ddatastax-java-driver.protocol.version=V5
 * </pre>
 */
@Category(ParallelizableTests.class)
public class PerRequestKeyspaceIT {

  @Rule public CcmRule ccmRule = CcmRule.getInstance();

  @Rule public ClusterRule clusterRule = ClusterRule.builder(ccmRule).withKeyspace(true).build();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TestName nameRule = new TestName();

  @Before
  public void setupSchema() {
    CqlSession session = clusterRule.session();
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE IF NOT EXISTS foo (k text, cc int, v int, PRIMARY KEY(k, cc))")
            .withConfigProfile(clusterRule.slowProfile())
            .build());
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_reject_simple_statement_with_keyspace_in_protocol_v4() {
    should_reject_statement_with_keyspace_in_protocol_v4(
        SimpleStatement.newInstance("SELECT * FROM foo").setKeyspace(clusterRule.keyspace()));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_reject_batch_statement_with_explicit_keyspace_in_protocol_v4() {
    SimpleStatement statementWithoutKeyspace =
        SimpleStatement.newInstance(
            "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 1, 1);
    should_reject_statement_with_keyspace_in_protocol_v4(
        BatchStatement.builder(BatchType.LOGGED)
            .withKeyspace(clusterRule.keyspace())
            .addStatement(statementWithoutKeyspace)
            .build());
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_reject_batch_statement_with_inferred_keyspace_in_protocol_v4() {
    SimpleStatement statementWithKeyspace =
        SimpleStatement.newInstance(
                "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 1, 1)
            .setKeyspace(clusterRule.keyspace());
    should_reject_statement_with_keyspace_in_protocol_v4(
        BatchStatement.builder(BatchType.LOGGED).addStatement(statementWithKeyspace).build());
  }

  private void should_reject_statement_with_keyspace_in_protocol_v4(Statement statement) {
    try (Cluster<CqlSession> cluster = ClusterUtils.newCluster(ccmRule, "protocol.version = V4")) {
      thrown.expect(IllegalArgumentException.class);
      thrown.expectMessage("Can't use per-request keyspace with protocol V4");
      cluster.connect().execute(statement);
    }
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_execute_simple_statement_with_keyspace() {
    CqlSession session = clusterRule.session();
    session.execute(
        SimpleStatement.newInstance(
                "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 1, 1)
            .setKeyspace(clusterRule.keyspace()));
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                        "SELECT v FROM foo WHERE k = ? AND cc = 1", nameRule.getMethodName())
                    .setKeyspace(clusterRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_execute_batch_with_explicit_keyspace() {
    CqlSession session = clusterRule.session();
    session.execute(
        BatchStatement.builder(BatchType.LOGGED)
            .withKeyspace(clusterRule.keyspace())
            .addStatements(
                SimpleStatement.newInstance(
                    "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 1, 1),
                SimpleStatement.newInstance(
                    "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 2, 2))
            .build());

    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                        "SELECT v FROM foo WHERE k = ? AND cc = 1", nameRule.getMethodName())
                    .setKeyspace(clusterRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_execute_batch_with_inferred_keyspace() {
    CqlSession session = clusterRule.session();
    session.execute(
        BatchStatement.builder(BatchType.LOGGED)
            .withKeyspace(clusterRule.keyspace())
            .addStatements(
                SimpleStatement.newInstance(
                        "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)",
                        nameRule.getMethodName(),
                        1,
                        1)
                    .setKeyspace(clusterRule.keyspace()),
                SimpleStatement.newInstance(
                        "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)",
                        nameRule.getMethodName(),
                        2,
                        2)
                    .setKeyspace(clusterRule.keyspace()))
            .build());

    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                        "SELECT v FROM foo WHERE k = ? AND cc = 1", nameRule.getMethodName())
                    .setKeyspace(clusterRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_prepare_statement_with_keyspace() {
    CqlSession session = clusterRule.session();
    PreparedStatement prepared =
        session.prepare(
            SimpleStatement.newInstance("INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)")
                .setKeyspace(clusterRule.keyspace()));
    session.execute(prepared.bind(nameRule.getMethodName(), 1, 1));

    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                        "SELECT v FROM foo WHERE k = ? AND cc = 1", nameRule.getMethodName())
                    .setKeyspace(clusterRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }
}
