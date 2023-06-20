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
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

/**
 * Note: at the time of writing, this test exercises features of an unreleased Cassandra version. To
 * test against a local build, run with
 *
 * <pre>
 *   -Dccm.version=4.0.0 -Dccm.directory=/path/to/cassandra -Ddatastax-java-driver.advanced.protocol.version=V5
 * </pre>
 */
@Category(ParallelizableTests.class)
public class PerRequestKeyspaceIT {

  private CcmRule ccmRule = CcmRule.getInstance();

  private SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Rule public TestName nameRule = new TestName();

  @Before
  public void setupSchema() {
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS foo (k text, cc int, v int, PRIMARY KEY(k, cc))")
                .setExecutionProfile(sessionRule.slowProfile())
                .build());
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_reject_simple_statement_with_keyspace_in_protocol_v4() {
    should_reject_statement_with_keyspace_in_protocol_v4(
        SimpleStatement.newInstance("SELECT * FROM foo").setKeyspace(sessionRule.keyspace()));
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_reject_batch_statement_with_explicit_keyspace_in_protocol_v4() {
    SimpleStatement statementWithoutKeyspace =
        SimpleStatement.newInstance(
            "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 1, 1);
    should_reject_statement_with_keyspace_in_protocol_v4(
        BatchStatement.builder(DefaultBatchType.LOGGED)
            .setKeyspace(sessionRule.keyspace())
            .addStatement(statementWithoutKeyspace)
            .build());
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_reject_batch_statement_with_inferred_keyspace_in_protocol_v4() {
    SimpleStatement statementWithKeyspace =
        SimpleStatement.newInstance(
                "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 1, 1)
            .setKeyspace(sessionRule.keyspace());
    should_reject_statement_with_keyspace_in_protocol_v4(
        BatchStatement.builder(DefaultBatchType.LOGGED)
            .addStatement(statementWithKeyspace)
            .build());
  }

  private void should_reject_statement_with_keyspace_in_protocol_v4(Statement statement) {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, loader)) {
      Throwable t = catchThrowable(() -> session.execute(statement));
      assertThat(t)
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage("Can't use per-request keyspace with protocol V4");
    }
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_execute_simple_statement_with_keyspace() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.newInstance(
                "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)", nameRule.getMethodName(), 1, 1)
            .setKeyspace(sessionRule.keyspace()));
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                        "SELECT v FROM foo WHERE k = ? AND cc = 1", nameRule.getMethodName())
                    .setKeyspace(sessionRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_execute_batch_with_explicit_keyspace() {
    CqlSession session = sessionRule.session();
    session.execute(
        BatchStatement.builder(DefaultBatchType.LOGGED)
            .setKeyspace(sessionRule.keyspace())
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
                    .setKeyspace(sessionRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_execute_batch_with_inferred_keyspace() {
    CqlSession session = sessionRule.session();
    session.execute(
        BatchStatement.builder(DefaultBatchType.LOGGED)
            .setKeyspace(sessionRule.keyspace())
            .addStatements(
                SimpleStatement.newInstance(
                        "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)",
                        nameRule.getMethodName(),
                        1,
                        1)
                    .setKeyspace(sessionRule.keyspace()),
                SimpleStatement.newInstance(
                        "INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)",
                        nameRule.getMethodName(),
                        2,
                        2)
                    .setKeyspace(sessionRule.keyspace()))
            .build());

    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                        "SELECT v FROM foo WHERE k = ? AND cc = 1", nameRule.getMethodName())
                    .setKeyspace(sessionRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_prepare_statement_with_keyspace() {
    CqlSession session = sessionRule.session();
    PreparedStatement prepared =
        session.prepare(
            SimpleStatement.newInstance("INSERT INTO foo (k, cc, v) VALUES (?, ?, ?)")
                .setKeyspace(sessionRule.keyspace()));
    session.execute(prepared.bind(nameRule.getMethodName(), 1, 1));

    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                        "SELECT v FROM foo WHERE k = ? AND cc = 1", nameRule.getMethodName())
                    .setKeyspace(sessionRule.keyspace()))
            .one();
    assertThat(row.getInt(0)).isEqualTo(1);
  }

  @Test
  @CassandraRequirement(min = "4.0")
  public void should_reprepare_statement_with_keyspace_on_the_fly() {
    // Create a separate session because we don't want it to have a default keyspace
    try (CqlSession session = SessionUtils.newSession(ccmRule)) {
      executeDdl(
          session,
          String.format(
              "CREATE TABLE IF NOT EXISTS %s.bar (k int primary key)", sessionRule.keyspace()));
      PreparedStatement pst =
          session.prepare(
              SimpleStatement.newInstance("SELECT * FROM bar WHERE k=?")
                  .setKeyspace(sessionRule.keyspace()));

      // Drop and re-create the table to invalidate the prepared statement server side
      executeDdl(session, String.format("DROP TABLE %s.bar", sessionRule.keyspace()));
      executeDdl(
          session,
          String.format("CREATE TABLE %s.bar (k int primary key)", sessionRule.keyspace()));
      assertThat(preparedStatementExistsOnServer(session, pst.getId())).isFalse();

      // This will re-prepare on the fly
      session.execute(pst.bind(0));
      assertThat(preparedStatementExistsOnServer(session, pst.getId())).isTrue();
    }
  }

  private void executeDdl(CqlSession session, String query) {
    session.execute(SimpleStatement.builder(query).setTimeout(Duration.ofSeconds(30)).build());
  }

  private boolean preparedStatementExistsOnServer(CqlSession session, ByteBuffer id) {
    ResultSet resultSet =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT * FROM system.prepared_statements WHERE prepared_id = ?", id));
    return resultSet.iterator().hasNext();
  }
}
