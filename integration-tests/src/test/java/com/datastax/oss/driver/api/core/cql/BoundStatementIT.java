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

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.query;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.type.codec.CqlIntToStringCodec;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestName;

@Category(ParallelizableTests.class)
public class BoundStatementIT {

  @ClassRule
  public static SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  @ClassRule public static CcmRule ccm = CcmRule.getInstance();

  @ClassRule
  public static SessionRule<CqlSession> sessionRule =
      new SessionRule<>(ccm, "basic.request.page-size = 20");

  @Rule public TestName name = new TestName();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String KEY = "test";

  private static final int VALUE = 7;

  @BeforeClass
  public static void setupSchema() {
    // table where every column forms the primary key.
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(
                    "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))")
                .withConfigProfile(sessionRule.slowProfile())
                .build());
    for (int i = 0; i < 100; i++) {
      sessionRule
          .session()
          .execute(
              SimpleStatement.builder("INSERT INTO test (k, v) VALUES (?, ?)")
                  .addPositionalValues(KEY, i)
                  .build());
    }

    // table with simple primary key, single cell.
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v0 int)")
                .withConfigProfile(sessionRule.slowProfile())
                .build());
  }

  @Before
  public void clearPrimes() {
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);
  }

  @Test(expected = IllegalStateException.class)
  public void should_not_allow_unset_value_when_protocol_less_than_v4() {
    try (CqlSession v3Session =
        SessionUtils.newSession(ccm, sessionRule.keyspace(), "advanced.protocol.version = V3")) {
      PreparedStatement prepared = v3Session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      BoundStatement boundStatement =
          prepared.boundStatementBuilder().setString(0, name.getMethodName()).unset(1).build();

      v3Session.execute(boundStatement);
    }
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_not_write_tombstone_if_value_is_implicitly_unset() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      session.execute(prepared.bind(name.getMethodName(), VALUE));

      BoundStatement boundStatement =
          prepared.boundStatementBuilder().setString(0, name.getMethodName()).build();

      verifyUnset(session, boundStatement, name.getMethodName());
    }
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_write_tombstone_if_value_is_explicitly_unset() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      session.execute(prepared.bind(name.getMethodName(), VALUE));

      BoundStatement boundStatement =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, VALUE + 1) // set initially, will be unset later
              .build();

      verifyUnset(session, boundStatement.unset(1), name.getMethodName());
    }
  }

  @Test
  @CassandraRequirement(min = "2.2")
  public void should_write_tombstone_if_value_is_explicitly_unset_on_builder() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      session.execute(prepared.bind(name.getMethodName(), VALUE));

      BoundStatement boundStatement =
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, VALUE + 1) // set initially, will be unset later
              .unset(1)
              .build();

      verifyUnset(session, boundStatement, name.getMethodName());
    }
  }

  @Test
  public void should_have_empty_result_definitions_for_update_query() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      assertThat(prepared.getResultSetDefinitions()).hasSize(0);

      ResultSet rs = session.execute(prepared.bind(name.getMethodName(), VALUE));
      assertThat(rs.getColumnDefinitions()).hasSize(0);
    }
  }

  @Test
  public void should_bind_null_value_when_setting_values_in_bulk() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
      BoundStatement boundStatement = prepared.bind(name.getMethodName(), null);
      assertThat(boundStatement.get(1, TypeCodecs.INT)).isNull();
    }
  }

  @Test
  public void should_allow_custom_codecs_when_setting_values_in_bulk() {
    // v0 is an int column, but we'll bind a String to it
    CqlIntToStringCodec codec = new CqlIntToStringCodec();
    try (CqlSession session = sessionWithCustomCodec(codec)) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");
      for (BoundStatement boundStatement :
          ImmutableList.of(
              prepared.bind(name.getMethodName(), "42"),
              prepared.boundStatementBuilder(name.getMethodName(), "42").build())) {

        session.execute(boundStatement);
        ResultSet rs =
            session.execute(
                SimpleStatement.newInstance(
                    "SELECT v0 FROM test2 WHERE k = ?", name.getMethodName()));
        assertThat(rs.one().getInt(0)).isEqualTo(42);
      }
    }
  }

  @Test
  public void should_use_page_size_from_simple_statement() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      SimpleStatement st = SimpleStatement.builder("SELECT v FROM test").withPageSize(10).build();
      PreparedStatement prepared = session.prepare(st);
      ResultSet result = session.execute(prepared.bind());

      // Should have only fetched 10 (page size) rows.
      assertThat(result.getAvailableWithoutFetching()).isEqualTo(10);
    }
  }

  @Test
  public void should_use_page_size() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      // set page size on simple statement, but will be unused since
      // overridden by bound statement.
      SimpleStatement st = SimpleStatement.builder("SELECT v FROM test").withPageSize(10).build();
      PreparedStatement prepared = session.prepare(st);
      ResultSet result = session.execute(prepared.bind().setPageSize(12));

      // Should have only fetched 10 (page size) rows.
      assertThat(result.getAvailableWithoutFetching()).isEqualTo(12);
    }
  }

  @Test
  public void should_use_consistencies_from_simple_statement() {
    try (CqlSession session = SessionUtils.newSession(simulacron)) {
      SimpleStatement st =
          SimpleStatement.builder("SELECT * FROM test where k = ?")
              .withConsistencyLevel(DefaultConsistencyLevel.TWO)
              .withSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
              .build();
      PreparedStatement prepared = session.prepare(st);
      simulacron.cluster().clearLogs();
      // since query is unprimed, we use a text value for bind parameter as this is
      // what simulacron expects for unprimed statements.
      session.execute(prepared.bind("0"));

      List<QueryLog> logs = simulacron.cluster().getLogs().getQueryLogs();
      assertThat(logs).hasSize(1);

      QueryLog log = logs.get(0);

      Message message = log.getFrame().message;
      assertThat(message).isInstanceOf(Execute.class);
      Execute execute = (Execute) message;
      assertThat(execute.options.consistency)
          .isEqualTo(DefaultConsistencyLevel.TWO.getProtocolCode());
      assertThat(execute.options.serialConsistency)
          .isEqualTo(DefaultConsistencyLevel.LOCAL_SERIAL.getProtocolCode());
    }
  }

  @Test
  public void should_use_consistencies() {
    try (CqlSession session = SessionUtils.newSession(simulacron)) {
      // set consistencies on simple statement, but they will be unused since
      // overridden by bound statement.
      SimpleStatement st =
          SimpleStatement.builder("SELECT * FROM test where k = ?")
              .withConsistencyLevel(DefaultConsistencyLevel.TWO)
              .withSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
              .build();
      PreparedStatement prepared = session.prepare(st);
      simulacron.cluster().clearLogs();
      // since query is unprimed, we use a text value for bind parameter as this is
      // what simulacron expects for unprimed statements.
      session.execute(
          prepared
              .boundStatementBuilder("0")
              .withConsistencyLevel(DefaultConsistencyLevel.THREE)
              .withSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
              .build());

      List<QueryLog> logs = simulacron.cluster().getLogs().getQueryLogs();
      assertThat(logs).hasSize(1);

      QueryLog log = logs.get(0);

      Message message = log.getFrame().message;
      assertThat(message).isInstanceOf(Execute.class);
      Execute execute = (Execute) message;
      assertThat(execute.options.consistency)
          .isEqualTo(DefaultConsistencyLevel.THREE.getProtocolCode());
      assertThat(execute.options.serialConsistency)
          .isEqualTo(DefaultConsistencyLevel.SERIAL.getProtocolCode());
    }
  }

  @Test
  public void should_use_timeout_from_simple_statement() {
    try (CqlSession session = SessionUtils.newSession(simulacron)) {
      Map<String, Object> params = ImmutableMap.of("k", 0);
      Map<String, String> paramTypes = ImmutableMap.of("k", "int");
      simulacron
          .cluster()
          .prime(
              when(query(
                      "mock query",
                      Lists.newArrayList(
                          com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE),
                      params,
                      paramTypes))
                  .then(noRows())
                  .delay(1500, TimeUnit.MILLISECONDS));
      SimpleStatement st =
          SimpleStatement.builder("mock query")
              .withTimeout(Duration.ofSeconds(1))
              .withConsistencyLevel(DefaultConsistencyLevel.ONE)
              .build();
      PreparedStatement prepared = session.prepare(st);

      thrown.expect(DriverTimeoutException.class);
      thrown.expectMessage("Query timed out after PT1S");

      session.execute(prepared.bind(0));
    }
  }

  @Test
  public void should_use_timeout() {
    try (CqlSession session = SessionUtils.newSession(simulacron)) {
      Map<String, Object> params = ImmutableMap.of("k", 0);
      Map<String, String> paramTypes = ImmutableMap.of("k", "int");
      // set timeout on simple statement, but will be unused since overridden by bound statement.
      simulacron
          .cluster()
          .prime(
              when(query(
                      "mock query",
                      Lists.newArrayList(
                          com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE),
                      params,
                      paramTypes))
                  .then(noRows())
                  .delay(1500, TimeUnit.MILLISECONDS));
      SimpleStatement st =
          SimpleStatement.builder("mock query")
              .withTimeout(Duration.ofSeconds(1))
              .withConsistencyLevel(DefaultConsistencyLevel.ONE)
              .build();
      PreparedStatement prepared = session.prepare(st);

      thrown.expect(DriverTimeoutException.class);
      thrown.expectMessage("Query timed out after PT0.15S");

      session.execute(prepared.bind(0).setTimeout(Duration.ofMillis(150)));
    }
  }

  private static void verifyUnset(
      CqlSession session, BoundStatement boundStatement, String valueName) {
    session.execute(boundStatement.unset(1));

    // Verify that no tombstone was written by reading data back and ensuring initial value is
    // retained.
    ResultSet result =
        session.execute(
            SimpleStatement.builder("SELECT v0 from test2 where k = ?")
                .addPositionalValue(valueName)
                .build());

    Row row = result.iterator().next();
    assertThat(row.getInt(0)).isEqualTo(VALUE);
  }

  @SuppressWarnings("unchecked")
  private CqlSession sessionWithCustomCodec(CqlIntToStringCodec codec) {
    return (CqlSession)
        SessionUtils.baseBuilder()
            .addContactPoints(ccm.getContactPoints())
            .withKeyspace(sessionRule.keyspace())
            .addTypeCodecs(codec)
            .build();
  }
}
