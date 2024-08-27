/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
import java.util.Iterator;
import java.util.List;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class BatchStatementIT {

  private CcmRule ccmRule = CcmRule.getInstance();

  private SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Rule public TestName name = new TestName();

  private static final int batchCount = 100;

  @Before
  public void createTable() {
    String[] schemaStatements =
        new String[] {
          "CREATE TABLE test (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))",
          "CREATE TABLE counter1 (k0 text PRIMARY KEY, c counter)",
          "CREATE TABLE counter2 (k0 text PRIMARY KEY, c counter)",
          "CREATE TABLE counter3 (k0 text PRIMARY KEY, c counter)",
        };

    for (String schemaStatement : schemaStatements) {
      sessionRule
          .session()
          .execute(
              SimpleStatement.newInstance(schemaStatement)
                  .setExecutionProfile(sessionRule.slowProfile()));
    }
  }

  @Test
  public void should_issue_log_warn_if_batched_statement_have_consistency_level_set() {
    SimpleStatement simpleStatement =
        SimpleStatement.builder("INSERT INTO test (k0, k1, v) values ('123123', ?, ?)").build();

    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      PreparedStatement prep = session.prepare(simpleStatement);
      BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
      batch.addStatement(prep.bind(1, 2).setConsistencyLevel(ConsistencyLevel.QUORUM));

      LoggerTest.LoggerSetup logger =
          LoggerTest.setupTestLogger(DefaultBatchStatement.class, Level.WARN);

      batch.build();

      verify(logger.appender).doAppend(logger.loggingEventCaptor.capture());
      assertThat(
              logger.loggingEventCaptor.getAllValues().stream()
                  .map(ILoggingEvent::getFormattedMessage))
          .contains(
              "You have submitted statement with non-default [serial] consistency level to the DefaultBatchStatement. "
                  + "Be aware that [serial] consistency level of child statements is not preserved by the DefaultBatchStatement. "
                  + "Use DefaultBatchStatement.setConsistencyLevel()/DefaultBatchStatement.setSerialConsistencyLevel() instead.");
    }
  }

  @Test
  public void should_execute_batch_of_simple_statements_with_variables() {
    // Build a batch of batchCount simple statements, each with their own positional variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getMethodName()))
              .addPositionalValues(i, i + 1)
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);

    verifyBatchInsert();
  }

  @Test
  public void should_execute_batch_of_bound_statements_with_variables() {
    // Build a batch of batchCount statements with bound statements, each with their own positional
    // variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getMethodName()))
            .build();
    PreparedStatement preparedStatement = sessionRule.session().prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);

    verifyBatchInsert();
  }

  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "2.2")
  public void should_execute_batch_of_bound_statements_with_unset_values() {
    // Build a batch of batchCount statements with bound statements, each with their own positional
    // variables.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getMethodName()))
            .build();
    PreparedStatement preparedStatement = sessionRule.session().prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);

    verifyBatchInsert();

    BatchStatementBuilder builder2 = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    for (int i = 0; i < batchCount; i++) {
      BoundStatement boundStatement = preparedStatement.bind(i, i + 2);
      // unset v every 20 statements.
      if (i % 20 == 0) {
        boundStatement = boundStatement.unset(1);
      }
      builder.addStatement(boundStatement);
    }

    sessionRule.session().execute(builder2.build());

    Statement<?> select =
        SimpleStatement.builder("SELECT * from test where k0 = ?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = sessionRule.session().execute(select);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(100);

    Iterator<Row> iterator = rows.iterator();
    for (int i = 0; i < batchCount; i++) {
      Row row = iterator.next();
      assertThat(row.getString("k0")).isEqualTo(name.getMethodName());
      assertThat(row.getInt("k1")).isEqualTo(i);
      // value should be from first insert (i + 1) if at row divisble by 20, otherwise second.
      int expectedValue = i % 20 == 0 ? i + 1 : i + 2;
      if (i % 20 == 0) {
        assertThat(row.getInt("v")).isEqualTo(expectedValue);
      }
    }
  }

  @Test
  public void should_execute_batch_of_bound_statements_with_named_variables() {
    // Build a batch of batchCount statements with bound statements, each with their own named
    // variable values.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    PreparedStatement preparedStatement =
        sessionRule.session().prepare("INSERT INTO test (k0, k1, v) values (:k0, :k1, :v)");

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(
          preparedStatement
              .boundStatementBuilder()
              .setString("k0", name.getMethodName())
              .setInt("k1", i)
              .setInt("v", i + 1)
              .build());
    }

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);

    verifyBatchInsert();
  }

  @Test
  public void should_execute_batch_of_bound_and_simple_statements_with_variables() {
    // Build a batch of batchCount statements with simple and bound statements alternating.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?)", name.getMethodName()))
            .build();
    PreparedStatement preparedStatement = sessionRule.session().prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      if (i % 2 == 1) {
        SimpleStatement simpleInsert =
            SimpleStatement.builder(
                    String.format(
                        "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getMethodName()))
                .addPositionalValues(i, i + 1)
                .build();
        builder.addStatement(simpleInsert);
      } else {
        builder.addStatement(preparedStatement.bind(i, i + 1));
      }
    }

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);

    verifyBatchInsert();
  }

  @Test
  public void should_execute_cas_batch() {
    // Build a batch with CAS operations on the same partition.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.UNLOGGED);
    SimpleStatement insert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ? , ?) IF NOT EXISTS",
                    name.getMethodName()))
            .build();
    PreparedStatement preparedStatement = sessionRule.session().prepare(insert);

    for (int i = 0; i < batchCount; i++) {
      builder.addStatement(preparedStatement.bind(i, i + 1));
    }

    BatchStatement batchStatement = builder.build();
    ResultSet result = sessionRule.session().execute(batchStatement);
    assertThat(result.wasApplied()).isTrue();

    verifyBatchInsert();

    // re execute same batch and ensure wasn't applied.
    result = sessionRule.session().execute(batchStatement);
    assertThat(result.wasApplied()).isFalse();
  }

  @Test
  public void should_execute_counter_batch() {
    // should be able to do counter increments in a counter batch.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.COUNTER);

    for (int i = 1; i <= 3; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "UPDATE counter%d set c = c + %d where k0 = '%s'",
                      i, i, name.getMethodName()))
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);

    for (int i = 1; i <= 3; i++) {
      ResultSet result =
          sessionRule
              .session()
              .execute(
                  String.format(
                      "SELECT c from counter%d where k0 = '%s'", i, name.getMethodName()));

      List<Row> rows = result.all();
      assertThat(rows).hasSize(1);

      Row row = rows.iterator().next();
      assertThat(row.getLong("c")).isEqualTo(i);
    }
  }

  @Test(expected = InvalidQueryException.class)
  public void should_fail_logged_batch_with_counter_increment() {
    // should not be able to do counter inserts in a unlogged batch.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.LOGGED);

    for (int i = 1; i <= 3; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "UPDATE counter%d set c = c + %d where k0 = '%s'",
                      i, i, name.getMethodName()))
              .build();
      builder.addStatement(insert);
    }

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);
  }

  @Test(expected = InvalidQueryException.class)
  public void should_fail_counter_batch_with_non_counter_increment() {
    // should not be able to do a counter batch if it contains a non-counter increment statement.
    BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.COUNTER);

    for (int i = 1; i <= 3; i++) {
      SimpleStatement insert =
          SimpleStatement.builder(
                  String.format(
                      "UPDATE counter%d set c = c + %d where k0 = '%s'",
                      i, i, name.getMethodName()))
              .build();
      builder.addStatement(insert);
    }
    // add a non-counter increment statement.
    SimpleStatement simpleInsert =
        SimpleStatement.builder(
                String.format(
                    "INSERT INTO test (k0, k1, v) values ('%s', ?, ?)", name.getMethodName()))
            .addPositionalValues(1, 2)
            .build();
    builder.addStatement(simpleInsert);

    BatchStatement batchStatement = builder.build();
    sessionRule.session().execute(batchStatement);
  }

  @Test
  @Ignore("@IntegrationTestDisabledCassandra4Failure")
  public void should_not_allow_unset_value_when_protocol_less_than_v4() {
    //    CREATE TABLE test (k0 text, k1 int, v int, PRIMARY KEY (k0, k1))
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V3")
            .build();
    try (CqlSession v3Session = SessionUtils.newSession(ccmRule, loader)) {
      // Intentionally use fully qualified table here to avoid warnings as these are not supported
      // by v3 protocol version, see JAVA-3068
      PreparedStatement prepared =
          v3Session.prepare(
              String.format(
                  "INSERT INTO %s.test (k0, k1, v) values (?, ?, ?)", sessionRule.keyspace()));

      BatchStatementBuilder builder = BatchStatement.builder(DefaultBatchType.LOGGED);
      builder.addStatements(
          // All set => OK
          prepared.bind(name.getMethodName(), 1, 1),
          // One variable unset => should fail
          prepared
              .boundStatementBuilder()
              .setString(0, name.getMethodName())
              .setInt(1, 2)
              .unset(2)
              .build());

      assertThatThrownBy(() -> v3Session.execute(builder.build()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Unset value at index");
    }
  }

  private void verifyBatchInsert() {
    // validate data inserted by the batch.
    Statement<?> select =
        SimpleStatement.builder("SELECT * from test where k0 = ?")
            .addPositionalValue(name.getMethodName())
            .build();

    ResultSet result = sessionRule.session().execute(select);

    List<Row> rows = result.all();
    assertThat(rows).hasSize(100);

    Iterator<Row> iterator = rows.iterator();
    for (int i = 0; i < batchCount; i++) {
      Row row = iterator.next();
      assertThat(row.getString("k0")).isEqualTo(name.getMethodName());
      assertThat(row.getInt("k1")).isEqualTo(i);
      assertThat(row.getInt("v")).isEqualTo(i + 1);
    }
  }
}
