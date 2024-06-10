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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.codahale.metrics.Gauge;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.testinfra.CassandraSkip;
import com.datastax.oss.driver.api.testinfra.ScyllaRequirement;
import com.datastax.oss.driver.api.testinfra.ScyllaSkip;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import junit.framework.TestCase;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Note: at the time of writing, some of these tests exercises features of an unreleased Cassandra
 * version. To test against a local build, run with
 *
 * <pre>
 *   -Dccm.version=4.0.0 -Dccm.directory=/path/to/cassandra -Ddatastax-java-driver.advanced.protocol.version=V5
 * </pre>
 */
@Category(ParallelizableTests.class)
public class PreparedStatementIT {

  private CcmRule ccmRule = CcmRule.getInstance();

  private SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 2)
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Before
  public void setupSchema() {
    for (String query :
        ImmutableList.of(
            "DROP TABLE IF EXISTS prepared_statement_test",
            "CREATE TABLE prepared_statement_test (a int PRIMARY KEY, b int, c int)",
            "INSERT INTO prepared_statement_test (a, b, c) VALUES (1, 1, 1)",
            "INSERT INTO prepared_statement_test (a, b, c) VALUES (2, 2, 2)",
            "INSERT INTO prepared_statement_test (a, b, c) VALUES (3, 3, 3)",
            "INSERT INTO prepared_statement_test (a, b, c) VALUES (4, 4, 4)")) {
      executeDdl(query);
    }
  }

  private void executeDdl(String query) {
    sessionRule
        .session()
        .execute(
            SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
  }

  @Test
  public void should_have_empty_result_definitions_for_insert_query_without_bound_variable() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      PreparedStatement prepared =
          session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (1, 1, 1)");
      assertThat(prepared.getVariableDefinitions()).isEmpty();
      assertThat(prepared.getPartitionKeyIndices()).isEmpty();
      assertThat(prepared.getResultSetDefinitions()).isEmpty();
    }
  }

  @Test
  public void should_have_non_empty_result_definitions_for_insert_query_with_bound_variable() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      PreparedStatement prepared =
          session.prepare("INSERT INTO prepared_statement_test (a, b, c) VALUES (?, ?, ?)");
      assertThat(prepared.getVariableDefinitions()).hasSize(3);
      assertThat(prepared.getPartitionKeyIndices()).hasSize(1);
      assertThat(prepared.getResultSetDefinitions()).isEmpty();
    }
  }

  @Test
  public void should_have_empty_variable_definitions_for_select_query_without_bound_variable() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      PreparedStatement prepared =
          session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = 1");
      assertThat(prepared.getVariableDefinitions()).isEmpty();
      assertThat(prepared.getPartitionKeyIndices()).isEmpty();
      assertThat(prepared.getResultSetDefinitions()).hasSize(3);
    }
  }

  @Test
  public void should_have_non_empty_variable_definitions_for_select_query_with_bound_variable() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      PreparedStatement prepared =
          session.prepare("SELECT a,b,c FROM prepared_statement_test WHERE a = ?");
      assertThat(prepared.getVariableDefinitions()).hasSize(1);
      assertThat(prepared.getPartitionKeyIndices()).hasSize(1);
      assertThat(prepared.getResultSetDefinitions()).hasSize(3);
    }
  }

  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0")
  @ScyllaSkip(description = "@IntegrationTestDisabledScyllaFailure")
  public void should_update_metadata_when_schema_changed_across_executions() {
    // Given
    CqlSession session = sessionRule.session();
    PreparedStatement ps = session.prepare("SELECT * FROM prepared_statement_test WHERE a = ?");
    ByteBuffer idBefore = ps.getResultMetadataId();

    // When
    session.execute(
        SimpleStatement.builder("ALTER TABLE prepared_statement_test ADD d int")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    BoundStatement bs = ps.bind(1);
    ResultSet rows = session.execute(bs);

    // Then
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
    for (ColumnDefinitions columnDefinitions :
        ImmutableList.of(
            ps.getResultSetDefinitions(),
            bs.getPreparedStatement().getResultSetDefinitions(),
            rows.getColumnDefinitions())) {
      assertThat(columnDefinitions).hasSize(4);
      assertThat(columnDefinitions.get("d").getType()).isEqualTo(DataTypes.INT);
    }
  }

  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0")
  @ScyllaSkip(description = "@IntegrationTestDisabledScyllaFailure")
  public void should_update_metadata_when_schema_changed_across_pages() {
    // Given
    CqlSession session = sessionRule.session();
    PreparedStatement ps = session.prepare("SELECT * FROM prepared_statement_test");
    ByteBuffer idBefore = ps.getResultMetadataId();
    assertThat(ps.getResultSetDefinitions()).hasSize(3);

    CompletionStage<AsyncResultSet> future = session.executeAsync(ps.bind());
    AsyncResultSet rows = CompletableFutures.getUninterruptibly(future);
    assertThat(rows.getColumnDefinitions()).hasSize(3);
    assertThat(rows.getColumnDefinitions().contains("d")).isFalse();
    // Consume the first page
    for (Row row : rows.currentPage()) {
      try {
        row.getInt("d");
        TestCase.fail("expected an error");
      } catch (IllegalArgumentException e) {
        /*expected*/
      }
    }

    // When
    session.execute(
        SimpleStatement.builder("ALTER TABLE prepared_statement_test ADD d int")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    // Then
    // this should trigger a background fetch of the second page, and therefore update the
    // definitions
    rows = CompletableFutures.getUninterruptibly(rows.fetchNextPage());
    for (Row row : rows.currentPage()) {
      assertThat(row.isNull("d")).isTrue();
    }
    assertThat(rows.getColumnDefinitions()).hasSize(4);
    assertThat(rows.getColumnDefinitions().get("d").getType()).isEqualTo(DataTypes.INT);
    // Should have updated the prepared statement too
    ByteBuffer idAfter = ps.getResultMetadataId();
    assertThat(Bytes.toHexString(idAfter)).isNotEqualTo(Bytes.toHexString(idBefore));
    assertThat(ps.getResultSetDefinitions()).hasSize(4);
    assertThat(ps.getResultSetDefinitions().get("d").getType()).isEqualTo(DataTypes.INT);
  }

  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0")
  @ScyllaSkip(description = "@IntegrationTestDisabledScyllaFailure")
  public void should_update_metadata_when_schema_changed_across_sessions() {
    // Given
    CqlSession session1 = sessionRule.session();
    CqlSession session2 = SessionUtils.newSession(ccmRule, sessionRule.keyspace());

    PreparedStatement ps1 = session1.prepare("SELECT * FROM prepared_statement_test WHERE a = ?");
    PreparedStatement ps2 = session2.prepare("SELECT * FROM prepared_statement_test WHERE a = ?");

    ByteBuffer id1a = ps1.getResultMetadataId();
    ByteBuffer id2a = ps2.getResultMetadataId();

    ResultSet rows1 = session1.execute(ps1.bind(1));
    ResultSet rows2 = session2.execute(ps2.bind(1));

    assertThat(rows1.getColumnDefinitions()).hasSize(3);
    assertThat(rows1.getColumnDefinitions().contains("d")).isFalse();
    assertThat(rows2.getColumnDefinitions()).hasSize(3);
    assertThat(rows2.getColumnDefinitions().contains("d")).isFalse();

    // When
    session1.execute("ALTER TABLE prepared_statement_test ADD d int");

    rows1 = session1.execute(ps1.bind(1));
    rows2 = session2.execute(ps2.bind(1));

    ByteBuffer id1b = ps1.getResultMetadataId();
    ByteBuffer id2b = ps2.getResultMetadataId();

    // Then
    assertThat(Bytes.toHexString(id1b)).isNotEqualTo(Bytes.toHexString(id1a));
    assertThat(Bytes.toHexString(id2b)).isNotEqualTo(Bytes.toHexString(id2a));

    assertThat(ps1.getResultSetDefinitions()).hasSize(4);
    assertThat(ps1.getResultSetDefinitions().contains("d")).isTrue();
    assertThat(ps2.getResultSetDefinitions()).hasSize(4);
    assertThat(ps2.getResultSetDefinitions().contains("d")).isTrue();

    assertThat(rows1.getColumnDefinitions()).hasSize(4);
    assertThat(rows1.getColumnDefinitions().contains("d")).isTrue();
    assertThat(rows2.getColumnDefinitions()).hasSize(4);
    assertThat(rows2.getColumnDefinitions().contains("d")).isTrue();

    session2.close();
  }

  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0")
  @ScyllaSkip(
      description =
          "@IntegrationTestDisabledScyllaFailure @IntegrationTestDisabledScyllaDifferentText")
  public void should_fail_to_reprepare_if_query_becomes_invalid() {
    // Given
    CqlSession session = sessionRule.session();
    session.execute("ALTER TABLE prepared_statement_test ADD d int");
    PreparedStatement ps =
        session.prepare("SELECT a, b, c, d FROM prepared_statement_test WHERE a = ?");
    session.execute("ALTER TABLE prepared_statement_test DROP d");

    // When
    Throwable t = catchThrowable(() -> session.execute(ps.bind()));

    // Then
    assertThat(t)
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("Undefined column name d");
  }

  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0")
  @ScyllaSkip(description = "@IntegrationTestDisabledScyllaFailure")
  public void should_not_store_metadata_for_conditional_updates() {
    should_not_store_metadata_for_conditional_updates(sessionRule.session());
  }

  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "2.2")
  @ScyllaSkip(description = "@IntegrationTestDisabledScyllaFailure")
  public void should_not_store_metadata_for_conditional_updates_in_legacy_protocol() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .build();
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace(), loader)) {
      should_not_store_metadata_for_conditional_updates(session);
    }
  }

  @ScyllaSkip(description = "@IntegrationTestDisabledScyllaFailure")
  private void should_not_store_metadata_for_conditional_updates(CqlSession session) {
    // Given
    PreparedStatement ps =
        session.prepare(
            "INSERT INTO prepared_statement_test (a, b, c) VALUES (?, ?, ?) IF NOT EXISTS");

    // Never store metadata in the prepared statement for conditional updates, since the result set
    // can change
    // depending on the outcome.
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    ByteBuffer idBefore = ps.getResultMetadataId();

    // When
    ResultSet rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Successful conditional update => only contains the [applied] column
    assertThat(rs.wasApplied()).isTrue();
    assertThat(rs.getColumnDefinitions()).hasSize(1);
    assertThat(rs.getColumnDefinitions().get("[applied]").getType()).isEqualTo(DataTypes.BOOLEAN);
    // However the prepared statement shouldn't have changed
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));

    // When
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(4);
    Row row = rs.one();
    assertThat(row.getBoolean("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    // The prepared statement still shouldn't have changed
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));

    // When
    session.execute("ALTER TABLE prepared_statement_test ADD d int");
    rs = session.execute(ps.bind(5, 5, 5));

    // Then
    // Failed conditional update => regular metadata that should also contain the new column
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.getColumnDefinitions()).hasSize(5);
    row = rs.one();
    assertThat(row.getBoolean("[applied]")).isFalse();
    assertThat(row.getInt("a")).isEqualTo(5);
    assertThat(row.getInt("b")).isEqualTo(5);
    assertThat(row.getInt("c")).isEqualTo(5);
    assertThat(row.isNull("d")).isTrue();
    assertThat(ps.getResultSetDefinitions()).hasSize(0);
    assertThat(Bytes.toHexString(ps.getResultMetadataId())).isEqualTo(Bytes.toHexString(idBefore));
  }

  @Test
  public void should_return_same_instance_when_repreparing_query() {
    try (CqlSession session = sessionWithCacheSizeMetric()) {
      // Given
      assertThat(getPreparedCacheSize(session)).isEqualTo(0);
      String query = "SELECT * FROM prepared_statement_test WHERE a = ?";

      // Send prepare requests, keep hold of CompletionStage objects to prevent them being removed
      // from CqlPrepareAsyncProcessor#cache, see JAVA-3062
      CompletionStage<PreparedStatement> preparedStatement1Future = session.prepareAsync(query);
      CompletionStage<PreparedStatement> preparedStatement2Future = session.prepareAsync(query);

      PreparedStatement preparedStatement1 =
          CompletableFutures.getUninterruptibly(preparedStatement1Future);
      PreparedStatement preparedStatement2 =
          CompletableFutures.getUninterruptibly(preparedStatement2Future);

      // Then
      assertThat(preparedStatement1).isSameAs(preparedStatement2);
      assertThat(getPreparedCacheSize(session)).isEqualTo(1);
    }
  }

  /** Just to illustrate that the driver does not sanitize query strings. */
  @Test
  public void should_create_separate_instances_for_differently_formatted_queries() {
    try (CqlSession session = sessionWithCacheSizeMetric()) {
      // Given
      assertThat(getPreparedCacheSize(session)).isEqualTo(0);

      // Send prepare requests, keep hold of CompletionStage objects to prevent them being removed
      // from CqlPrepareAsyncProcessor#cache, see JAVA-3062
      CompletionStage<PreparedStatement> preparedStatement1Future =
          session.prepareAsync("SELECT * FROM prepared_statement_test WHERE a = ?");
      CompletionStage<PreparedStatement> preparedStatement2Future =
          session.prepareAsync("select * from prepared_statement_test where a = ?");

      PreparedStatement preparedStatement1 =
          CompletableFutures.getUninterruptibly(preparedStatement1Future);
      PreparedStatement preparedStatement2 =
          CompletableFutures.getUninterruptibly(preparedStatement2Future);

      // Then
      assertThat(preparedStatement1).isNotSameAs(preparedStatement2);
      assertThat(getPreparedCacheSize(session)).isEqualTo(2);
    }
  }

  @Test
  public void should_create_separate_instances_for_different_statement_parameters() {
    try (CqlSession session = sessionWithCacheSizeMetric()) {
      // Given
      assertThat(getPreparedCacheSize(session)).isEqualTo(0);
      SimpleStatement statement =
          SimpleStatement.newInstance("SELECT * FROM prepared_statement_test");

      // Send prepare requests, keep hold of CompletionStage objects to prevent them being removed
      // from CqlPrepareAsyncProcessor#cache, see JAVA-3062
      CompletionStage<PreparedStatement> preparedStatement1Future =
          session.prepareAsync(statement.setPageSize(1));
      CompletionStage<PreparedStatement> preparedStatement2Future =
          session.prepareAsync(statement.setPageSize(4));

      PreparedStatement preparedStatement1 =
          CompletableFutures.getUninterruptibly(preparedStatement1Future);
      PreparedStatement preparedStatement2 =
          CompletableFutures.getUninterruptibly(preparedStatement2Future);

      // Then
      assertThat(preparedStatement1).isNotSameAs(preparedStatement2);
      assertThat(getPreparedCacheSize(session)).isEqualTo(2);
      // Each bound statement uses the page size it was prepared with
      assertThat(firstPageOf(session.executeAsync(preparedStatement1.bind()))).hasSize(1);
      assertThat(firstPageOf(session.executeAsync(preparedStatement2.bind()))).hasSize(4);
    }
  }

  /**
   * This method reproduces CASSANDRA-15252 which is fixed in 3.0.26/3.11.12/4.0.2.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-15252">CASSANDRA-15252</a>
   */
  private AbstractThrowableAssert<?, ? extends Throwable> assertableReprepareAfterIdChange() {
    try (CqlSession session = SessionUtils.newSession(ccmRule)) {
      PreparedStatement preparedStatement =
          session.prepare(
              String.format(
                  "SELECT * FROM %s.prepared_statement_test WHERE a = ?", sessionRule.keyspace()));

      session.execute("USE " + sessionRule.keyspace().asCql(false));

      // Drop and recreate the table to invalidate the prepared statement server-side
      executeDdl("DROP TABLE prepared_statement_test");
      executeDdl("CREATE TABLE prepared_statement_test (a int PRIMARY KEY, b int, c int)");

      return assertThatCode(() -> session.execute(preparedStatement.bind(1)));
    }
  }

  // Add version bounds to the DSE requirement if there is a version containing fix for
  // CASSANDRA-15252
  @BackendRequirement(
      type = BackendType.DSE,
      description = "No DSE version contains fix for CASSANDRA-15252")
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "3.0.0", maxExclusive = "3.0.26")
  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "3.11.0",
      maxExclusive = "3.11.12")
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0.0", maxExclusive = "4.0.2")
  @Test
  @Ignore("@IntegrationTestDisabledCassandra3Failure")
  public void should_fail_fast_if_id_changes_on_reprepare() {
    assertableReprepareAfterIdChange()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ID mismatch while trying to reprepare");
  }

  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "3.0.26",
      maxExclusive = "3.11.0")
  @BackendRequirement(
      type = BackendType.CASSANDRA,
      minInclusive = "3.11.12",
      maxExclusive = "4.0.0")
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "4.0.2")
  @Test
  public void handle_id_changes_on_reprepare() {
    assertableReprepareAfterIdChange().doesNotThrowAnyException();
  }

  @Test
  public void should_infer_routing_information_when_partition_key_is_bound() {
    should_infer_routing_information_when_partition_key_is_bound(
        "SELECT a FROM prepared_statement_test WHERE a = ?");
    should_infer_routing_information_when_partition_key_is_bound(
        "INSERT INTO prepared_statement_test (a) VALUES (?)");
    should_infer_routing_information_when_partition_key_is_bound(
        "UPDATE prepared_statement_test SET b = 1 WHERE a = ?");
    should_infer_routing_information_when_partition_key_is_bound(
        "DELETE FROM prepared_statement_test WHERE a = ?");
  }

  @Test
  @CassandraSkip // Functionality only available in Scylla
  @ScyllaRequirement(
      minEnterprise = "2021.0.0",
      minOSS = "4.3.rc0",
      description = "Requires LWT_ADD_METADATA_MARK extension")
  public void scylla_should_recognize_prepared_lwt_query() {
    CqlSession session = sessionRule.session();
    PreparedStatement statementNonLWT =
        session.prepare("UPDATE prepared_statement_test SET b = 3 WHERE a = 1");
    PreparedStatement statementLWT =
        session.prepare("UPDATE prepared_statement_test SET b = 3 WHERE a = 1 IF b = 5");

    assertThat(statementNonLWT.isLWT()).isFalse();
    assertThat(statementLWT.isLWT()).isTrue();
  }

  @Test
  @ScyllaSkip // Scylla behaves differently - see `scylla_should_recognize_prepared_lwt_query` test
  // This test is just to check that no crashes or other weird behaviour occur when this feature is
  // not supported.
  public void cassandra_should_not_recognize_prepared_lwt_query() {
    CqlSession session = sessionRule.session();
    PreparedStatement statementNonLWT =
        session.prepare("UPDATE prepared_statement_test SET b = 3 WHERE a = 1");
    PreparedStatement statementLWT =
        session.prepare("UPDATE prepared_statement_test SET b = 3 WHERE a = 1 IF b = 5");

    assertThat(statementNonLWT.isLWT()).isFalse();
    assertThat(statementLWT.isLWT()).isFalse();
  }

  private void should_infer_routing_information_when_partition_key_is_bound(String queryString) {
    CqlSession session = sessionRule.session();
    TokenFactory tokenFactory =
        ((DefaultTokenMap) session.getMetadata().getTokenMap().orElseThrow(AssertionError::new))
            .getTokenFactory();

    // We'll bind a=1 in the query, check what token this is supposed to produce
    Token expectedToken =
        session
            .execute("SELECT token(a) FROM prepared_statement_test WHERE a = 1")
            .one()
            .getToken(0);

    BoundStatement boundStatement = session.prepare(queryString).bind().setInt("a", 1);

    assertThat(boundStatement.getRoutingKeyspace()).isEqualTo(sessionRule.keyspace());
    assertThat(tokenFactory.hash(boundStatement.getRoutingKey())).isEqualTo(expectedToken);
  }

  private static Iterable<Row> firstPageOf(CompletionStage<AsyncResultSet> stage) {
    return CompletableFutures.getUninterruptibly(stage).currentPage();
  }

  private CqlSession sessionWithCacheSizeMetric() {
    return SessionUtils.newSession(
        ccmRule,
        sessionRule.keyspace(),
        SessionUtils.configLoaderBuilder()
            .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 2)
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
            .withStringList(
                DefaultDriverOption.METRICS_SESSION_ENABLED,
                ImmutableList.of(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath()))
            .build());
  }

  @SuppressWarnings("unchecked")
  private static long getPreparedCacheSize(CqlSession session) {
    return session
        .getMetrics()
        .flatMap(metrics -> metrics.getSessionMetric(DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE))
        .map(metric -> ((Gauge<Long>) metric).getValue())
        .orElseThrow(
            () ->
                new AssertionError(
                    "Could not access metric "
                        + DefaultSessionMetric.CQL_PREPARED_CACHE_SIZE.getPath()));
  }
}
