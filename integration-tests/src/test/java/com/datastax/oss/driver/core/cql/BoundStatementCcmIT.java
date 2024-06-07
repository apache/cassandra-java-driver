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
package com.datastax.oss.driver.core.cql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.ccm.SchemaChangeSynchronizer;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.core.type.codec.CqlIntToStringCodec;
import com.datastax.oss.driver.internal.core.DefaultProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.RoutingKey;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class BoundStatementCcmIT {

  private CcmRule ccmRule = CcmRule.getInstance();

  private final boolean atLeastV4 = ccmRule.getHighestProtocolVersion().getCode() >= 4;

  private SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 20)
                  .build())
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @Rule public TestName name = new TestName();

  private static final String KEY = "test";

  private static final int VALUE = 7;

  @Before
  public void setupSchema() {
    // table where every column forms the primary key.
    SchemaChangeSynchronizer.withLock(
        () -> {
          sessionRule
              .session()
              .execute(
                  SimpleStatement.builder(
                          "CREATE TABLE IF NOT EXISTS test (k text, v int, PRIMARY KEY(k, v))")
                      .setExecutionProfile(sessionRule.slowProfile())
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
                  SimpleStatement.builder(
                          "CREATE TABLE IF NOT EXISTS test2 (k text primary key, v0 int)")
                      .setExecutionProfile(sessionRule.slowProfile())
                      .build());

          // table with composite partition key
          sessionRule
              .session()
              .execute(
                  SimpleStatement.builder(
                          "CREATE TABLE IF NOT EXISTS test3 "
                              + "(pk1 int, pk2 int, v int, "
                              + "PRIMARY KEY ((pk1, pk2)))")
                      .setExecutionProfile(sessionRule.slowProfile())
                      .build());
        });
  }

  @Test
  public void should_not_allow_unset_value_when_protocol_less_than_v4() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V3")
            .build();
    try (CqlSession v3Session = SessionUtils.newSession(ccmRule, loader)) {
      // Intentionally use fully qualified table here to avoid warnings as these are not supported
      // by v3 protocol version, see JAVA-3068
      PreparedStatement prepared =
          v3Session.prepare(
              String.format("INSERT INTO %s.test2 (k, v0) values (?, ?)", sessionRule.keyspace()));

      BoundStatement boundStatement =
          prepared.boundStatementBuilder().setString(0, name.getMethodName()).unset(1).build();

      assertThatThrownBy(() -> v3Session.execute(boundStatement))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Unset value at index");
    }
  }

  @Test
  public void should_not_write_tombstone_if_value_is_implicitly_unset() {
    assumeThat(atLeastV4).as("unset values require protocol V4+").isTrue();
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      session.execute(prepared.bind(name.getMethodName(), VALUE));

      BoundStatement boundStatement =
          prepared.boundStatementBuilder().setString(0, name.getMethodName()).build();

      verifyUnset(session, boundStatement, name.getMethodName());
    }
  }

  @Test
  public void should_write_tombstone_if_value_is_explicitly_unset() {
    assumeThat(atLeastV4).as("unset values require protocol V4+").isTrue();
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
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
  public void should_write_tombstone_if_value_is_explicitly_unset_on_builder() {
    assumeThat(atLeastV4).as("unset values require protocol V4+").isTrue();
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
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
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      PreparedStatement prepared = session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      assertThat(prepared.getResultSetDefinitions()).hasSize(0);

      ResultSet rs = session.execute(prepared.bind(name.getMethodName(), VALUE));
      assertThat(rs.getColumnDefinitions()).hasSize(0);
    }
  }

  @Test
  public void should_bind_null_value_when_setting_values_in_bulk() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
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
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      SimpleStatement st = SimpleStatement.builder("SELECT v FROM test").setPageSize(10).build();
      PreparedStatement prepared = session.prepare(st);
      CompletionStage<AsyncResultSet> future = session.executeAsync(prepared.bind());
      AsyncResultSet result = CompletableFutures.getUninterruptibly(future);

      // Should have only fetched 10 (page size) rows.
      assertThat(result.remaining()).isEqualTo(10);
    }
  }

  @Test
  public void should_use_page_size() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {
      // set page size on simple statement, but will be unused since
      // overridden by bound statement.
      SimpleStatement st = SimpleStatement.builder("SELECT v FROM test").setPageSize(10).build();
      PreparedStatement prepared = session.prepare(st);
      CompletionStage<AsyncResultSet> future =
          session.executeAsync(prepared.bind().setPageSize(12));
      AsyncResultSet result = CompletableFutures.getUninterruptibly(future);

      // Should have fetched 12 (page size) rows.
      assertThat(result.remaining()).isEqualTo(12);
    }
  }

  @Test
  public void should_propagate_attributes_when_preparing_a_simple_statement() {
    CqlSession session = sessionRule.session();

    DriverExecutionProfile mockProfile =
        session
            .getContext()
            .getConfig()
            .getDefaultProfile()
            // Value doesn't matter, we just want a distinct profile
            .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10));
    ByteBuffer mockPagingState = Bytes.fromHexString("0xaaaa");
    CqlIdentifier mockKeyspace =
        supportsPerRequestKeyspace(session) ? CqlIdentifier.fromCql("system") : null;
    CqlIdentifier mockRoutingKeyspace = CqlIdentifier.fromCql("mockRoutingKeyspace");
    ByteBuffer mockRoutingKey = Bytes.fromHexString("0xbbbb");
    Token mockRoutingToken = session.getMetadata().getTokenMap().get().newToken(mockRoutingKey);
    Map<String, ByteBuffer> mockCustomPayload =
        NullAllowingImmutableMap.of("key1", Bytes.fromHexString("0xcccc"));
    Duration mockTimeout = Duration.ofSeconds(1);
    ConsistencyLevel mockCl = DefaultConsistencyLevel.LOCAL_QUORUM;
    ConsistencyLevel mockSerialCl = DefaultConsistencyLevel.LOCAL_SERIAL;
    int mockPageSize = 2000;

    SimpleStatementBuilder simpleStatementBuilder =
        SimpleStatement.builder("SELECT release_version FROM system.local")
            .setExecutionProfile(mockProfile)
            .setPagingState(mockPagingState)
            .setKeyspace(mockKeyspace)
            .setRoutingKeyspace(mockRoutingKeyspace)
            .setRoutingKey(mockRoutingKey)
            .setRoutingToken(mockRoutingToken)
            .setQueryTimestamp(42)
            .setIdempotence(true)
            .setTracing()
            .setTimeout(mockTimeout)
            .setConsistencyLevel(mockCl)
            .setSerialConsistencyLevel(mockSerialCl)
            .setPageSize(mockPageSize);

    if (atLeastV4) {
      simpleStatementBuilder =
          simpleStatementBuilder.addCustomPayload("key1", mockCustomPayload.get("key1"));
    }

    PreparedStatement preparedStatement = session.prepare(simpleStatementBuilder.build());

    // Cover all the ways to create bound statements:
    ImmutableList<Function<PreparedStatement, BoundStatement>> createMethods =
        ImmutableList.of(PreparedStatement::bind, p -> p.boundStatementBuilder().build());

    for (Function<PreparedStatement, BoundStatement> createMethod : createMethods) {
      BoundStatement boundStatement = createMethod.apply(preparedStatement);

      assertThat(boundStatement.getExecutionProfile()).isEqualTo(mockProfile);
      assertThat(boundStatement.getPagingState()).isEqualTo(mockPagingState);
      assertThat(boundStatement.getRoutingKeyspace())
          .isEqualTo(mockKeyspace != null ? mockKeyspace : mockRoutingKeyspace);
      assertThat(boundStatement.getRoutingKey()).isEqualTo(mockRoutingKey);
      assertThat(boundStatement.getRoutingToken()).isEqualTo(mockRoutingToken);
      if (atLeastV4) {
        assertThat(boundStatement.getCustomPayload()).isEqualTo(mockCustomPayload);
      }
      assertThat(boundStatement.isIdempotent()).isTrue();
      assertThat(boundStatement.isTracing()).isTrue();
      assertThat(boundStatement.getTimeout()).isEqualTo(mockTimeout);
      assertThat(boundStatement.getConsistencyLevel()).isEqualTo(mockCl);
      assertThat(boundStatement.getSerialConsistencyLevel()).isEqualTo(mockSerialCl);
      assertThat(boundStatement.getPageSize()).isEqualTo(mockPageSize);

      // Bound statements do not support per-query keyspaces, so this is not set
      assertThat(boundStatement.getKeyspace()).isNull();
      // Should not be propagated
      assertThat(boundStatement.getQueryTimestamp()).isEqualTo(Statement.NO_DEFAULT_TIMESTAMP);
    }
  }

  // Test for JAVA-2066
  @Test
  @BackendRequirement(type = BackendType.CASSANDRA, minInclusive = "2.2")
  public void should_compute_routing_key_when_indices_randomly_distributed() {
    try (CqlSession session = SessionUtils.newSession(ccmRule, sessionRule.keyspace())) {

      PreparedStatement ps = session.prepare("INSERT INTO test3 (v, pk2, pk1) VALUES (?,?,?)");

      List<Integer> indices = ps.getPartitionKeyIndices();
      assertThat(indices).containsExactly(2, 1);

      BoundStatement bs = ps.bind(1, 2, 3);
      ByteBuffer routingKey = bs.getRoutingKey();

      assertThat(routingKey)
          .isEqualTo(RoutingKey.compose(bs.getBytesUnsafe(2), bs.getBytesUnsafe(1)));
    }
  }

  @Test
  public void should_set_all_occurrences_of_variable() {
    CqlSession session = sessionRule.session();
    PreparedStatement ps = session.prepare("INSERT INTO test3 (pk1, pk2, v) VALUES (:i, :i, :i)");

    CqlIdentifier id = CqlIdentifier.fromCql("i");
    ColumnDefinitions variableDefinitions = ps.getVariableDefinitions();
    assertThat(variableDefinitions.allIndicesOf(id)).containsExactly(0, 1, 2);

    should_set_all_occurrences_of_variable(ps.bind().setInt(id, 12));
    should_set_all_occurrences_of_variable(ps.boundStatementBuilder().setInt(id, 12).build());
  }

  private void should_set_all_occurrences_of_variable(BoundStatement bs) {
    assertThat(bs.getInt(0)).isEqualTo(12);
    assertThat(bs.getInt(1)).isEqualTo(12);
    assertThat(bs.getInt(2)).isEqualTo(12);

    // Nothing should be shared internally (this would be a bug if the client later retrieves a
    // buffer with getBytesUnsafe and modifies it)
    ByteBuffer bytes0 = bs.getBytesUnsafe(0);
    ByteBuffer bytes1 = bs.getBytesUnsafe(1);
    assertThat(bytes0).isNotNull();
    assertThat(bytes1).isNotNull();
    // Not the same instance
    assertThat(bytes0).isNotSameAs(bytes1);
    // Contents are not shared
    bytes0.putInt(0, 11);
    assertThat(bytes1.getInt(0)).isEqualTo(12);
    bytes0.putInt(0, 12);

    CqlSession session = sessionRule.session();
    session.execute(bs);
    Row row = session.execute("SELECT * FROM test3 WHERE pk1 = 12 AND pk2 = 12").one();
    assertThat(row).isNotNull();
    assertThat(row.getInt("v")).isEqualTo(12);
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
            .addContactEndPoints(ccmRule.getContactPoints())
            .withKeyspace(sessionRule.keyspace())
            .addTypeCodecs(codec)
            .build();
  }

  private boolean supportsPerRequestKeyspace(CqlSession session) {
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    ProtocolVersionRegistry protocolVersionRegistry = context.getProtocolVersionRegistry();
    return protocolVersionRegistry.supports(
        context.getProtocolVersion(), DefaultProtocolFeature.PER_REQUEST_KEYSPACE);
  }
}
