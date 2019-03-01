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
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.DefaultProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.type.codec.CqlIntToStringCodec;
import com.datastax.oss.driver.internal.core.util.RoutingKey;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class BoundStatementIT {

  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private CcmRule ccm = CcmRule.getInstance();

  private final boolean atLeastV4 = ccm.getHighestProtocolVersion().getCode() >= 4;

  private SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccm)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.REQUEST_PAGE_SIZE, 20)
                  .build())
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @Rule public TestName name = new TestName();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String KEY = "test";

  private static final int VALUE = 7;

  @Before
  public void setupSchema() {
    // table where every column forms the primary key.
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
            SimpleStatement.builder("CREATE TABLE IF NOT EXISTS test2 (k text primary key, v0 int)")
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
  }

  @Before
  public void clearPrimes() {
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);
  }

  @Test(expected = IllegalStateException.class)
  public void should_not_allow_unset_value_when_protocol_less_than_v4() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V3")
            .build();
    try (CqlSession v3Session = SessionUtils.newSession(ccm, sessionRule.keyspace(), loader)) {
      PreparedStatement prepared = v3Session.prepare("INSERT INTO test2 (k, v0) values (?, ?)");

      BoundStatement boundStatement =
          prepared.boundStatementBuilder().setString(0, name.getMethodName()).unset(1).build();

      v3Session.execute(boundStatement);
    }
  }

  @Test
  public void should_not_write_tombstone_if_value_is_implicitly_unset() {
    assumeThat(atLeastV4).as("unset values require protocol V4+").isTrue();
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
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
  public void should_write_tombstone_if_value_is_explicitly_unset_on_builder() {
    assumeThat(atLeastV4).as("unset values require protocol V4+").isTrue();
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
      SimpleStatement st = SimpleStatement.builder("SELECT v FROM test").setPageSize(10).build();
      PreparedStatement prepared = session.prepare(st);
      CompletionStage<? extends AsyncResultSet> future = session.executeAsync(prepared.bind());
      AsyncResultSet result = CompletableFutures.getUninterruptibly(future);

      // Should have only fetched 10 (page size) rows.
      assertThat(result.remaining()).isEqualTo(10);
    }
  }

  @Test
  public void should_use_page_size() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {
      // set page size on simple statement, but will be unused since
      // overridden by bound statement.
      SimpleStatement st = SimpleStatement.builder("SELECT v FROM test").setPageSize(10).build();
      PreparedStatement prepared = session.prepare(st);
      CompletionStage<? extends AsyncResultSet> future =
          session.executeAsync(prepared.bind().setPageSize(12));
      AsyncResultSet result = CompletableFutures.getUninterruptibly(future);

      // Should have fetched 12 (page size) rows.
      assertThat(result.remaining()).isEqualTo(12);
    }
  }

  @Test
  public void should_use_consistencies_from_simple_statement() {
    try (CqlSession session = SessionUtils.newSession(simulacron)) {
      SimpleStatement st =
          SimpleStatement.builder("SELECT * FROM test where k = ?")
              .setConsistencyLevel(DefaultConsistencyLevel.TWO)
              .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
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
              .setConsistencyLevel(DefaultConsistencyLevel.TWO)
              .setSerialConsistencyLevel(DefaultConsistencyLevel.LOCAL_SERIAL)
              .build();
      PreparedStatement prepared = session.prepare(st);
      simulacron.cluster().clearLogs();
      // since query is unprimed, we use a text value for bind parameter as this is
      // what simulacron expects for unprimed statements.
      session.execute(
          prepared
              .boundStatementBuilder("0")
              .setConsistencyLevel(DefaultConsistencyLevel.THREE)
              .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
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
              .setTimeout(Duration.ofSeconds(1))
              .setConsistencyLevel(DefaultConsistencyLevel.ONE)
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
              .setTimeout(Duration.ofSeconds(1))
              .setConsistencyLevel(DefaultConsistencyLevel.ONE)
              .build();
      PreparedStatement prepared = session.prepare(st);

      thrown.expect(DriverTimeoutException.class);
      thrown.expectMessage("Query timed out after PT0.15S");

      session.execute(prepared.bind(0).setTimeout(Duration.ofMillis(150)));
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
    String mockConfigProfileName = "mockConfigProfileName";
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
            .setExecutionProfileName(mockConfigProfileName)
            .setPagingState(mockPagingState)
            .setKeyspace(mockKeyspace)
            .setRoutingKeyspace(mockRoutingKeyspace)
            .setRoutingKey(mockRoutingKey)
            .setRoutingToken(mockRoutingToken)
            .setTimestamp(42)
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
      assertThat(boundStatement.getExecutionProfileName()).isEqualTo(mockConfigProfileName);
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
      assertThat(boundStatement.getTimestamp()).isEqualTo(Long.MIN_VALUE);
    }
  }

  // Test for JAVA-2066
  @Test
  @CassandraRequirement(min = "2.2")
  public void should_compute_routing_key_when_indices_randomly_distributed() {
    try (CqlSession session = SessionUtils.newSession(ccm, sessionRule.keyspace())) {

      PreparedStatement ps = session.prepare("INSERT INTO test3 (v, pk2, pk1) VALUES (?,?,?)");

      List<Integer> indices = ps.getPartitionKeyIndices();
      assertThat(indices).containsExactly(2, 1);

      BoundStatement bs = ps.bind(1, 2, 3);
      ByteBuffer routingKey = bs.getRoutingKey();

      assertThat(routingKey)
          .isEqualTo(RoutingKey.compose(bs.getBytesUnsafe(2), bs.getBytesUnsafe(1)));
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

  private boolean supportsPerRequestKeyspace(CqlSession session) {
    InternalDriverContext context = (InternalDriverContext) session.getContext();
    ProtocolVersionRegistry protocolVersionRegistry = context.getProtocolVersionRegistry();
    return protocolVersionRegistry.supports(
        context.getProtocolVersion(), DefaultProtocolFeature.PER_REQUEST_KEYSPACE);
  }
}
