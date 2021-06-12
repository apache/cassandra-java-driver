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
package com.datastax.oss.driver.mapper;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.query;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class StatementAttributesIT {

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));
  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static String PAGING_STATE = "paging_state";
  private static int PAGE_SIZE = 13;

  private static final Simple simple = new Simple(UUID.randomUUID(), "DATA");

  @SuppressWarnings("UnnecessaryLambda")
  private static final Function<BoundStatementBuilder, BoundStatementBuilder> statementFunction =
      builder ->
          builder
              .setConsistencyLevel(DefaultConsistencyLevel.ANY)
              .setPageSize(PAGE_SIZE)
              .setSerialConsistencyLevel(DefaultConsistencyLevel.QUORUM)
              .setPagingState(ByteBuffer.wrap(PAGING_STATE.getBytes(UTF_8)));

  @SuppressWarnings("UnnecessaryLambda")
  private static final Function<BoundStatementBuilder, BoundStatementBuilder> badStatementFunction =
      builder -> {
        throw new IllegalStateException("mock error");
      };

  private static SimpleDao dao;

  @BeforeClass
  public static void setupClass() {
    primeDeleteQuery();
    primeInsertQuery();
    primeSelectQuery();
    primeCountQuery();
    primeUpdateQuery();

    InventoryMapper inventoryMapper =
        new StatementAttributesIT_InventoryMapperBuilder(SESSION_RULE.session()).build();
    dao = inventoryMapper.simpleDao();
  }

  @Before
  public void setup() {
    SIMULACRON_RULE.cluster().clearLogs();
  }

  @Test
  public void should_honor_runtime_attributes_on_insert() {
    dao.save(simple, statementFunction);

    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), true);
  }

  @Test
  public void should_honor_annotation_attributes_on_insert() {
    dao.save2(simple);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), false);
  }

  @Test
  public void should_use_runtime_attributes_over_annotation_attributes() {
    dao.save3(simple, statementFunction);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), false);
  }

  @Test
  public void should_honor_runtime_attributes_on_delete() {
    dao.delete(simple, statementFunction);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), true);
  }

  @Test
  public void should_honor_annotation_attributes_on_delete() {
    dao.delete2(simple);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), false);
  }

  @Test
  public void should_honor_runtime_attributes_on_select() {
    dao.findByPk(simple.getPk(), statementFunction);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), true);
  }

  @Test
  public void should_honor_annotation_attributes_on_select() {
    dao.findByPk2(simple.getPk());
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), false);
  }

  @Test
  public void should_honor_runtime_attributes_on_query() {
    dao.count(simple.getPk(), statementFunction);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), true);
  }

  @Test
  public void should_honor_annotation_attributes_on_query() {
    dao.count2(simple.getPk());
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), false);
  }

  @Test
  public void should_honor_runtime_attributes_on_update() {
    dao.update(simple, statementFunction);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), true);
  }

  @Test
  public void should_honor_annotation_attributes_on_update() {
    dao.update2(simple);
    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0), false);
  }

  @Test
  public void should_fail_runtime_attributes_bad() {
    Throwable t = catchThrowable(() -> dao.save(simple, badStatementFunction));
    assertThat(t).isInstanceOf(IllegalStateException.class).hasMessage("mock error");
  }

  private static void primeInsertQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", simple.getPk(), "data", simple.getData()));
    LinkedHashMap<String, String> paramTypes =
        new LinkedHashMap<>(ImmutableMap.of("pk", "uuid", "data", "ascii"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "INSERT INTO ks.simple (pk,data) VALUES (:pk,:data)",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(noRows()));
  }

  private static void primeDeleteQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", simple.getPk()));
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("pk", "uuid"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "DELETE FROM ks.simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(noRows())
                .delay(1, TimeUnit.MILLISECONDS));
  }

  private static void primeSelectQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", simple.getPk()));
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("pk", "uuid"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "SELECT pk,data FROM ks.simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(noRows())
                .delay(1, TimeUnit.MILLISECONDS));
  }

  private static void primeCountQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", simple.getPk()));
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("pk", "uuid"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "SELECT count(*) FROM ks.simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(PrimeDsl.rows().row("count", 1L).columnTypes("count", "bigint").build())
                .delay(1, TimeUnit.MILLISECONDS));
  }

  private static void primeUpdateQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", simple.getPk(), "data", simple.getData()));
    LinkedHashMap<String, String> paramTypes =
        new LinkedHashMap<>(ImmutableMap.of("pk", "uuid", "data", "ascii"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "UPDATE ks.simple SET data=:data WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(noRows()));
  }

  private void validateQueryOptions(QueryLog log, boolean validatePageState) {

    Message message = log.getFrame().message;
    assertThat(message).isInstanceOf(Execute.class);
    Execute queryExecute = (Execute) message;
    assertThat(queryExecute.options.consistency)
        .isEqualTo(DefaultConsistencyLevel.ANY.getProtocolCode());
    assertThat(queryExecute.options.serialConsistency)
        .isEqualTo(DefaultConsistencyLevel.QUORUM.getProtocolCode());
    assertThat(queryExecute.options.pageSize).isEqualTo(PAGE_SIZE);
    if (validatePageState) {
      String pagingState = UTF_8.decode(queryExecute.options.pagingState).toString();
      assertThat(pagingState).isEqualTo(PAGING_STATE);
    }
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    StatementAttributesIT.SimpleDao simpleDao();
  }

  @Dao
  public interface SimpleDao {
    @Insert
    void save(Simple simple, Function<BoundStatementBuilder, BoundStatementBuilder> function);

    @Insert
    @StatementAttributes(consistencyLevel = "ANY", serialConsistencyLevel = "QUORUM", pageSize = 13)
    void save2(Simple simple);

    @Insert
    @StatementAttributes(consistencyLevel = "ONE", pageSize = 500)
    void save3(Simple simple, Function<BoundStatementBuilder, BoundStatementBuilder> function);

    @Delete
    void delete(Simple simple, Function<BoundStatementBuilder, BoundStatementBuilder> function);

    @Delete
    @StatementAttributes(consistencyLevel = "ANY", serialConsistencyLevel = "QUORUM", pageSize = 13)
    void delete2(Simple simple);

    @Select
    @SuppressWarnings("UnusedReturnValue")
    Simple findByPk(UUID pk, Function<BoundStatementBuilder, BoundStatementBuilder> function);

    @Select
    @StatementAttributes(consistencyLevel = "ANY", serialConsistencyLevel = "QUORUM", pageSize = 13)
    @SuppressWarnings("UnusedReturnValue")
    Simple findByPk2(UUID pk);

    @Query("SELECT count(*) FROM ks.simple WHERE pk=:pk")
    long count(UUID pk, Function<BoundStatementBuilder, BoundStatementBuilder> function);

    @Query("SELECT count(*) FROM ks.simple WHERE pk=:pk")
    @StatementAttributes(consistencyLevel = "ANY", serialConsistencyLevel = "QUORUM", pageSize = 13)
    @SuppressWarnings("UnusedReturnValue")
    long count2(UUID pk);

    @Update
    void update(Simple simple, Function<BoundStatementBuilder, BoundStatementBuilder> function);

    @Update
    @StatementAttributes(consistencyLevel = "ANY", serialConsistencyLevel = "QUORUM", pageSize = 13)
    void update2(Simple simple);
  }

  @Entity(defaultKeyspace = "ks")
  public static class Simple {
    @PartitionKey private UUID pk;
    private String data;

    public Simple() {}

    public Simple(UUID pk, String data) {
      this.pk = pk;
      this.data = data;
    }

    public UUID getPk() {
      return pk;
    }

    public String getData() {
      return data;
    }

    public void setPk(UUID pk) {

      this.pk = pk;
    }

    public void setData(String data) {
      this.data = data;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Simple)) {
        return false;
      }
      Simple simple = (Simple) o;
      return Objects.equals(pk, simple.pk) && Objects.equals(data, simple.data);
    }

    @Override
    public int hashCode() {

      return Objects.hash(pk, data);
    }

    @Override
    public String toString() {
      return "Simple{" + "pk=" + pk + ", data='" + data + '\'' + '}';
    }
  }
}
