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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.mapper.StatementAttributes;
import com.datastax.oss.driver.api.mapper.StatementAttributesBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class StatementAttributesIT {

  @ClassRule
  public static SimulacronRule simulacronRule =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(simulacronRule).build();

  private static SimpleDao simpleDao;

  private static String PAGING_STATE = "paging_state";
  private static int PAGE_SIZE = 13;

  private static final Simple simple = new Simple(UUID.randomUUID(), "DATA");

  @Before
  public void setup() {
    simulacronRule.cluster().clearPrimes(true);
    simulacronRule.cluster().clearLogs();
  }

  @Test
  public void should_honor_runtime_attributes_insert() {
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk(), "data", simple.getData());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid", "data", "ascii");
    simulacronRule
        .cluster()
        .prime(
            when(query(
                    "INSERT INTO simple (pk,data) VALUES (:pk,:data)",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(noRows()));
    CqlSession session = SessionUtils.newSession(simulacronRule);
    InventoryMapper inventoryMapper =
        new StatementAttributesIT_InventoryMapperBuilder(session).build();
    simpleDao = inventoryMapper.simpleDao(sessionRule.keyspace());
    StatementAttributes attributes = buildRunTimeAttributes();
    simulacronRule.cluster().clearLogs();
    simpleDao.save(simple, attributes);
    ClusterQueryLogReport report = simulacronRule.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0));
  }

  @Test
  public void should_honor_runtime_attributes_delete() {
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid");
    simulacronRule
        .cluster()
        .prime(
            when(query(
                    "DELETE FROM simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(noRows())
                .delay(1, TimeUnit.MILLISECONDS));
    CqlSession session = SessionUtils.newSession(simulacronRule);
    InventoryMapper inventoryMapper =
        new StatementAttributesIT_InventoryMapperBuilder(session).build();
    simpleDao = inventoryMapper.simpleDao(sessionRule.keyspace());
    StatementAttributes attributes = buildRunTimeAttributes();
    simulacronRule.cluster().clearLogs();
    simpleDao.delete(simple, attributes);
    ClusterQueryLogReport report = simulacronRule.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0));
  }

  @Test
  public void should_honor_runtime_attributes_select() {
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid");
    simulacronRule
        .cluster()
        .prime(
            when(query(
                    "SELECT pk,data FROM simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ANY),
                    params,
                    paramTypes))
                .then(noRows())
                .delay(1, TimeUnit.MILLISECONDS));
    CqlSession session = SessionUtils.newSession(simulacronRule);
    InventoryMapper inventoryMapper =
        new StatementAttributesIT_InventoryMapperBuilder(session).build();
    simpleDao = inventoryMapper.simpleDao(sessionRule.keyspace());

    StatementAttributes attributes = buildRunTimeAttributes();
    simulacronRule.cluster().clearLogs();
    simpleDao.findByPk(simple.getPk(), attributes);
    ClusterQueryLogReport report = simulacronRule.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0));
  }

  private StatementAttributes buildRunTimeAttributes() {
    StatementAttributes attributes =
        StatementAttributes.builder()
            .withTimeout(Duration.ofSeconds(3))
            .withConsistencyLevel(DefaultConsistencyLevel.QUORUM)
            .build();
    StatementAttributesBuilder builder = StatementAttributes.builder();

    return builder
        .withConsistencyLevel(DefaultConsistencyLevel.ANY)
        .withPageSize(PAGE_SIZE)
        .withSerialConsistencyLevel(DefaultConsistencyLevel.QUORUM)
        .withPagingState(ByteBuffer.wrap(PAGING_STATE.getBytes(UTF_8)))
        .build();
  }

  private void validateQueryOptions(QueryLog log) {

    Message message = log.getFrame().message;
    assertThat(message).isInstanceOf(Execute.class);
    Execute queryExecute = (Execute) message;
    assertThat(queryExecute.options.consistency)
        .isEqualTo(DefaultConsistencyLevel.ANY.getProtocolCode());
    assertThat(queryExecute.options.serialConsistency)
        .isEqualTo(DefaultConsistencyLevel.QUORUM.getProtocolCode());
    assertThat(queryExecute.options.pageSize).isEqualTo(PAGE_SIZE);
    String pagingState = UTF_8.decode(queryExecute.options.pagingState).toString();
    assertThat(pagingState).isEqualTo(PAGING_STATE);
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    StatementAttributesIT.SimpleDao simpleDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface SimpleDao {
    @Insert
    void save(Simple simple, StatementAttributes attributes);

    @Delete
    void delete(Simple simple, StatementAttributes attributes);

    @Select
    Simple findByPk(UUID pk, StatementAttributes attributes);
  }

  @Entity
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
