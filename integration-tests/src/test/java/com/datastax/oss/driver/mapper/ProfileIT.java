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
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoProfile;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ProfileIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static ProfileIT.SimpleDao dao;

  @BeforeClass
  public static void setupClass() {
    primeDeleteQuery();
    primeInsertQuery();
    primeSelectQuery();
    primeCountQuery();
    primeUpdateQuery();

    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .startProfile("cl")
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ANY")
            .build();
    CqlSession mapperSession = SessionUtils.newSession(SIMULACRON_RULE, loader);

    ProfileIT.InventoryMapper inventoryMapper =
        new ProfileIT_InventoryMapperBuilder(mapperSession).build();
    dao = inventoryMapper.simpleDao("cl");
  }

  @Before
  public void setup() {
    SIMULACRON_RULE.cluster().clearLogs();
  }

  private static final ProfileIT.Simple simple = new ProfileIT.Simple(UUID.randomUUID(), "DATA");

  @Test
  public void should_honor_exec_profile_on_insert() {
    dao.save(simple);

    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0));
  }

  @Test
  public void should_honor_exec_profile_on_delete() {
    dao.delete(simple);

    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0));
  }

  @Test
  public void should_honor_exec_profile_on_update() {
    dao.update(simple);

    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0));
  }

  @Test
  public void should_honor_exec_profile_on_query() {
    dao.findByPk(simple.pk);

    ClusterQueryLogReport report = SIMULACRON_RULE.cluster().getLogs();
    validateQueryOptions(report.getQueryLogs().get(0));
  }

  private void validateQueryOptions(QueryLog log) {

    Message message = log.getFrame().message;
    assertThat(message).isInstanceOf(Execute.class);
    Execute queryExecute = (Execute) message;
    assertThat(queryExecute.options.consistency)
        .isEqualTo(DefaultConsistencyLevel.ANY.getProtocolCode());
  }

  private static void primeInsertQuery() {
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk(), "data", simple.getData());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid", "data", "ascii");
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
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid");
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
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid");
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
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid");
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
    Map<String, Object> params = ImmutableMap.of("pk", simple.getPk(), "data", simple.getData());
    Map<String, String> paramTypes = ImmutableMap.of("pk", "uuid", "data", "ascii");
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

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProfileIT.SimpleDao simpleDao(@DaoProfile String executionProfile);
  }

  @Dao
  public interface SimpleDao {
    @Insert
    void save(ProfileIT.Simple simple);

    @Delete
    void delete(ProfileIT.Simple simple);

    @Select
    ProfileIT.Simple findByPk(UUID pk);

    @Query("SELECT count(*) FROM ks.simple WHERE pk=:pk")
    long count(UUID pk);

    @Update
    void update(ProfileIT.Simple simple);
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
      if (!(o instanceof ProfileIT.Simple)) {
        return false;
      }
      ProfileIT.Simple simple = (ProfileIT.Simple) o;
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
