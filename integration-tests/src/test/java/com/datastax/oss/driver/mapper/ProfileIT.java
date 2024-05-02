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
package com.datastax.oss.driver.mapper;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.query;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
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
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class ProfileIT {

  private static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(SIMULACRON_RULE)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .startProfile("cl_one")
                  .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE")
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(SIMULACRON_RULE).around(SESSION_RULE);

  private static final Simple SAMPLE_ENTITY = new Simple(UUID.randomUUID(), "DATA");

  private static DriverExecutionProfile clTwoProfile;
  private MapperBuilder<SimpleMapper> mapperBuilder;

  @BeforeClass
  public static void setupClass() {
    primeDeleteQuery();
    primeInsertQuery();
    primeSelectQuery();
    primeCountQuery();
    primeUpdateQuery();

    // Deliberately based on the default profile, so that we can assert that a dynamically-set
    // option is correctly taken into account
    clTwoProfile =
        SESSION_RULE
            .session()
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "TWO");
  }

  @Before
  public void setup() {
    SIMULACRON_RULE.cluster().clearLogs();
    mapperBuilder = SimpleMapper.builder(SESSION_RULE.session());
  }

  @Test
  public void should_build_dao_with_profile_name() {
    SimpleMapper mapper = mapperBuilder.build();
    SimpleDao dao = mapper.simpleDao("cl_one");
    assertClForAllQueries(dao, ConsistencyLevel.ONE);
  }

  @Test
  public void should_build_dao_with_profile() {
    SimpleMapper mapper = mapperBuilder.build();
    SimpleDao dao = mapper.simpleDao(clTwoProfile);
    assertClForAllQueries(dao, ConsistencyLevel.TWO);
  }

  @Test
  public void should_inherit_mapper_profile_name() {
    SimpleMapper mapper = mapperBuilder.withDefaultExecutionProfileName("cl_one").build();
    SimpleDao dao = mapper.simpleDao();
    assertClForAllQueries(dao, ConsistencyLevel.ONE);
  }

  @Test
  public void should_inherit_mapper_profile() {
    SimpleMapper mapper = mapperBuilder.withDefaultExecutionProfile(clTwoProfile).build();
    SimpleDao dao = mapper.simpleDao();
    assertClForAllQueries(dao, ConsistencyLevel.TWO);
  }

  @Test
  public void should_override_mapper_profile_name() {
    SimpleMapper mapper =
        mapperBuilder
            .withDefaultExecutionProfileName("defaultProfile") // doesn't need to exist
            .build();
    SimpleDao dao = mapper.simpleDao("cl_one");
    assertClForAllQueries(dao, ConsistencyLevel.ONE);
  }

  @Test
  public void should_override_mapper_profile() {
    DriverExecutionProfile clThreeProfile =
        SESSION_RULE
            .session()
            .getContext()
            .getConfig()
            .getDefaultProfile()
            .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "THREE");
    SimpleMapper mapper = mapperBuilder.withDefaultExecutionProfile(clThreeProfile).build();
    SimpleDao dao = mapper.simpleDao(clTwoProfile);
    assertClForAllQueries(dao, ConsistencyLevel.TWO);
  }

  @Test
  public void should_override_mapper_profile_name_with_a_profile() {
    SimpleMapper mapper =
        mapperBuilder
            .withDefaultExecutionProfileName("defaultProfile") // doesn't need to exist
            .build();
    SimpleDao dao = mapper.simpleDao(clTwoProfile);
    assertClForAllQueries(dao, ConsistencyLevel.TWO);
  }

  @Test
  public void should_override_mapper_profile_with_a_name() {
    SimpleMapper mapper = mapperBuilder.withDefaultExecutionProfile(clTwoProfile).build();
    SimpleDao dao = mapper.simpleDao("cl_one");
    assertClForAllQueries(dao, ConsistencyLevel.ONE);
  }

  @Test
  public void should_use_default_when_no_profile() {
    SimpleMapper mapper = mapperBuilder.build();
    SimpleDao dao = mapper.simpleDao();
    // Default CL inherited from reference.conf
    assertClForAllQueries(dao, ConsistencyLevel.LOCAL_ONE);
  }

  private void assertClForAllQueries(SimpleDao dao, ConsistencyLevel expectedLevel) {
    dao.save(SAMPLE_ENTITY);
    assertServerSideCl(expectedLevel);
    dao.delete(SAMPLE_ENTITY);
    assertServerSideCl(expectedLevel);
    dao.update(SAMPLE_ENTITY);
    assertServerSideCl(expectedLevel);
    dao.findByPk(SAMPLE_ENTITY.pk);
    assertServerSideCl(expectedLevel);

    // Special cases: profile defined at the method level with statement attributes, should override
    // dao-level profile.
    dao.saveWithClOne(SAMPLE_ENTITY);
    assertServerSideCl(ConsistencyLevel.ONE);
    dao.saveWithCustomAttributes(SAMPLE_ENTITY, bs -> bs.setExecutionProfileName("cl_one"));
    assertServerSideCl(ConsistencyLevel.ONE);
  }

  private void assertServerSideCl(ConsistencyLevel expectedCl) {
    List<QueryLog> queryLogs = SIMULACRON_RULE.cluster().getLogs().getQueryLogs();
    QueryLog lastLog = queryLogs.get(queryLogs.size() - 1);
    Message message = lastLog.getFrame().message;
    assertThat(message).isInstanceOf(Execute.class);
    Execute queryExecute = (Execute) message;
    assertThat(queryExecute.options.consistency).isEqualTo(expectedCl.getProtocolCode());
  }

  private static void primeInsertQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(
            ImmutableMap.of("pk", SAMPLE_ENTITY.getPk(), "data", SAMPLE_ENTITY.getData()));
    LinkedHashMap<String, String> paramTypes =
        new LinkedHashMap<>(ImmutableMap.of("pk", "uuid", "data", "ascii"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "INSERT INTO ks.simple (pk,data) VALUES (:pk,:data)",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.TWO),
                    params,
                    paramTypes))
                .then(noRows()));
  }

  private static void primeDeleteQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", SAMPLE_ENTITY.getPk()));
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("pk", "uuid"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "DELETE FROM ks.simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.TWO),
                    params,
                    paramTypes))
                .then(noRows())
                .delay(1, TimeUnit.MILLISECONDS));
  }

  private static void primeSelectQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", SAMPLE_ENTITY.getPk()));
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("pk", "uuid"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "SELECT pk,data FROM ks.simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.TWO),
                    params,
                    paramTypes))
                .then(noRows())
                .delay(1, TimeUnit.MILLISECONDS));
  }

  private static void primeCountQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(ImmutableMap.of("pk", SAMPLE_ENTITY.getPk()));
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>(ImmutableMap.of("pk", "uuid"));
    SIMULACRON_RULE
        .cluster()
        .prime(
            when(query(
                    "SELECT count(*) FROM ks.simple WHERE pk=:pk",
                    Lists.newArrayList(
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.LOCAL_ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.ONE,
                        com.datastax.oss.simulacron.common.codec.ConsistencyLevel.TWO),
                    params,
                    paramTypes))
                .then(PrimeDsl.rows().row("count", 1L).columnTypes("count", "bigint").build())
                .delay(1, TimeUnit.MILLISECONDS));
  }

  private static void primeUpdateQuery() {
    LinkedHashMap<String, Object> params =
        new LinkedHashMap<>(
            ImmutableMap.of("pk", SAMPLE_ENTITY.getPk(), "data", SAMPLE_ENTITY.getData()));
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

  @Mapper
  public interface SimpleMapper {
    @DaoFactory
    SimpleDao simpleDao();

    @DaoFactory
    SimpleDao simpleDao(@DaoProfile String executionProfile);

    @DaoFactory
    SimpleDao simpleDao(@DaoProfile DriverExecutionProfile executionProfile);

    static MapperBuilder<SimpleMapper> builder(CqlSession session) {
      return new ProfileIT_SimpleMapperBuilder(session);
    }
  }

  @Dao
  public interface SimpleDao {
    @Insert
    void save(Simple simple);

    @Delete
    void delete(Simple simple);

    @Select
    @SuppressWarnings("UnusedReturnValue")
    Simple findByPk(UUID pk);

    @Query("SELECT count(*) FROM ks.simple WHERE pk=:pk")
    long count(UUID pk);

    @Update
    void update(Simple simple);

    @Insert
    @StatementAttributes(executionProfileName = "cl_one")
    void saveWithClOne(Simple simple);

    @Insert
    void saveWithCustomAttributes(Simple simple, UnaryOperator<BoundStatementBuilder> attributes);
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
