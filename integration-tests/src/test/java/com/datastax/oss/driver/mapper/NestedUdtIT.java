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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/** Tests that entities with UDTs nested at various levels are properly mapped. */
@Category(ParallelizableTests.class)
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "2.2",
    description = "support for unset values")
public class NestedUdtIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static final UUID CONTAINER_ID = UUID.randomUUID();

  private static final Container SAMPLE_CONTAINER =
      new Container(
          CONTAINER_ID,
          ImmutableList.of(new Type1("a1", "a2"), new Type1("b1", "b2")),
          ImmutableMap.of(
              "cd",
              ImmutableList.of(new Type1("c1", "c2"), new Type1("d1", "d2")),
              "ef",
              ImmutableList.of(new Type1("e1", "e2"), new Type1("f1", "f2"))),
          ImmutableMap.of(
              new Type1("12", "34"),
              ImmutableSet.of(
                  ImmutableList.of(new Type2(1, 2)), ImmutableList.of(new Type2(3, 4)))),
          ImmutableMap.of(
              new Type1("12", "34"),
              ImmutableMap.of("12", ImmutableSet.of(new Type2(1, 2), new Type2(3, 4)))));

  private static final Container SAMPLE_CONTAINER_NULL_LIST =
      new Container(
          CONTAINER_ID,
          null,
          ImmutableMap.of(
              "cd",
              ImmutableList.of(new Type1("c1", "c2"), new Type1("d1", "d2")),
              "ef",
              ImmutableList.of(new Type1("e1", "e2"), new Type1("f1", "f2"))),
          ImmutableMap.of(
              new Type1("12", "34"),
              ImmutableSet.of(
                  ImmutableList.of(new Type2(1, 2)), ImmutableList.of(new Type2(3, 4)))),
          ImmutableMap.of(
              new Type1("12", "34"),
              ImmutableMap.of("12", ImmutableSet.of(new Type2(1, 2), new Type2(3, 4)))));

  private static ContainerDao containerDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query :
        ImmutableList.of(
            "CREATE TYPE type1(s1 text, s2 text)",
            "CREATE TYPE type2(i1 int, i2 int)",
            "CREATE TYPE type1_partial(s1 text)",
            "CREATE TYPE type2_partial(i1 int)",
            "CREATE TABLE container(id uuid PRIMARY KEY, "
                + "list frozen<list<type1>>, "
                + "map1 frozen<map<text, list<type1>>>, "
                + "map2 frozen<map<type1, set<list<type2>>>>,"
                + "map3 frozen<map<type1, map<text, set<type2>>>>"
                + ")",
            "CREATE TABLE container_partial(id uuid PRIMARY KEY, "
                + "list frozen<list<type1_partial>>, "
                + "map1 frozen<map<text, list<type1_partial>>>, "
                + "map2 frozen<map<type1_partial, set<list<type2_partial>>>>,"
                + "map3 frozen<map<type1_partial, map<text, set<type2_partial>>>>"
                + ")")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    UserDefinedType type1Partial =
        session
            .getKeyspace()
            .flatMap(ks -> session.getMetadata().getKeyspace(ks))
            .flatMap(ks -> ks.getUserDefinedType("type1_partial"))
            .orElseThrow(AssertionError::new);

    session.execute(
        SimpleStatement.newInstance(
            "INSERT INTO container_partial (id, list) VALUES (?, ?)",
            SAMPLE_CONTAINER.getId(),
            Lists.newArrayList(type1Partial.newValue("a"), type1Partial.newValue("b"))));

    UdtsMapper udtsMapper = new NestedUdtIT_UdtsMapperBuilder(session).build();
    containerDao = udtsMapper.containerDao(SESSION_RULE.keyspace());
  }

  @Before
  public void clearContainerData() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE container")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());
  }

  @Test
  public void should_insert_and_retrieve_entity_with_nested_udts() {
    // When
    containerDao.save(SAMPLE_CONTAINER);
    Container retrievedEntity = containerDao.loadByPk(SAMPLE_CONTAINER.getId());

    // Then
    assertThat(retrievedEntity).isEqualTo(SAMPLE_CONTAINER);
  }

  @Test
  public void should_insert_do_not_set_to_null_udts() {
    // Given
    containerDao.save(SAMPLE_CONTAINER);
    Container retrievedEntity = containerDao.loadByPk(SAMPLE_CONTAINER.getId());

    assertThat(retrievedEntity.list).isNotNull();

    // When
    containerDao.saveDoNotSetNull(SAMPLE_CONTAINER_NULL_LIST);
    Container retrievedEntitySecond = containerDao.loadByPk(SAMPLE_CONTAINER.getId());
    assertThat(retrievedEntitySecond.list).isNotNull();
  }

  @Test
  public void should_insert_set_to_null_udts() {
    // Given
    containerDao.save(SAMPLE_CONTAINER);
    Container retrievedEntity = containerDao.loadByPk(SAMPLE_CONTAINER.getId());

    assertThat(retrievedEntity.list).isNotNull();

    // When
    containerDao.saveSetToNull(SAMPLE_CONTAINER_NULL_LIST);
    Container retrievedEntitySecond = containerDao.loadByPk(SAMPLE_CONTAINER.getId());
    assertThat(retrievedEntitySecond.list).isEmpty();
  }

  @Test
  public void should_get_entity_from_complete_row() {
    CqlSession session = SESSION_RULE.session();
    containerDao.save(SAMPLE_CONTAINER);
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT * FROM container WHERE id = ?", SAMPLE_CONTAINER.getId()));
    Row row = rs.one();
    assertThat(row).isNotNull();
    Container actual = containerDao.get(row);
    assertThat(actual).isEqualTo(SAMPLE_CONTAINER);
  }

  @Test
  public void should_not_get_entity_from_partial_row_when_not_lenient() {
    CqlSession session = SESSION_RULE.session();
    containerDao.save(SAMPLE_CONTAINER);
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT id FROM container WHERE id = ?", SAMPLE_CONTAINER.getId()));
    Row row = rs.one();
    assertThat(row).isNotNull();
    Throwable error = catchThrowable(() -> containerDao.get(row));
    assertThat(error).hasMessage("list is not a column in this row");
  }

  @Test
  public void should_get_entity_from_partial_row_when_lenient() {
    CqlSession session = SESSION_RULE.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "SELECT id, list FROM container_partial WHERE id = ?", SAMPLE_CONTAINER.getId()));
    Row row = rs.one();
    assertThat(row).isNotNull();
    Container actual = containerDao.getLenient(row);
    assertThat(actual.getId()).isEqualTo(SAMPLE_CONTAINER.getId());
    assertThat(actual.getList()).containsExactly(new Type1("a", null), new Type1("b", null));
    assertThat(actual.getMap1()).isNull();
    assertThat(actual.getMap2()).isNull();
    assertThat(actual.getMap3()).isNull();
  }

  @Test
  public void should_set_entity_on_partial_statement_builder_when_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps =
        session.prepare("INSERT INTO container_partial (id, list) VALUES (?, ?)");
    BoundStatementBuilder builder = ps.boundStatementBuilder();
    containerDao.setLenient(SAMPLE_CONTAINER, builder);
    assertThat(builder.getUuid(0)).isEqualTo(SAMPLE_CONTAINER.getId());
    assertThat(builder.getList(1, UdtValue.class)).hasSize(2);
  }

  @Test
  public void should_not_set_entity_on_partial_statement_builder_when_not_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps = session.prepare("INSERT INTO container (id, list) VALUES (?, ?)");
    Throwable error =
        catchThrowable(() -> containerDao.set(SAMPLE_CONTAINER, ps.boundStatementBuilder()));
    assertThat(error).hasMessage("map1 is not a variable in this bound statement");
  }

  @Mapper
  public interface UdtsMapper {
    @DaoFactory
    ContainerDao containerDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ContainerDao {

    @Select
    Container loadByPk(UUID id);

    @Insert
    void save(Container container);

    @Insert(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void saveDoNotSetNull(Container container);

    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void saveSetToNull(Container container);

    @GetEntity
    Container get(Row source);

    @GetEntity(lenient = true)
    Container getLenient(Row source);

    @SetEntity
    void set(Container container, BoundStatementBuilder target);

    @SetEntity(lenient = true)
    void setLenient(Container container, BoundStatementBuilder target);
  }

  @Entity
  public static class Container {

    @PartitionKey private UUID id;
    private List<Type1> list;
    private Map<String, List<Type1>> map1;
    private Map<Type1, Set<List<Type2>>> map2;
    private Map<Type1, Map<String, Set<Type2>>> map3;

    public Container() {}

    public Container(
        UUID id,
        List<Type1> list,
        Map<String, List<Type1>> map1,
        Map<Type1, Set<List<Type2>>> map2,
        Map<Type1, Map<String, Set<Type2>>> map3) {
      this.id = id;
      this.list = list;
      this.map1 = map1;
      this.map2 = map2;
      this.map3 = map3;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public List<Type1> getList() {
      return list;
    }

    public void setList(List<Type1> list) {
      this.list = list;
    }

    public Map<String, List<Type1>> getMap1() {
      return map1;
    }

    public void setMap1(Map<String, List<Type1>> map1) {
      this.map1 = map1;
    }

    public Map<Type1, Set<List<Type2>>> getMap2() {
      return map2;
    }

    public void setMap2(Map<Type1, Set<List<Type2>>> map2) {
      this.map2 = map2;
    }

    public Map<Type1, Map<String, Set<Type2>>> getMap3() {
      return map3;
    }

    public void setMap3(Map<Type1, Map<String, Set<Type2>>> map3) {
      this.map3 = map3;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Container) {
        Container that = (Container) other;
        return Objects.equals(this.id, that.id)
            && Objects.equals(this.list, that.list)
            && Objects.equals(this.map1, that.map1)
            && Objects.equals(this.map2, that.map2)
            && Objects.equals(this.map3, that.map3);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, list, map1, map2, map3);
    }
  }

  @Entity
  public static class Type1 {
    private String s1;
    private String s2;

    public Type1() {}

    public Type1(String s1, String s2) {
      this.s1 = s1;
      this.s2 = s2;
    }

    public String getS1() {
      return s1;
    }

    public void setS1(String s1) {
      this.s1 = s1;
    }

    public String getS2() {
      return s2;
    }

    public void setS2(String s2) {
      this.s2 = s2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Type1)) {
        return false;
      }
      Type1 type1 = (Type1) o;
      return Objects.equals(s1, type1.s1) && Objects.equals(s2, type1.s2);
    }

    @Override
    public int hashCode() {
      return Objects.hash(s1, s2);
    }
  }

  @Entity
  public static class Type2 {
    private int i1;
    private int i2;

    public Type2() {}

    public Type2(int i1, int i2) {
      this.i1 = i1;
      this.i2 = i2;
    }

    public int getI1() {
      return i1;
    }

    public void setI1(int i1) {
      this.i1 = i1;
    }

    public int getI2() {
      return i2;
    }

    public void setI2(int i2) {
      this.i2 = i2;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Type2)) {
        return false;
      }
      Type2 type2 = (Type2) o;
      return i1 == type2.i1 && i2 == type2.i2;
    }

    @Override
    public int hashCode() {
      return Objects.hash(i1, i2);
    }
  }
}
