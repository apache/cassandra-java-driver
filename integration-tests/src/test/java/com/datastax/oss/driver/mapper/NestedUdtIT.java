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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
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
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/** Tests that entities with UDTs nested at various levels are properly mapped. */
@Category(ParallelizableTests.class)
@CassandraRequirement(min = "2.2", description = "support for unset values")
public class NestedUdtIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static UUID CONTAINER_ID = UUID.randomUUID();
  private static final Container SAMPLE_CONTAINER =
      new Container(
          CONTAINER_ID,
          ImmutableList.of(new Type1("a"), new Type1("b")),
          ImmutableMap.of(
              "cd",
              ImmutableList.of(new Type1("c"), new Type1("d")),
              "ef",
              ImmutableList.of(new Type1("e"), new Type1("f"))),
          ImmutableMap.of(
              new Type1("12"),
              ImmutableSet.of(ImmutableList.of(new Type2(1)), ImmutableList.of(new Type2(2)))),
          ImmutableMap.of(
              new Type1("12"), ImmutableMap.of("12", ImmutableSet.of(new Type2(1), new Type2(2)))));

  private static final Container SAMPLE_CONTAINER_NULL_LIST =
      new Container(
          CONTAINER_ID,
          null,
          ImmutableMap.of(
              "cd",
              ImmutableList.of(new Type1("c"), new Type1("d")),
              "ef",
              ImmutableList.of(new Type1("e"), new Type1("f"))),
          ImmutableMap.of(
              new Type1("12"),
              ImmutableSet.of(ImmutableList.of(new Type2(1)), ImmutableList.of(new Type2(2)))),
          ImmutableMap.of(
              new Type1("12"), ImmutableMap.of("12", ImmutableSet.of(new Type2(1), new Type2(2)))));

  private static ContainerDao containerDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query :
        ImmutableList.of(
            "CREATE TYPE type1(s text)",
            "CREATE TYPE type2(i int)",
            "CREATE TABLE container(id uuid PRIMARY KEY, "
                + "list frozen<list<type1>>, "
                + "map1 frozen<map<text, list<type1>>>, "
                + "map2 frozen<map<type1, set<list<type2>>>>,"
                + "map3 frozen<map<type1, map<text, set<type2>>>>"
                + ")")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    UdtsMapper udtsMapper = new NestedUdtIT_UdtsMapperBuilder(session).build();
    containerDao = udtsMapper.containerDao(sessionRule.keyspace());
  }

  @Before
  public void clearContainerData() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE container")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
  }

  @Test
  public void should_insert_and_retrieve_entity_with_nested_udts() {
    // Given
    CqlSession session = sessionRule.session();

    // When
    containerDao.save(SAMPLE_CONTAINER);
    Container retrievedEntity = containerDao.loadByPk(SAMPLE_CONTAINER.getId());

    // Then
    assertThat(retrievedEntity).isEqualTo(SAMPLE_CONTAINER);
  }

  @Test
  public void should_insert_do_not_set_to_null_udts() {
    // Given
    CqlSession session = sessionRule.session();

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
    CqlSession session = sessionRule.session();

    containerDao.save(SAMPLE_CONTAINER);
    Container retrievedEntity = containerDao.loadByPk(SAMPLE_CONTAINER.getId());

    assertThat(retrievedEntity.list).isNotNull();

    // When
    containerDao.saveSetToNull(SAMPLE_CONTAINER_NULL_LIST);
    Container retrievedEntitySecond = containerDao.loadByPk(SAMPLE_CONTAINER.getId());
    assertThat(retrievedEntitySecond.list).isEmpty();
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
    Container get(GettableByName source);
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
    private String s;

    public Type1() {}

    public Type1(String s) {
      this.s = s;
    }

    public String getS() {
      return s;
    }

    public void setS(String s) {
      this.s = s;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Type1) {
        Type1 that = (Type1) other;
        return Objects.equals(this.s, that.s);
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return s == null ? 0 : s.hashCode();
    }
  }

  @Entity
  public static class Type2 {
    private int i;

    public Type2() {}

    public Type2(int i) {
      this.i = i;
    }

    public int getI() {
      return i;
    }

    public void setI(int i) {
      this.i = i;
    }

    @Override
    public boolean equals(Object other) {
      if (other == this) {
        return true;
      } else if (other instanceof Type2) {
        Type2 that = (Type2) other;
        return this.i == that.i;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return i;
    }
  }
}
