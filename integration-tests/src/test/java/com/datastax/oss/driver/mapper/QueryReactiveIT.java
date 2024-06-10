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

import com.datastax.dse.driver.api.core.cql.reactive.ReactiveResultSet;
import com.datastax.dse.driver.api.mapper.reactive.MappedReactiveResultSet;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import io.reactivex.Flowable;
import java.util.List;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class QueryReactiveIT {

  private static CcmRule ccmRule = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccmRule).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  private static DseTestDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE test_entity(id int, rank int, value int, PRIMARY KEY(id, rank))")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    TestMapper testMapper = new QueryReactiveIT_TestMapperBuilder(session).build();
    dao = testMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void insertData() {
    for (int i = 0; i < 10; i++) {
      dao.insert(new TestEntity(1, i, i));
    }
  }

  @Test
  public void should_query_reactive() {
    ReactiveResultSet rs = dao.findByIdReactive(1);
    assertThat(Flowable.fromPublisher(rs).count().blockingGet()).isEqualTo(10);
  }

  @Test
  public void should_query_reactive_mapped() {
    MappedReactiveResultSet<TestEntity> rs = dao.findByIdReactiveMapped(1);
    List<TestEntity> results = Flowable.fromPublisher(rs).toList().blockingGet();
    assertThat(results).hasSize(10);
    assertThat(results).extracting("rank").containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Mapper
  public interface TestMapper {

    @DaoFactory
    DseTestDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface DseTestDao {

    @Insert
    void insert(TestEntity entity);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id")
    MappedReactiveResultSet<TestEntity> findByIdReactiveMapped(int id);

    @Query("SELECT * FROM ${keyspaceId}.test_entity WHERE id = :id")
    ReactiveResultSet findByIdReactive(int id);
  }

  @Entity
  public static class TestEntity {
    @PartitionKey private int id;

    @ClusteringColumn private int rank;

    private Integer value;

    public TestEntity() {}

    public TestEntity(int id, int rank, Integer value) {
      this.id = id;
      this.rank = rank;
      this.value = value;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getRank() {
      return rank;
    }

    public void setRank(int rank) {
      this.rank = rank;
    }

    public Integer getValue() {
      return value;
    }

    public void setValue(Integer value) {
      this.value = value;
    }
  }
}
