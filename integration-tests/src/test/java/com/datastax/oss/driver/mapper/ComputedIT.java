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

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.data.Offset.offset;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Computed;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class ComputedIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestMapper mapper;

  private static AtomicInteger keyProvider = new AtomicInteger();

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query :
        ImmutableList.of(
            "CREATE TABLE computed_entity(id int, c_id int, v int, primary key (id, c_id))")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    mapper = new ComputedIT_TestMapperBuilder(session).build();
  }

  @Test
  public void should_not_include_computed_values_in_insert() {
    ComputedDao computedDao = mapper.computedDao(SESSION_RULE.keyspace());
    int key = keyProvider.incrementAndGet();

    ComputedEntity entity = new ComputedEntity(key, 1, 2);
    computedDao.save(entity);

    ComputedEntity retrievedValue = computedDao.findById(key, 1);
    assertThat(retrievedValue.getId()).isEqualTo(key);
    assertThat(retrievedValue.getcId()).isEqualTo(1);
    assertThat(retrievedValue.getV()).isEqualTo(2);
  }

  @Test
  public void should_return_computed_values_in_select() {
    ComputedDao computedDao = mapper.computedDao(SESSION_RULE.keyspace());
    int key = keyProvider.incrementAndGet();

    long time = System.currentTimeMillis() - 1000;
    ComputedEntity entity = new ComputedEntity(key, 1, 2);
    computedDao.saveWithTime(entity, 3600, time);

    ComputedEntity retrievedValue = computedDao.findById(key, 1);
    assertThat(retrievedValue.getId()).isEqualTo(key);
    assertThat(retrievedValue.getcId()).isEqualTo(1);
    assertThat(retrievedValue.getV()).isEqualTo(2);
    assertThat(retrievedValue.getTtl()).isCloseTo(3600, offset(10));
    assertThat(retrievedValue.getWritetime()).isEqualTo(time);
  }

  @Test
  public void should_not_include_computed_values_in_delete() {
    // should not be the case since delete operates on primary key..
    ComputedDao computedDao = mapper.computedDao(SESSION_RULE.keyspace());
    int key = keyProvider.incrementAndGet();

    ComputedEntity entity = new ComputedEntity(key, 1, 2);
    computedDao.save(entity);

    // retrieve values so computed values are present.
    ComputedEntity retrievedValue = computedDao.findById(key, 1);

    computedDao.delete(retrievedValue);

    assertThat(computedDao.findById(key, 1)).isNull();
  }

  @Test
  public void should_not_include_computed_values_in_SetEntity() {
    ComputedDao computedDao = mapper.computedDao(SESSION_RULE.keyspace());
    int key = keyProvider.incrementAndGet();

    CqlSession session = SESSION_RULE.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO computed_entity (id, c_id, v) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();
    ComputedEntity entity = new ComputedEntity(key, 1, 2);
    BoundStatement statement = computedDao.set(builder, entity).build();
    session.execute(statement);

    // retrieve values to ensure was successful.
    ComputedEntity retrievedValue = computedDao.findById(key, 1);

    assertThat(retrievedValue.getId()).isEqualTo(key);
    assertThat(retrievedValue.getcId()).isEqualTo(1);
    assertThat(retrievedValue.getV()).isEqualTo(2);
  }

  @Test
  public void should_return_computed_values_in_GetEntity() {
    ComputedDao computedDao = mapper.computedDao(SESSION_RULE.keyspace());
    int key = keyProvider.incrementAndGet();

    long time = System.currentTimeMillis() - 1000;
    ComputedEntity entity = new ComputedEntity(key, 1, 2);
    computedDao.saveWithTime(entity, 3600, time);

    CqlSession session = SESSION_RULE.session();

    /*
     * Query with the computed values included.
     *
     * Since the mapper expects the result name to match the property name, we used aliasing
     * here.
     *
     * In the case of ttl(v), since we annotated ttl as @CqlName("myttl") we expect myttl to
     * be the alias.
     */
    ResultSet result =
        session.execute(
            SimpleStatement.newInstance(
                "select id, c_id, v, writetime(v) as writetime, ttl(v) as myttl from "
                    + "computed_entity where "
                    + "id=? and "
                    + "c_id=? limit 1",
                key,
                1));
    assertThat(result.getAvailableWithoutFetching()).isEqualTo(1);

    ComputedEntity retrievedValue = computedDao.get(result.one());
    assertThat(retrievedValue.getId()).isEqualTo(key);
    assertThat(retrievedValue.getcId()).isEqualTo(1);
    assertThat(retrievedValue.getV()).isEqualTo(2);

    // these should be set
    assertThat(retrievedValue.getTtl()).isCloseTo(3600, offset(10));
    assertThat(retrievedValue.getWritetime()).isEqualTo(time);
  }

  @Test
  public void should_fail_if_alias_does_not_match_cqlName() {
    ComputedDao computedDao = mapper.computedDao(SESSION_RULE.keyspace());
    int key = keyProvider.incrementAndGet();

    long time = System.currentTimeMillis() - 1000;
    ComputedEntity entity = new ComputedEntity(key, 1, 2);
    computedDao.saveWithTime(entity, 3600, time);

    CqlSession session = SESSION_RULE.session();

    /*
     * Query with the computed values included.
     *
     * Since the mapper expects the result name to match the property name, we used aliasing
     * here and used the wrong name for the alias 'notwritetime' which does not map to the cqlName.
     */
    ResultSet result =
        session.execute(
            SimpleStatement.newInstance(
                "select id, c_id, v, writetime(v) as notwritetime, ttl(v) as myttl from "
                    + "computed_entity where "
                    + "id=? and "
                    + "c_id=? limit 1",
                key,
                1));

    // should raise an exception as 'writetime' is not found in result set.
    Throwable t = catchThrowable(() -> computedDao.get(result.one()));

    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("writetime is not a column in this row");
  }

  @Test
  public void should_return_computed_values_in_query() {
    ComputedDao computedDao = mapper.computedDao(SESSION_RULE.keyspace());
    int key = keyProvider.incrementAndGet();

    long time = System.currentTimeMillis() - 1000;
    ComputedEntity entity = new ComputedEntity(key, 1, 2);
    computedDao.saveWithTime(entity, 3600, time);

    ComputedEntity retrievedValue = computedDao.findByIdQuery(key, 1);
    assertThat(retrievedValue.getId()).isEqualTo(key);
    assertThat(retrievedValue.getcId()).isEqualTo(1);
    assertThat(retrievedValue.getV()).isEqualTo(2);

    // these should be set
    assertThat(retrievedValue.getTtl()).isCloseTo(3600, offset(10));
    assertThat(retrievedValue.getWritetime()).isEqualTo(time);
  }

  @Entity
  public static class ComputedEntity {

    @PartitionKey private int id;

    @ClusteringColumn private int cId;

    private int v;

    @Computed("writetime(v)")
    private long writetime;

    // use CqlName to ensure it is used for the alias.
    @CqlName("myttl")
    @Computed("ttl(v)")
    private int ttl;

    public ComputedEntity() {}

    public ComputedEntity(int id, int cId, int v) {
      this.id = id;
      this.cId = cId;
      this.v = v;
    }

    public int getId() {
      return id;
    }

    public void setId(int id) {
      this.id = id;
    }

    public int getcId() {
      return cId;
    }

    public void setcId(int cId) {
      this.cId = cId;
    }

    public int getV() {
      return v;
    }

    public void setV(int v) {
      this.v = v;
    }

    public long getWritetime() {
      return writetime;
    }

    public void setWritetime(long writetime) {
      this.writetime = writetime;
    }

    public int getTtl() {
      return ttl;
    }

    public void setTtl(int ttl) {
      this.ttl = ttl;
    }
  }

  @Dao
  public interface ComputedDao {
    @Select
    ComputedEntity findById(int id, int cId);

    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void save(ComputedEntity entity);

    @Insert(
        ttl = ":ttl",
        timestamp = ":writeTime",
        nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void saveWithTime(ComputedEntity entity, int ttl, long writeTime);

    @Delete
    void delete(ComputedEntity entity);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    BoundStatementBuilder set(BoundStatementBuilder builder, ComputedEntity computedEntity);

    @GetEntity
    ComputedEntity get(Row row);

    @Query(
        value =
            "select id, c_id, v, ttl(v) as myttl, writetime(v) as writetime from "
                + "${qualifiedTableId} WHERE id = :id and "
                + "c_id = :cId",
        nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    ComputedEntity findByIdQuery(int id, int cId);

    @Update(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void update(ComputedEntity entity);
  }

  @Mapper
  public interface TestMapper {
    @DaoFactory
    ComputedDao computedDao(@DaoKeyspace CqlIdentifier keyspace);
  }
}
