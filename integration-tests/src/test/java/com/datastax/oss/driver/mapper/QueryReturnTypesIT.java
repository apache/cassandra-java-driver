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
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
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
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/** Covers the return types of {@link Query} methods. */
@Category(ParallelizableTests.class)
public class QueryReturnTypesIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestDao dao;

  @BeforeClass
  public static void createSchema() {
    CqlSession session = SESSION_RULE.session();

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE test_entity(id int, rank int, value int, PRIMARY KEY(id, rank))")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());

    TestMapper mapper = new QueryReturnTypesIT_TestMapperBuilder(session).build();
    dao = mapper.dao(SESSION_RULE.keyspace(), CqlIdentifier.fromCql("test_entity"));
  }

  @Before
  public void insertData() {
    for (int i = 0; i < 10; i++) {
      dao.insert(new TestEntity(1, i, i));
    }
  }

  @Test
  public void should_execute_query_and_map_to_void() {
    dao.delete(1, 1);
    assertThat(dao.findByIdAndRank(1, 1)).isNull();
  }

  @Test
  public void should_execute_async_query_and_map_to_void() {
    CompletableFutures.getUninterruptibly(dao.deleteAsync(1, 1).toCompletableFuture());
    assertThat(dao.findByIdAndRank(1, 1)).isNull();
  }

  @Test
  public void should_execute_conditional_query_and_map_to_boolean() {
    assertThat(dao.deleteIfExists(1, 1)).isTrue();
    assertThat(dao.deleteIfExists(1, 1)).isFalse();
  }

  @Test
  public void should_execute_async_conditional_query_and_map_to_boolean() {
    assertThat(CompletableFutures.getUninterruptibly(dao.deleteIfExistsAsync(1, 1))).isTrue();
    assertThat(CompletableFutures.getUninterruptibly(dao.deleteIfExistsAsync(1, 1))).isFalse();
  }

  @Test
  public void should_execute_count_query_and_map_to_long() {
    assertThat(dao.countById(1)).isEqualTo(10);
  }

  @Test
  public void should_fail_to_map_to_long_if_query_returns_other_type() {
    Throwable t = catchThrowable(() -> dao.wrongCount());
    assertThat(t)
        .isInstanceOf(MapperException.class)
        .hasMessage(
            "Expected the query to return a column with CQL type BIGINT in first position "
                + "(return type long is intended for COUNT queries)");
  }

  @Test
  public void should_execute_async_count_query_and_map_to_long() {
    assertThat(CompletableFutures.getUninterruptibly(dao.countByIdAsync(1))).isEqualTo(10);
  }

  @Test
  public void should_execute_query_and_map_to_row() {
    Row row = dao.findRowByIdAndRank(1, 1);
    assertThat(row).isNotNull();
    assertThat(row.getColumnDefinitions().size()).isEqualTo(3);
    assertThat(row.getInt("id")).isEqualTo(1);
    assertThat(row.getInt("rank")).isEqualTo(1);
    assertThat(row.getInt("value")).isEqualTo(1);
  }

  @Test
  public void should_execute_async_query_and_map_to_row() {
    Row row = CompletableFutures.getUninterruptibly(dao.findRowByIdAndRankAsync(1, 1));
    assertThat(row).isNotNull();
    assertThat(row.getColumnDefinitions().size()).isEqualTo(3);
    assertThat(row.getInt("id")).isEqualTo(1);
    assertThat(row.getInt("rank")).isEqualTo(1);
    assertThat(row.getInt("value")).isEqualTo(1);
  }

  @Test
  public void should_execute_query_and_map_to_result_set() {
    ResultSet resultSet = dao.findRowsById(1);
    assertThat(resultSet.all()).hasSize(10);
  }

  @Test
  public void should_execute_async_query_and_map_to_result_set() {
    AsyncResultSet resultSet = CompletableFutures.getUninterruptibly(dao.findRowsByIdAsync(1));
    assertThat(ImmutableList.copyOf(resultSet.currentPage())).hasSize(10);
    assertThat(resultSet.hasMorePages()).isFalse();
  }

  @Test
  public void should_execute_query_and_map_to_entity() {
    TestEntity entity = dao.findByIdAndRank(1, 1);
    assertThat(entity.getId()).isEqualTo(1);
    assertThat(entity.getRank()).isEqualTo(1);
    assertThat(entity.getValue()).isEqualTo(1);

    entity = dao.findByIdAndRank(2, 1);
    assertThat(entity).isNull();
  }

  @Test
  public void should_execute_async_query_and_map_to_entity() {
    TestEntity entity = CompletableFutures.getUninterruptibly(dao.findByIdAndRankAsync(1, 1));
    assertThat(entity.getId()).isEqualTo(1);
    assertThat(entity.getRank()).isEqualTo(1);
    assertThat(entity.getValue()).isEqualTo(1);

    entity = dao.findByIdAndRank(2, 1);
    assertThat(entity).isNull();
  }

  @Test
  public void should_execute_query_and_map_to_optional_entity() {
    Optional<TestEntity> maybeEntity = dao.findOptionalByIdAndRank(1, 1);
    assertThat(maybeEntity)
        .hasValueSatisfying(
            entity -> {
              assertThat(entity.getId()).isEqualTo(1);
              assertThat(entity.getRank()).isEqualTo(1);
              assertThat(entity.getValue()).isEqualTo(1);
            });

    maybeEntity = dao.findOptionalByIdAndRank(2, 1);
    assertThat(maybeEntity).isEmpty();
  }

  @Test
  public void should_execute_async_query_and_map_to_optional_entity() {
    Optional<TestEntity> maybeEntity =
        CompletableFutures.getUninterruptibly(dao.findOptionalByIdAndRankAsync(1, 1));
    assertThat(maybeEntity)
        .hasValueSatisfying(
            entity -> {
              assertThat(entity.getId()).isEqualTo(1);
              assertThat(entity.getRank()).isEqualTo(1);
              assertThat(entity.getValue()).isEqualTo(1);
            });

    maybeEntity = dao.findOptionalByIdAndRank(2, 1);
    assertThat(maybeEntity).isEmpty();
  }

  @Test
  public void should_execute_query_and_map_to_iterable() {
    PagingIterable<TestEntity> iterable = dao.findById(1);
    assertThat(iterable.all()).hasSize(10);
  }

  @Test
  public void should_execute_query_and_map_to_stream() {
    Stream<TestEntity> stream = dao.findByIdAsStream(1);
    assertThat(stream).hasSize(10);
  }

  @Test
  public void should_execute_async_query_and_map_to_iterable() {
    MappedAsyncPagingIterable<TestEntity> iterable =
        CompletableFutures.getUninterruptibly(dao.findByIdAsync(1));
    assertThat(ImmutableList.copyOf(iterable.currentPage())).hasSize(10);
    assertThat(iterable.hasMorePages()).isFalse();
  }

  @Test
  public void should_execute_query_and_map_to_stream_async()
      throws ExecutionException, InterruptedException {
    CompletableFuture<Stream<TestEntity>> stream = dao.findByIdAsStreamAsync(1);
    assertThat(stream.get()).hasSize(10);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface TestDao {
    @Insert
    void insert(TestEntity entity);

    @Query("DELETE FROM ${qualifiedTableId} WHERE id = :id and rank = :rank")
    void delete(int id, int rank);

    @Query("DELETE FROM ${qualifiedTableId} WHERE id = :id and rank = :rank")
    CompletionStage<Void> deleteAsync(int id, int rank);

    @Query("DELETE FROM ${qualifiedTableId} WHERE id = :id and rank = :rank IF EXISTS")
    boolean deleteIfExists(int id, int rank);

    @Query("DELETE FROM ${qualifiedTableId} WHERE id = :id and rank = :rank IF EXISTS")
    CompletableFuture<Boolean> deleteIfExistsAsync(int id, int rank);

    @Query("SELECT count(*) FROM ${qualifiedTableId} WHERE id = :id")
    long countById(int id);

    @Query("SELECT count(*) FROM ${qualifiedTableId} WHERE id = :id")
    CompletableFuture<Long> countByIdAsync(int id);

    // Error: the query does not return a long as the first column
    @Query("SELECT release_version FROM system.local WHERE key='local'")
    @SuppressWarnings("UnusedReturnValue")
    long wrongCount();

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id AND rank = :rank")
    Row findRowByIdAndRank(int id, int rank);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id AND rank = :rank")
    CompletableFuture<Row> findRowByIdAndRankAsync(int id, int rank);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id")
    ResultSet findRowsById(int id);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id")
    CompletableFuture<AsyncResultSet> findRowsByIdAsync(int id);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id AND rank = :rank")
    TestEntity findByIdAndRank(int id, int rank);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id AND rank = :rank")
    CompletableFuture<TestEntity> findByIdAndRankAsync(int id, int rank);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id AND rank = :rank")
    Optional<TestEntity> findOptionalByIdAndRank(int id, int rank);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id AND rank = :rank")
    CompletableFuture<Optional<TestEntity>> findOptionalByIdAndRankAsync(int id, int rank);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id")
    PagingIterable<TestEntity> findById(int id);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id")
    Stream<TestEntity> findByIdAsStream(int id);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id")
    CompletableFuture<MappedAsyncPagingIterable<TestEntity>> findByIdAsync(int id);

    @Query("SELECT * FROM ${qualifiedTableId} WHERE id = :id")
    CompletableFuture<Stream<TestEntity>> findByIdAsStreamAsync(int id);
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

  @Mapper
  public interface TestMapper {
    @DaoFactory
    TestDao dao(@DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);
  }
}
