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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class QueryIT {
  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static TestMapper mapper;
  /** A DAO connected to the default keyspace of the session rule. */
  private static TestDao defaultDao;

  private static final CqlIdentifier TABLE_ID = CqlIdentifier.fromCql("test_entity");
  private static final CqlIdentifier ALTERNATE_KEYSPACE =
      CqlIdentifier.fromCql(QueryIT.class.getSimpleName() + "_alt");

  @BeforeClass
  public static void createSchema() {
    CqlSession session = sessionRule.session();

    for (String query :
        ImmutableList.of(
            String.format(
                "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                ALTERNATE_KEYSPACE.asCql(false)),
            "CREATE TABLE test_entity(id int, rank int, value int, PRIMARY KEY(id, rank))",
            String.format(
                "CREATE TABLE %s.test_entity(id int, rank int, value int, PRIMARY KEY(id, rank))",
                ALTERNATE_KEYSPACE.asCql(false)))) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    mapper = new QueryIT_TestMapperBuilder(session).build();
    defaultDao = mapper.dao(sessionRule.keyspace(), TABLE_ID);
  }

  @Before
  public void insertData() {
    for (TestDao dao : ImmutableList.of(defaultDao, mapper.dao(ALTERNATE_KEYSPACE, TABLE_ID))) {
      for (int i = 0; i < 10; i++) {
        dao.insert(new TestEntity(1, i, i));
      }
    }
  }

  @Test
  public void should_not_use_keyspace_in_qualifiedTableId_when_dao_does_not_specify_one() {
    TestDao dao = mapper.dao(null, TABLE_ID);
    // The query uses ${qualifiedTableId} but the DAO doesn't specify a keyspace, so the table
    // will be unqualified in the query. This still works since the session has a default keyspace.
    dao.deleteWithQualifiedTableId(1, 1);

    assertThat(defaultDao.findByIdAndRank(1, 1)).isNull();
  }

  @Test
  public void should_fail_if_query_string_uses_qualifiedTableId_but_dao_does_not_specify_table() {
    TestDao dao = mapper.dao();
    // The query uses ${qualifiedTableId}. The DAO doesn't specify a table, and we can't infer an
    // entity class from the method signature.
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Can't use ${tableId} or ${qualifiedTableId} in @Query method if it doesn't "
            + "return an entity class and the DAO wasn't built with a table");
    dao.deleteWithQualifiedTableId(1, 1);
  }

  @Test
  public void should_fail_if_query_string_uses_keyspaceId_but_dao_does_not_specify_keyspace() {
    TestDao dao = mapper.dao();
    // The query uses ${keyspaceId} but the DAO was not built with the keyspace, there's no way to
    // infer a keyspace at DAO construction time.
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Can't use ${keyspaceId} in @Query method if the DAO wasn't built with a keyspace");
    dao.deleteWithKeyspaceIdAndTableId(1, 1);
  }

  @Test
  public void should_execute_query_and_map_to_void() {
    defaultDao.deleteWithQualifiedTableId(1, 1);
    assertThat(defaultDao.findByIdAndRank(1, 1)).isNull();
  }

  @Test
  public void should_execute_async_query_and_map_to_void() {
    CompletableFutures.getUninterruptibly(defaultDao.deleteAsync(1, 1).toCompletableFuture());
    assertThat(defaultDao.findByIdAndRank(1, 1)).isNull();
  }

  @Test
  public void should_execute_conditional_query_and_map_to_boolean() {
    assertThat(defaultDao.deleteIfExists(1, 1)).isTrue();
    assertThat(defaultDao.deleteIfExists(1, 1)).isFalse();
  }

  @Test
  public void should_execute_async_conditional_query_and_map_to_boolean() {
    assertThat(CompletableFutures.getUninterruptibly(defaultDao.deleteIfExistsAsync(1, 1)))
        .isTrue();
    assertThat(CompletableFutures.getUninterruptibly(defaultDao.deleteIfExistsAsync(1, 1)))
        .isFalse();
  }

  @Test
  public void should_execute_count_query_and_map_to_long() {
    assertThat(defaultDao.countById(1)).isEqualTo(10);
  }

  @Test
  public void should_fail_to_map_to_long_if_query_returns_other_type() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "Expected the query to return a column with CQL type BIGINT in first position "
            + "(return type long is intended for COUNT queries)");
    defaultDao.wrongCount();
  }

  @Test
  public void should_execute_async_count_query_and_map_to_long() {
    assertThat(CompletableFutures.getUninterruptibly(defaultDao.countByIdAsync(1))).isEqualTo(10);
  }

  @Test
  public void should_execute_query_and_map_to_row() {
    Row row = defaultDao.findRowByIdAndRank(1, 1);
    assertThat(row).isNotNull();
    assertThat(row.getColumnDefinitions().size()).isEqualTo(3);
    assertThat(row.getInt("id")).isEqualTo(1);
    assertThat(row.getInt("rank")).isEqualTo(1);
    assertThat(row.getInt("value")).isEqualTo(1);
  }

  @Test
  public void should_execute_async_query_and_map_to_row() {
    Row row = CompletableFutures.getUninterruptibly(defaultDao.findRowByIdAndRankAsync(1, 1));
    assertThat(row).isNotNull();
    assertThat(row.getColumnDefinitions().size()).isEqualTo(3);
    assertThat(row.getInt("id")).isEqualTo(1);
    assertThat(row.getInt("rank")).isEqualTo(1);
    assertThat(row.getInt("value")).isEqualTo(1);
  }

  @Test
  public void should_execute_query_and_map_to_result_set() {
    ResultSet resultSet = defaultDao.findRowsById(1);
    assertThat(resultSet.all()).hasSize(10);
  }

  @Test
  public void should_execute_async_query_and_map_to_result_set() {
    AsyncResultSet resultSet =
        CompletableFutures.getUninterruptibly(defaultDao.findRowsByIdAsync(1));
    assertThat(ImmutableList.copyOf(resultSet.currentPage())).hasSize(10);
    assertThat(resultSet.hasMorePages()).isFalse();
  }

  @Test
  public void should_execute_query_and_map_to_entity() {
    TestEntity entity = defaultDao.findByIdAndRank(1, 1);
    assertThat(entity.getId()).isEqualTo(1);
    assertThat(entity.getRank()).isEqualTo(1);
    assertThat(entity.getValue()).isEqualTo(1);

    entity = defaultDao.findByIdAndRank(2, 1);
    assertThat(entity).isNull();
  }

  @Test
  public void should_execute_async_query_and_map_to_entity() {
    TestEntity entity =
        CompletableFutures.getUninterruptibly(defaultDao.findByIdAndRankAsync(1, 1));
    assertThat(entity.getId()).isEqualTo(1);
    assertThat(entity.getRank()).isEqualTo(1);
    assertThat(entity.getValue()).isEqualTo(1);

    entity = defaultDao.findByIdAndRank(2, 1);
    assertThat(entity).isNull();
  }

  @Test
  public void should_execute_query_and_map_to_optional_entity() {
    Optional<TestEntity> maybeEntity = defaultDao.findOptionalByIdAndRank(1, 1);
    assertThat(maybeEntity)
        .hasValueSatisfying(
            entity -> {
              assertThat(entity.getId()).isEqualTo(1);
              assertThat(entity.getRank()).isEqualTo(1);
              assertThat(entity.getValue()).isEqualTo(1);
            });

    maybeEntity = defaultDao.findOptionalByIdAndRank(2, 1);
    assertThat(maybeEntity).isEmpty();
  }

  @Test
  public void should_execute_async_query_and_map_to_optional_entity() {
    Optional<TestEntity> maybeEntity =
        CompletableFutures.getUninterruptibly(defaultDao.findOptionalByIdAndRankAsync(1, 1));
    assertThat(maybeEntity)
        .hasValueSatisfying(
            entity -> {
              assertThat(entity.getId()).isEqualTo(1);
              assertThat(entity.getRank()).isEqualTo(1);
              assertThat(entity.getValue()).isEqualTo(1);
            });

    maybeEntity = defaultDao.findOptionalByIdAndRank(2, 1);
    assertThat(maybeEntity).isEmpty();
  }

  @Test
  public void should_execute_query_and_map_to_iterable() {
    PagingIterable<TestEntity> iterable = defaultDao.findById(1);
    assertThat(iterable.all()).hasSize(10);
  }

  @Test
  public void should_execute_async_query_and_map_to_iterable() {
    MappedAsyncPagingIterable<TestEntity> iterable =
        CompletableFutures.getUninterruptibly(defaultDao.findByIdAsync(1));
    assertThat(ImmutableList.copyOf(iterable.currentPage())).hasSize(10);
    assertThat(iterable.hasMorePages()).isFalse();
  }

  @Dao
  public interface TestDao {
    @Insert
    void insert(TestEntity entity);

    @Query("DELETE FROM ${qualifiedTableId} WHERE id = :id and rank = :rank")
    void deleteWithQualifiedTableId(int id, int rank);

    @Query("DELETE FROM ${keyspaceId}.${tableId} WHERE id = :id and rank = :rank")
    void deleteWithKeyspaceIdAndTableId(int id, int rank);

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
    CompletableFuture<MappedAsyncPagingIterable<TestEntity>> findByIdAsync(int id);
  }

  @Entity
  public static class TestEntity {
    @PartitionKey private int id;

    @ClusteringColumn private int rank;

    private int value;

    public TestEntity() {}

    public TestEntity(int id, int rank, int value) {
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

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }

  @Mapper
  public interface TestMapper {
    @DaoFactory
    TestDao dao();

    @DaoFactory
    TestDao dao(@DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);
  }
}
