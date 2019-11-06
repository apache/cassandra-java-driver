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
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(
    min = "3.0",
    description = ">= in WHERE clause not supported in legacy versions")
public class DeleteIT extends InventoryITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ProductDao dao;

  private static ProductSaleDao saleDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query : createStatements(CCM_RULE)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new DeleteIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(SESSION_RULE.keyspace());
    saleDao = inventoryMapper.productSaleDao(SESSION_RULE.keyspace());
  }

  @Before
  public void insertFixtures() {
    dao.save(FLAMETHROWER);

    saleDao.save(FLAMETHROWER_SALE_1);
    saleDao.save(FLAMETHROWER_SALE_2);
    saleDao.save(FLAMETHROWER_SALE_3);
    saleDao.save(FLAMETHROWER_SALE_4);
    saleDao.save(FLAMETHROWER_SALE_5);
    saleDao.save(MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_entity() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    dao.delete(FLAMETHROWER);
    assertThat(dao.findById(id)).isNull();
  }

  @Test
  public void should_delete_entity_asynchronously() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    CompletableFutures.getUninterruptibly(dao.deleteAsync(FLAMETHROWER));
    assertThat(dao.findById(id)).isNull();
  }

  @Test
  public void should_delete_by_id() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    dao.deleteById(id);
    assertThat(dao.findById(id)).isNull();

    // Non-existing id should be silently ignored
    dao.deleteById(id);
  }

  @Test
  public void should_delete_by_id_asynchronously() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    CompletableFutures.getUninterruptibly(dao.deleteAsyncById(id));
    assertThat(dao.findById(id)).isNull();

    // Non-existing id should be silently ignored
    CompletableFutures.getUninterruptibly(dao.deleteAsyncById(id));
  }

  @Test
  public void should_delete_if_exists() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    assertThat(dao.deleteIfExists(FLAMETHROWER)).isTrue();
    assertThat(dao.findById(id)).isNull();

    assertThat(dao.deleteIfExists(FLAMETHROWER)).isFalse();
  }

  @Test
  public void should_delete_if_exists_asynchronously() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    assertThat(CompletableFutures.getUninterruptibly(dao.deleteAsyncIfExists(FLAMETHROWER)))
        .isTrue();
    assertThat(dao.findById(id)).isNull();

    assertThat(CompletableFutures.getUninterruptibly(dao.deleteAsyncIfExists(FLAMETHROWER)))
        .isFalse();
  }

  @Test
  public void should_delete_with_condition() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    ResultSet rs = dao.deleteIfDescriptionMatches(id, "foo");
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.one().getString("description")).isEqualTo(FLAMETHROWER.getDescription());

    rs = dao.deleteIfDescriptionMatches(id, FLAMETHROWER.getDescription());
    assertThat(rs.wasApplied()).isTrue();
    assertThat(dao.findById(id)).isNull();
  }

  @Test
  public void should_delete_with_condition_statement() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    BoundStatement bs = dao.deleteIfDescriptionMatchesStatement(id, "foo");
    ResultSet rs = SESSION_RULE.session().execute(bs);
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.one().getString("description")).isEqualTo(FLAMETHROWER.getDescription());

    rs = dao.deleteIfDescriptionMatches(id, FLAMETHROWER.getDescription());
    assertThat(rs.wasApplied()).isTrue();
    assertThat(dao.findById(id)).isNull();
  }

  @Test
  public void should_delete_with_condition_asynchronously() {
    UUID id = FLAMETHROWER.getId();
    assertThat(dao.findById(id)).isNotNull();

    AsyncResultSet rs =
        CompletableFutures.getUninterruptibly(dao.deleteAsyncIfDescriptionMatches(id, "foo"));
    assertThat(rs.wasApplied()).isFalse();
    assertThat(rs.one().getString("description")).isEqualTo(FLAMETHROWER.getDescription());

    rs =
        CompletableFutures.getUninterruptibly(
            dao.deleteAsyncIfDescriptionMatches(id, FLAMETHROWER.getDescription()));
    assertThat(rs.wasApplied()).isTrue();
    assertThat(dao.findById(id)).isNull();
  }

  @Test
  public void should_delete_by_partition_key() {
    // should delete FLAMETHROWER_SALE_[1-4]
    saleDao.deleteByIdForDay(FLAMETHROWER.getId(), DATE_1);
    assertThat(saleDao.all().all()).containsOnly(FLAMETHROWER_SALE_5, MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_by_partition_key_statement() {
    // should delete FLAMETHROWER_SALE_[1-4]
    SESSION_RULE.session().execute(saleDao.deleteByIdForDayStatement(FLAMETHROWER.getId(), DATE_1));
    assertThat(saleDao.all().all()).containsOnly(FLAMETHROWER_SALE_5, MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_by_partition_key_and_partial_clustering() {
    // should delete FLAMETHROWER_SALE_{1,3,4]
    saleDao.deleteByIdForCustomer(FLAMETHROWER.getId(), DATE_1, 1);
    assertThat(saleDao.all().all())
        .containsOnly(FLAMETHROWER_SALE_2, FLAMETHROWER_SALE_5, MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_by_partition_key_and_partial_clustering_statement() {
    // should delete FLAMETHROWER_SALE_{1,3,4]
    SESSION_RULE
        .session()
        .execute(saleDao.deleteByIdForCustomerStatement(FLAMETHROWER.getId(), DATE_1, 1));
    assertThat(saleDao.all().all())
        .containsOnly(FLAMETHROWER_SALE_2, FLAMETHROWER_SALE_5, MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_by_primary_key_sales() {
    // should delete FLAMETHROWER_SALE_2
    saleDao.deleteByIdForCustomerAtTime(
        FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs());
    assertThat(saleDao.all().all())
        .containsOnly(
            FLAMETHROWER_SALE_1,
            FLAMETHROWER_SALE_3,
            FLAMETHROWER_SALE_4,
            FLAMETHROWER_SALE_5,
            MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_by_primary_key_sales_statement() {
    // should delete FLAMETHROWER_SALE_2
    SESSION_RULE
        .session()
        .execute(
            saleDao.deleteByIdForCustomerAtTimeStatement(
                FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs()));
    assertThat(saleDao.all().all())
        .containsOnly(
            FLAMETHROWER_SALE_1,
            FLAMETHROWER_SALE_3,
            FLAMETHROWER_SALE_4,
            FLAMETHROWER_SALE_5,
            MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_if_price_matches() {
    ResultSet result =
        saleDao.deleteIfPriceMatches(
            FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs(), 250.0);

    assertThat(result.wasApplied()).isFalse();
    Row row = result.one();
    assertThat(row).isNotNull();
    assertThat(row.getDouble("price")).isEqualTo(500.0);

    result =
        saleDao.deleteIfPriceMatches(
            FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs(), 500.0);

    assertThat(result.wasApplied()).isTrue();
  }

  @Test
  public void should_delete_if_price_matchesStatement() {
    BoundStatement bs =
        saleDao.deleteIfPriceMatchesStatement(
            FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs(), 250.0);
    ResultSet result = SESSION_RULE.session().execute(bs);

    assertThat(result.wasApplied()).isFalse();
    Row row = result.one();
    assertThat(row).isNotNull();
    assertThat(row.getDouble("price")).isEqualTo(500.0);

    bs =
        saleDao.deleteIfPriceMatchesStatement(
            FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs(), 500.0);
    result = SESSION_RULE.session().execute(bs);

    assertThat(result.wasApplied()).isTrue();
  }

  @Test
  public void should_delete_if_exists_sales() {
    assertThat(saleDao.deleteIfExists(FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs()))
        .isTrue();

    assertThat(saleDao.deleteIfExists(FLAMETHROWER.getId(), DATE_1, 2, FLAMETHROWER_SALE_2.getTs()))
        .isFalse();
  }

  @Test
  public void should_delete_within_time_range() {
    // should delete FLAMETHROWER_SALE_{1,3}, but not 4 because range ends before
    saleDao.deleteInTimeRange(
        FLAMETHROWER.getId(),
        DATE_1,
        1,
        FLAMETHROWER_SALE_1.getTs(),
        Uuids.startOf(Uuids.unixTimestamp(FLAMETHROWER_SALE_4.getTs()) - 1000));

    assertThat(saleDao.all().all())
        .containsOnly(
            FLAMETHROWER_SALE_2, FLAMETHROWER_SALE_4, FLAMETHROWER_SALE_5, MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_within_time_range_statement() {
    // should delete FLAMETHROWER_SALE_{1,3}, but not 4 because range ends before
    SESSION_RULE
        .session()
        .execute(
            saleDao.deleteInTimeRangeStatement(
                FLAMETHROWER.getId(),
                DATE_1,
                1,
                FLAMETHROWER_SALE_1.getTs(),
                Uuids.startOf(Uuids.unixTimestamp(FLAMETHROWER_SALE_4.getTs()) - 1000)));

    assertThat(saleDao.all().all())
        .containsOnly(
            FLAMETHROWER_SALE_2, FLAMETHROWER_SALE_4, FLAMETHROWER_SALE_5, MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_delete_if_price_matches_custom_where() {
    ResultSet result =
        saleDao.deleteCustomWhereCustomIf(
            2, FLAMETHROWER.getId(), DATE_1, FLAMETHROWER_SALE_2.getTs(), 250.0);

    assertThat(result.wasApplied()).isFalse();
    Row row = result.one();
    assertThat(row).isNotNull();
    assertThat(row.getDouble("price")).isEqualTo(500.0);

    result =
        saleDao.deleteCustomWhereCustomIf(
            2, FLAMETHROWER.getId(), DATE_1, FLAMETHROWER_SALE_2.getTs(), 500.0);

    assertThat(result.wasApplied()).isTrue();
  }

  @Test
  public void should_delete_if_price_matches_custom_where_statement() {
    BoundStatement bs =
        saleDao.deleteCustomWhereCustomIfStatement(
            2, FLAMETHROWER.getId(), DATE_1, FLAMETHROWER_SALE_2.getTs(), 250.0);
    ResultSet result = SESSION_RULE.session().execute(bs);

    assertThat(result.wasApplied()).isFalse();
    Row row = result.one();
    assertThat(row).isNotNull();
    assertThat(row.getDouble("price")).isEqualTo(500.0);

    bs =
        saleDao.deleteCustomWhereCustomIfStatement(
            2, FLAMETHROWER.getId(), DATE_1, FLAMETHROWER_SALE_2.getTs(), 500.0);
    result = SESSION_RULE.session().execute(bs);

    assertThat(result.wasApplied()).isTrue();
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSaleDao productSaleDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductDao {

    @Delete
    void delete(Product product);

    @Delete(entityClass = Product.class)
    void deleteById(UUID productId);

    @Delete(ifExists = true)
    boolean deleteIfExists(Product product);

    @Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
    ResultSet deleteIfDescriptionMatches(UUID productId, String expectedDescription);

    @Delete(entityClass = Product.class, customIfClause = "description = :expectedDescription")
    BoundStatement deleteIfDescriptionMatchesStatement(UUID productId, String expectedDescription);

    @Delete
    CompletionStage<Void> deleteAsync(Product product);

    @Delete(entityClass = Product.class)
    CompletableFuture<Void> deleteAsyncById(UUID productId);

    @Delete(ifExists = true)
    CompletableFuture<Boolean> deleteAsyncIfExists(Product product);

    @Delete(entityClass = Product.class, customIfClause = "description = :\"ExpectedDescription\"")
    CompletableFuture<AsyncResultSet> deleteAsyncIfDescriptionMatches(
        UUID productId, @CqlName("\"ExpectedDescription\"") String expectedDescription);

    @Select
    Product findById(UUID productId);

    @Insert
    void save(Product product);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductSaleDao {
    @Delete
    void delete(ProductSale product);

    // delete all rows in partition
    @Delete(entityClass = ProductSale.class)
    ResultSet deleteByIdForDay(UUID id, String day);

    // delete all rows in partition
    @Delete(entityClass = ProductSale.class)
    BoundStatement deleteByIdForDayStatement(UUID id, String day);

    // delete by partition key and partial clustering key
    @Delete(entityClass = ProductSale.class)
    ResultSet deleteByIdForCustomer(UUID id, String day, int customerId);

    // delete by partition key and partial clustering key
    @Delete(entityClass = ProductSale.class)
    BoundStatement deleteByIdForCustomerStatement(UUID id, String day, int customerId);

    // delete row (full primary key)
    @Delete(entityClass = ProductSale.class)
    ResultSet deleteByIdForCustomerAtTime(UUID id, String day, int customerId, UUID ts);

    // delete row (full primary key)
    @Delete(entityClass = ProductSale.class)
    BoundStatement deleteByIdForCustomerAtTimeStatement(
        UUID id, String day, int customerId, UUID ts);

    @Delete(entityClass = ProductSale.class, customIfClause = "price = :expectedPrice")
    ResultSet deleteIfPriceMatches(
        UUID id, String day, int customerId, UUID ts, double expectedPrice);

    @Delete(entityClass = ProductSale.class, customIfClause = "price = :expectedPrice")
    BoundStatement deleteIfPriceMatchesStatement(
        UUID id, String day, int customerId, UUID ts, double expectedPrice);

    @Delete(
        entityClass = ProductSale.class,
        customWhereClause =
            "id = :id and day = :day and customer_id = :customerId and ts >= :startTs and ts < "
                + ":endTs")
    ResultSet deleteInTimeRange(UUID id, String day, int customerId, UUID startTs, UUID endTs);

    @Delete(
        entityClass = ProductSale.class,
        customWhereClause =
            "id = :id and day = :day and customer_id = :customerId and ts >= :startTs and ts < "
                + ":endTs")
    BoundStatement deleteInTimeRangeStatement(
        UUID id, String day, int customerId, UUID startTs, UUID endTs);

    // transpose order of parameters so doesn't match primary key to ensure that works.
    @Delete(
        entityClass = ProductSale.class,
        customWhereClause = "id = :id and day = :day and customer_id = :customerId and ts = :ts",
        customIfClause = "price = :expectedPrice")
    ResultSet deleteCustomWhereCustomIf(
        int customerId, UUID id, String day, UUID ts, double expectedPrice);

    // transpose order of parameters so doesn't match primary key to ensure that works.
    @Delete(
        entityClass = ProductSale.class,
        customWhereClause = "id = :id and day = :day and customer_id = :customerId and ts = :ts",
        customIfClause = "price = :expectedPrice")
    BoundStatement deleteCustomWhereCustomIfStatement(
        int customerId, UUID id, String day, UUID ts, double expectedPrice);

    @Delete(entityClass = ProductSale.class, ifExists = true)
    boolean deleteIfExists(UUID id, String day, int customerId, UUID ts);

    @Select
    PagingIterable<ProductSale> all();

    @Insert
    void save(ProductSale sale);
  }
}
