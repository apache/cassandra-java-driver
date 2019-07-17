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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SelectIT extends InventoryITBase {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao dao;

  private static ProductSaleDao saleDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : createStatements(ccm)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new SelectIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
    saleDao = inventoryMapper.productSaleDao(sessionRule.keyspace());
  }

  @Before
  public void insertData() {
    dao.save(FLAMETHROWER);
    dao.save(MP3_DOWNLOAD);

    saleDao.save(FLAMETHROWER_SALE_1);
    saleDao.save(FLAMETHROWER_SALE_2);
    saleDao.save(FLAMETHROWER_SALE_3);
    saleDao.save(FLAMETHROWER_SALE_4);
    saleDao.save(FLAMETHROWER_SALE_5);
    saleDao.save(MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_select_by_primary_key() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);

    dao.delete(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
  }

  @Test
  public void should_select_all() {
    assertThat(dao.all().all()).hasSize(2);
  }

  @Test
  public void should_select_by_primary_key_asynchronously() {
    assertThat(CompletableFutures.getUninterruptibly(dao.findByIdAsync(FLAMETHROWER.getId())))
        .isEqualTo(FLAMETHROWER);

    dao.delete(FLAMETHROWER);
    assertThat(CompletableFutures.getUninterruptibly(dao.findByIdAsync(FLAMETHROWER.getId())))
        .isNull();
  }

  @Test
  public void should_select_by_primary_key_and_return_optional() {
    assertThat(dao.findOptionalById(FLAMETHROWER.getId())).contains(FLAMETHROWER);

    dao.delete(FLAMETHROWER);
    assertThat(dao.findOptionalById(FLAMETHROWER.getId())).isEmpty();
  }

  @Test
  public void should_select_by_primary_key_and_return_optional_asynchronously() {
    assertThat(
            CompletableFutures.getUninterruptibly(dao.findOptionalByIdAsync(FLAMETHROWER.getId())))
        .contains(FLAMETHROWER);

    dao.delete(FLAMETHROWER);
    assertThat(
            CompletableFutures.getUninterruptibly(dao.findOptionalByIdAsync(FLAMETHROWER.getId())))
        .isEmpty();
  }

  @Test
  public void should_select_all_sales() {
    assertThat(saleDao.all().all())
        .containsOnly(
            FLAMETHROWER_SALE_1,
            FLAMETHROWER_SALE_3,
            FLAMETHROWER_SALE_4,
            FLAMETHROWER_SALE_2,
            FLAMETHROWER_SALE_5,
            MP3_DOWNLOAD_SALE_1);
  }

  @Test
  public void should_select_by_partition_key() {
    assertThat(saleDao.salesByIdForDay(FLAMETHROWER.getId(), DATE_1).all())
        .containsOnly(
            FLAMETHROWER_SALE_1, FLAMETHROWER_SALE_3, FLAMETHROWER_SALE_2, FLAMETHROWER_SALE_4);
  }

  @Test
  public void should_select_by_partition_key_and_partial_clustering() {
    assertThat(saleDao.salesByIdForCustomer(FLAMETHROWER.getId(), DATE_1, 1).all())
        .containsOnly(FLAMETHROWER_SALE_1, FLAMETHROWER_SALE_3, FLAMETHROWER_SALE_4);
  }

  @Test
  public void should_select_by_primary_key_sales() {
    assertThat(
            saleDao.salesByIdForCustomerAtTime(
                MP3_DOWNLOAD.getId(), DATE_3, 7, MP3_DOWNLOAD_SALE_1.getTs()))
        .isEqualTo(MP3_DOWNLOAD_SALE_1);
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
    @Select
    Product findById(UUID productId);

    @Select
    PagingIterable<Product> all();

    @Select
    Optional<Product> findOptionalById(UUID productId);

    @Select
    CompletionStage<Product> findByIdAsync(UUID productId);

    @Select
    CompletionStage<Optional<Product>> findOptionalByIdAsync(UUID productId);

    @Delete
    void delete(Product product);

    @Insert
    void save(Product product);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductSaleDao {
    // range query
    @Select
    PagingIterable<ProductSale> all();

    // partition key provided
    @Select
    PagingIterable<ProductSale> salesByIdForDay(UUID id, String day);

    // partition key and partial clustering key
    @Select
    PagingIterable<ProductSale> salesByIdForCustomer(UUID id, String day, int customerId);

    // full primary key
    @Select
    ProductSale salesByIdForCustomerAtTime(UUID id, String day, int customerId, UUID ts);

    @Insert
    void save(ProductSale sale);
  }
}
