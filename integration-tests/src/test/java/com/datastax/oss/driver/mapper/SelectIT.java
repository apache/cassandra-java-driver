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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
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
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class SelectIT extends InventoryITBase {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : createStatements()) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new SelectIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void insertData() {
    dao.save(FLAMETHROWER);
    dao.save(MP3_DOWNLOAD);
  }

  @Test
  public void should_select_by_primary_key() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);

    dao.delete(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
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
  public void should_select_with_custom_clause() {
    PagingIterable<Product> products = dao.findByDescription("%mp3%");
    assertThat(products.one()).isEqualTo(MP3_DOWNLOAD);
    assertThat(products.iterator()).isExhausted();
  }

  @Test
  public void should_select_with_custom_clause_asynchronously() {
    MappedAsyncPagingIterable<Product> iterable =
        CompletableFutures.getUninterruptibly(
            dao.findByDescriptionAsync("%mp3%").toCompletableFuture());
    assertThat(iterable.one()).isEqualTo(MP3_DOWNLOAD);
    assertThat(iterable.currentPage().iterator()).isExhausted();
    assertThat(iterable.hasMorePages()).isFalse();
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductDao {
    @Select
    Product findById(UUID productId);

    @Select
    Optional<Product> findOptionalById(UUID productId);

    @Select
    CompletionStage<Product> findByIdAsync(UUID productId);

    @Select
    CompletionStage<Optional<Product>> findOptionalByIdAsync(UUID productId);

    /** Note that this relies on a SASI index. */
    @Select(customWhereClause = "description LIKE :searchString")
    PagingIterable<Product> findByDescription(String searchString);

    /** Note that this relies on a SASI index. */
    @Select(customWhereClause = "description LIKE :searchString")
    CompletionStage<MappedAsyncPagingIterable<Product>> findByDescriptionAsync(String searchString);

    @Delete
    void delete(Product product);

    @Insert
    void save(Product product);
  }
}
