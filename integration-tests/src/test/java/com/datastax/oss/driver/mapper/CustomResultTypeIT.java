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
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperBuilder;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.ListenableFuture;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class CustomResultTypeIT extends InventoryITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ProductDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query : createStatements(CCM_RULE)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    InventoryMapper mapper = InventoryMapper.builder(SESSION_RULE.session()).build();
    dao = mapper.productDao(SESSION_RULE.keyspace());
  }

  @Test
  public void should_use_custom_result_for_insert_method()
      throws ExecutionException, InterruptedException {

    ListenableFuture<Void> insertFuture = dao.insert(FLAMETHROWER);
    insertFuture.get();

    Row row = SESSION_RULE.session().execute("SELECT id FROM product").one();
    UUID insertedId = row.getUuid(0);
    assertThat(insertedId).isEqualTo(FLAMETHROWER.getId());
  }

  @Test
  public void should_use_custom_result_for_select_method()
      throws ExecutionException, InterruptedException {

    dao.insert(FLAMETHROWER).get();

    ListenableFuture<Product> selectFuture = dao.select(FLAMETHROWER.getId());
    Product selectedProduct = selectFuture.get();
    assertThat(selectedProduct).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_use_custom_result_for_update_method()
      throws ExecutionException, InterruptedException {

    dao.insert(FLAMETHROWER).get();

    Product productToUpdate = dao.select(FLAMETHROWER.getId()).get();
    productToUpdate.setDescription("changed description");
    ListenableFuture<Void> updateFuture = dao.update(productToUpdate);
    updateFuture.get();

    Product selectedProduct = dao.select(FLAMETHROWER.getId()).get();
    assertThat(selectedProduct.getDescription()).isEqualTo("changed description");
  }

  @Test
  public void should_use_custom_result_for_delete_method()
      throws ExecutionException, InterruptedException {
    dao.insert(FLAMETHROWER).get();

    ListenableFuture<Void> deleteFuture = dao.delete(FLAMETHROWER);
    deleteFuture.get();

    Product selectedProduct = dao.select(FLAMETHROWER.getId()).get();
    assertThat(selectedProduct).isNull();
  }

  @Test
  public void should_use_custom_result_for_query_method()
      throws ExecutionException, InterruptedException {
    dao.insert(FLAMETHROWER).get();

    ListenableFuture<Void> deleteFuture = dao.deleteById(FLAMETHROWER.getId());
    deleteFuture.get();

    Product selectedProduct = dao.select(FLAMETHROWER.getId()).get();
    assertThat(selectedProduct).isNull();
  }

  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ListenableFutureDao<EntityT> {

    @Select
    ListenableFuture<EntityT> select(UUID id);

    @Update
    ListenableFuture<Void> update(EntityT entity);

    @Insert
    ListenableFuture<Void> insert(EntityT entity);

    @Delete
    ListenableFuture<Void> delete(EntityT entity);
  }

  @Dao
  public interface ProductDao extends ListenableFutureDao<Product> {

    // We could do this easier with @Delete, but the goal here is to test @Query
    @Query("DELETE FROM ${keyspaceId}.product WHERE id = :id")
    ListenableFuture<Void> deleteById(UUID id);
  }

  @Mapper
  public interface InventoryMapper {

    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);

    static MapperBuilder<InventoryMapper> builder(CqlSession session) {
      return new CustomResultTypeIT_InventoryMapperBuilder(session);
    }
  }
}
