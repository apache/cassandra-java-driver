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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.mapper.model.EntityFixture;
import com.datastax.oss.driver.mapper.model.inventory.InventoryFixtures;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapper;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapperBuilder;
import com.datastax.oss.driver.mapper.model.inventory.Product;
import com.datastax.oss.driver.mapper.model.inventory.ProductDao;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class GetEntityIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao productDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : InventoryFixtures.createStatements()) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
    productDao = inventoryMapper.productDao(sessionRule.keyspace());
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatement boundStatement = preparedStatement.bind();
    session.execute(productDao.set(InventoryFixtures.FLAMETHROWER.entity, boundStatement));
    session.execute(productDao.set(InventoryFixtures.MP3_DOWNLOAD.entity, boundStatement));
  }

  @Test
  public void should_get_entity_with_row() {
    should_get_entity_with_row(InventoryFixtures.FLAMETHROWER);
    should_get_entity_with_row(InventoryFixtures.MP3_DOWNLOAD);
  }

  private void should_get_entity_with_row(EntityFixture<Product> entityFixture) {
    CqlSession session = sessionRule.session();
    ResultSet rs =
        session.execute(
            "Select * FROM product WHERE id=" + entityFixture.entity.getId().toString());
    List<Row> rows = rs.all();
    assertThat(rows.size()).isEqualTo(1);
    Product product = productDao.get(rows.get(0));
    assertThat(product).isEqualTo(entityFixture.entity);
  }

  @Test
  public void should_get_entity_with_result_set() {
    should_get_entity_with_result_set(InventoryFixtures.FLAMETHROWER);
    should_get_entity_with_result_set(InventoryFixtures.MP3_DOWNLOAD);
  }

  private void should_get_entity_with_result_set(EntityFixture<Product> entityFixture) {
    CqlSession session = sessionRule.session();
    ResultSet rs =
        session.execute(
            SimpleStatement.newInstance(
                "Select * FROM product WHERE id=?", entityFixture.entity.getId()));
    Product product = productDao.getOne(rs);
    assertThat(product).isEqualTo(entityFixture.entity);
  }

  @Test
  public void should_get_entity_with_async_result_set() {
    should_get_entity_with_async_result_set(InventoryFixtures.FLAMETHROWER);
    should_get_entity_with_async_result_set(InventoryFixtures.MP3_DOWNLOAD);
  }

  private void should_get_entity_with_async_result_set(EntityFixture<Product> entityFixture) {
    CqlSession session = sessionRule.session();
    CompletionStage<? extends AsyncResultSet> future =
        session.executeAsync(
            SimpleStatement.newInstance(
                "Select * FROM product WHERE id=?", entityFixture.entity.getId()));
    AsyncResultSet rs = CompletableFutures.getUninterruptibly(future);
    Product product = productDao.getOne(rs);
    assertThat(product).isEqualTo(entityFixture.entity);
  }

  @Test
  public void should_get_iterable_with_result_set() {
    CqlSession session = sessionRule.session();
    ResultSet rs = session.execute("Select * FROM product");
    PagingIterable<Product> products = productDao.get(rs);
    assertThat(Sets.newHashSet(products))
        .containsOnly(InventoryFixtures.FLAMETHROWER.entity, InventoryFixtures.MP3_DOWNLOAD.entity);
  }

  @Test
  public void should_get_async_iterable_with_async_result_set() {
    CqlSession session = sessionRule.session();
    CompletionStage<? extends AsyncResultSet> future =
        session.executeAsync("Select * FROM product");
    AsyncResultSet rs = CompletableFutures.getUninterruptibly(future);
    MappedAsyncPagingIterable<Product> products = productDao.get(rs);
    assertThat(Sets.newHashSet(products.currentPage()))
        .containsOnly(InventoryFixtures.FLAMETHROWER.entity, InventoryFixtures.MP3_DOWNLOAD.entity);
    assertThat(products.hasMorePages()).isFalse();
  }
}
