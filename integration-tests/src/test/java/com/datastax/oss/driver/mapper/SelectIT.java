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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.mapper.model.EntityFixture;
import com.datastax.oss.driver.mapper.model.inventory.InventoryFixtures;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapper;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapperBuilder;
import com.datastax.oss.driver.mapper.model.inventory.Product;
import com.datastax.oss.driver.mapper.model.inventory.ProductDao;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class SelectIT {

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
  public void should_select_by_primary_key() {
    should_select_by_primary_key(InventoryFixtures.FLAMETHROWER);
    should_select_by_primary_key(InventoryFixtures.MP3_DOWNLOAD);
  }

  private void should_select_by_primary_key(EntityFixture<Product> entityFixture) {
    Product selectedEntity = productDao.findById(entityFixture.entity.getId());
    assertThat(selectedEntity).isEqualTo(entityFixture.entity);
  }

  @Test
  public void should_select_by_primary_key_async() throws Exception {
    should_select_by_primary_key_async(InventoryFixtures.FLAMETHROWER);
    should_select_by_primary_key_async(InventoryFixtures.MP3_DOWNLOAD);
  }

  private void should_select_by_primary_key_async(EntityFixture<Product> entityFixture)
      throws Exception {
    CompletionStage<Product> future = productDao.findByIdAsync(entityFixture.entity.getId());
    Product selectedEntity = future.toCompletableFuture().get(500, TimeUnit.MILLISECONDS);
    assertThat(selectedEntity).isEqualTo(entityFixture.entity);
  }

  @Test
  public void should_select_with_custom_clause() {
    PagingIterable<Product> products = productDao.findByDescription("%mp3%");
    assertThat(products.one()).isEqualTo(InventoryFixtures.MP3_DOWNLOAD.entity);
    assertThat(products.iterator()).isExhausted();
  }

  @Test
  public void should_select_with_custom_clause_async() throws Exception {
    CompletionStage<MappedAsyncPagingIterable<Product>> future =
        productDao.findByDescriptionAsync("%mp3%");
    MappedAsyncPagingIterable<Product> iterable =
        future.toCompletableFuture().get(500, TimeUnit.MILLISECONDS);
    assertThat(iterable.one()).isEqualTo(InventoryFixtures.MP3_DOWNLOAD.entity);
    assertThat(iterable.currentPage().iterator()).isExhausted();
    assertThat(iterable.hasMorePages()).isFalse();
  }
}
