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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
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
public class DeleteIT extends InventoryITBase {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao dao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : createStatements(ccm)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new DeleteIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void insertFixtures() {
    dao.save(FLAMETHROWER);
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

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
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
}
