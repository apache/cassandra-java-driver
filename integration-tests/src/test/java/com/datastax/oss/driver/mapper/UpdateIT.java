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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class UpdateIT extends InventoryITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ProductDao dao;
  private static InventoryMapper inventoryMapper;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query : createStatements(CCM_RULE)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }
    session.execute(
        SimpleStatement.newInstance("CREATE TABLE only_p_k(id uuid PRIMARY KEY)")
            .setExecutionProfile(SESSION_RULE.slowProfile()));

    inventoryMapper = new UpdateIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(SESSION_RULE.keyspace());
  }

  @Before
  public void clearProductData() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE product")
            .setExecutionProfile(SESSION_RULE.slowProfile())
            .build());
  }

  @Test
  public void should_update_entity() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_update_entity_matching_custom_where_clause() {
    // given
    Product toBeUpdated = new Product(UUID.randomUUID(), "a", new Dimensions(1, 1, 1));
    Product shouldNotBeUpdated = new Product(UUID.randomUUID(), "b", new Dimensions(1, 1, 1));

    dao.update(toBeUpdated);
    dao.update(shouldNotBeUpdated);

    assertThat(dao.findById(toBeUpdated.getId())).isEqualTo(toBeUpdated);
    assertThat(dao.findById(shouldNotBeUpdated.getId())).isEqualTo(shouldNotBeUpdated);

    // when
    Product afterUpdate = new Product(toBeUpdated.getId(), "c", new Dimensions(1, 1, 1));
    dao.updateWhereId(afterUpdate, toBeUpdated.getId());

    // then
    assertThat(dao.findById(toBeUpdated.getId())).isEqualTo(afterUpdate);
    assertThat(dao.findById(shouldNotBeUpdated.getId())).isEqualTo(shouldNotBeUpdated);
  }

  @Test
  public void should_update_entity_matching_custom_where_in_clause() {
    // given
    Product toBeUpdated = new Product(UUID.randomUUID(), "a", new Dimensions(1, 1, 1));
    Product toBeUpdated2 = new Product(UUID.randomUUID(), "b", new Dimensions(1, 1, 1));

    dao.update(toBeUpdated);
    dao.update(toBeUpdated2);

    assertThat(dao.findById(toBeUpdated.getId())).isEqualTo(toBeUpdated);
    assertThat(dao.findById(toBeUpdated2.getId())).isEqualTo(toBeUpdated2);

    // when
    Product afterUpdate = new Product(toBeUpdated.getId(), "c", new Dimensions(1, 1, 1));
    dao.updateWhereIdIn(afterUpdate, toBeUpdated.getId(), toBeUpdated2.getId());

    // then
    assertThat(dao.findById(toBeUpdated.getId()).getDescription())
        .isEqualTo(afterUpdate.getDescription());
    assertThat(dao.findById(toBeUpdated2.getId()).getDescription())
        .isEqualTo(afterUpdate.getDescription());
  }

  @Test
  public void should_update_entity_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    CompletableFutures.getUninterruptibly(dao.updateAsync(FLAMETHROWER));
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_update_entity_with_timestamp() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    long timestamp = 1234;
    dao.updateWithBoundTimestamp(FLAMETHROWER, timestamp);

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT WRITETIME(description) FROM product WHERE id = ?",
                    FLAMETHROWER.getId()))
            .one();
    assertThat(row).isNotNull();
    long writeTime = row.getLong(0);
    assertThat(writeTime).isEqualTo(timestamp);
  }

  @Test
  public void should_update_entity_with_timestamp_literal() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.updateWithTimestampLiteral(FLAMETHROWER);

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT WRITETIME(description) FROM product WHERE id = ?",
                    FLAMETHROWER.getId()))
            .one();
    assertThat(row).isNotNull();
    long writeTime = row.getLong(0);
    assertThat(writeTime).isEqualTo(1000L);
  }

  @Test
  public void should_update_entity_with_ttl() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    int ttl = 100_000;
    dao.updateWithBoundTtl(FLAMETHROWER, ttl);

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT TTL(description) FROM product WHERE id = ?", FLAMETHROWER.getId()))
            .one();
    assertThat(row).isNotNull();
    int writeTime = row.getInt(0);
    assertThat(writeTime).isBetween(ttl - 10, ttl);
  }

  @Test
  public void should_update_entity_with_ttl_literal() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.updateWithTtlLiteral(FLAMETHROWER);

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT TTL(description) FROM product WHERE id = ?", FLAMETHROWER.getId()))
            .one();
    assertThat(row).isNotNull();
    int writeTime = row.getInt(0);
    assertThat(writeTime).isBetween(990, 1000);
  }

  @Test
  public void should_update_entity_with_timestamp_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    long timestamp = 1234;
    CompletableFutures.getUninterruptibly(
        dao.updateAsyncWithBoundTimestamp(FLAMETHROWER, timestamp));

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT WRITETIME(description) FROM product WHERE id = ?",
                    FLAMETHROWER.getId()))
            .one();
    assertThat(row).isNotNull();
    long writeTime = row.getLong(0);
    assertThat(writeTime).isEqualTo(timestamp);
  }

  @Test
  public void should_update_entity_if_exists() {
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.updateIfExists(otherProduct).wasApplied()).isEqualTo(true);
  }

  @Test
  public void should_update_entity_if_exists_statement() {
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            SESSION_RULE.session().execute(dao.updateIfExistsStatement(otherProduct)).wasApplied())
        .isEqualTo(true);
  }

  @Test
  public void should_not_update_entity_if_not_exists() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.updateIfExists(otherProduct).wasApplied()).isEqualTo(false);
  }

  @Test
  public void should_not_update_entity_if_not_exists_statement() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            SESSION_RULE.session().execute(dao.updateIfExistsStatement(otherProduct)).wasApplied())
        .isEqualTo(false);
  }

  @Test
  public void should_update_entity_if_exists_asynchronously() {
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            CompletableFutures.getUninterruptibly(dao.updateAsyncIfExists(otherProduct))
                .wasApplied())
        .isEqualTo(true);
  }

  @Test
  public void should_not_update_entity_if_not_exists_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            CompletableFutures.getUninterruptibly(dao.updateAsyncIfExists(otherProduct))
                .wasApplied())
        .isEqualTo(false);
  }

  @Test
  public void should_throw_when_try_to_use_dao_with_update_only_pk() {
    assertThatThrownBy(() -> inventoryMapper.onlyPkDao(SESSION_RULE.keyspace()))
        .isInstanceOf(MapperException.class)
        .hasMessageContaining("Entity OnlyPK does not have any non PK columns.");
  }

  @Test
  public void should_update_entity_and_return_was_applied() {
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    assertThat(dao.updateReturnWasApplied(FLAMETHROWER)).isTrue();
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_not_update_entity_and_return_was_not_applied() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    assertThat(dao.updateReturnWasApplied(FLAMETHROWER)).isFalse();
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
  }

  @Test
  public void should_update_entity_and_return_was_applied_async() {
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    assertThat(CompletableFutures.getUninterruptibly(dao.updateReturnWasAppliedAsync(FLAMETHROWER)))
        .isTrue();
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_not_update_entity_and_return_was_not_applied_async() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    assertThat(CompletableFutures.getUninterruptibly(dao.updateReturnWasAppliedAsync(FLAMETHROWER)))
        .isFalse();
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
  }

  @Test
  public void should_update_entity_without_pk_placeholders_matching_custom_where_in_clause() {
    // given
    ProductWithoutIdDao dao = inventoryMapper.productWithoutIdDao(SESSION_RULE.keyspace());
    UUID idOne = UUID.randomUUID();
    UUID idTwo = UUID.randomUUID();
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.newInstance(
                "INSERT INTO product_without_id (id, clustering, description) VALUES (?,?,?)",
                idOne,
                1,
                "a"));
    SESSION_RULE
        .session()
        .execute(
            SimpleStatement.newInstance(
                "INSERT INTO product_without_id (id, clustering, description) VALUES (?,?,?)",
                idTwo,
                1,
                "b"));

    assertThat(dao.findById(idOne).getDescription()).isEqualTo("a");
    assertThat(dao.findById(idTwo).getDescription()).isEqualTo("b");

    // when
    ProductWithoutId afterUpdate = new ProductWithoutId("c");
    dao.updateWhereIdInSetWithoutPKPlaceholders(afterUpdate, idOne, idTwo);

    // then
    assertThat(dao.findById(idOne).getDescription()).isEqualTo(afterUpdate.getDescription());
    assertThat(dao.findById(idTwo).getDescription()).isEqualTo(afterUpdate.getDescription());
  }

  @Test
  public void should_update_entity_and_set_null_field() {
    // given
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.updateSetNull(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNull();
  }

  @Test
  public void should_update_entity_udt_and_set_null_field() {
    // given
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDimensions()).isNotNull();

    // when
    dao.updateSetNull(new Product(FLAMETHROWER.getId(), "desc", null));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDimensions()).isNull();
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    OnlyPKDao onlyPkDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductWithoutIdDao productWithoutIdDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductDao {

    @Update
    void update(Product product);

    @Update(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void updateSetNull(Product product);

    @Update(customWhereClause = "id = :id")
    void updateWhereId(Product product, UUID id);

    @Update(customWhereClause = "id IN (:id1, :id2)")
    void updateWhereIdIn(Product product, UUID id1, UUID id2);

    @Update(timestamp = ":timestamp")
    void updateWithBoundTimestamp(Product product, long timestamp);

    @Update(timestamp = "1000")
    void updateWithTimestampLiteral(Product product);

    @Update(ttl = ":ttl")
    void updateWithBoundTtl(Product product, int ttl);

    @Update(ttl = "1000")
    void updateWithTtlLiteral(Product product);

    @Update(ifExists = true)
    ResultSet updateIfExists(Product product);

    @Update(ifExists = true)
    BoundStatement updateIfExistsStatement(Product product);

    @Update
    CompletableFuture<Void> updateAsync(Product product);

    @Update(timestamp = ":timestamp")
    CompletableFuture<Void> updateAsyncWithBoundTimestamp(Product product, long timestamp);

    @Update(ifExists = true)
    CompletableFuture<AsyncResultSet> updateAsyncIfExists(Product product);

    @Update(ifExists = true)
    boolean updateReturnWasApplied(Product product);

    @Update(ifExists = true)
    CompletableFuture<Boolean> updateReturnWasAppliedAsync(Product product);

    @Select
    Product findById(UUID productId);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface OnlyPKDao {
    @Update
    void update(OnlyPK onlyPK);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductWithoutIdDao {
    @Update(customWhereClause = "id IN (:id, :id2) AND clustering = 1")
    void updateWhereIdInSetWithoutPKPlaceholders(ProductWithoutId product, UUID id, UUID id2);

    @Select(customWhereClause = "id = :productId")
    ProductWithoutId findById(UUID productId);
  }
}
