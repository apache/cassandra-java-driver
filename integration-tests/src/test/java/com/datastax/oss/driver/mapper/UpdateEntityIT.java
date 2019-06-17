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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
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
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class UpdateEntityIT extends InventoryITBase {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao dao;
  private static InventoryMapper inventoryMapper;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : createStatements()) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    inventoryMapper = new UpdateEntityIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void clearProductData() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE product")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE only_pk")
            .setExecutionProfile(sessionRule.slowProfile())
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

    CqlSession session = sessionRule.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT WRITETIME(description) FROM product WHERE id = ?",
                    FLAMETHROWER.getId()))
            .one();
    long writeTime = row.getLong(0);
    assertThat(writeTime).isEqualTo(timestamp);
  }

  @Test
  public void should_update_entity_with_timestamp_literal() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.updateWithBoundTimestampLiteral(FLAMETHROWER);

    CqlSession session = sessionRule.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT WRITETIME(description) FROM product WHERE id = ?",
                    FLAMETHROWER.getId()))
            .one();
    long writeTime = row.getLong(0);
    assertThat(writeTime).isEqualTo(1000L);
  }

  @Test
  public void should_update_entity_with_ttl() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    int ttl = 100_000;
    dao.updateWithBoundTtl(FLAMETHROWER, ttl);

    CqlSession session = sessionRule.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT TTL(description) FROM product WHERE id = ?", FLAMETHROWER.getId()))
            .one();
    int writeTime = row.getInt(0);
    assertThat(writeTime).isEqualTo(ttl);
  }

  @Test
  public void should_update_entity_with_ttl_literal() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.updateWithBoundTtlLiteral(FLAMETHROWER);

    CqlSession session = sessionRule.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT TTL(description) FROM product WHERE id = ?", FLAMETHROWER.getId()))
            .one();
    int writeTime = row.getInt(0);
    assertThat(writeTime).isEqualTo(1000);
  }

  @Test
  public void should_update_entity_with_timestamp_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    long timestamp = 1234;
    CompletableFutures.getUninterruptibly(
        dao.updateAsyncWithBoundTimestamp(FLAMETHROWER, timestamp));

    CqlSession session = sessionRule.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT WRITETIME(description) FROM product WHERE id = ?",
                    FLAMETHROWER.getId()))
            .one();
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
  public void should_not_update_entity_if_not_exists() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.updateIfExists(otherProduct).wasApplied()).isEqualTo(false);
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
  public void should_update_entity_if_condition_is_met() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.updateIfLength(otherProduct, 10).wasApplied()).isEqualTo(true);
  }

  @Test
  public void should_not_update_entity_if_condition_is_not_met() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.updateIfLength(otherProduct, 20).wasApplied()).isEqualTo(false);
  }

  @Test
  public void should_async_update_entity_if_condition_is_met() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            CompletableFutures.getUninterruptibly(dao.updateIfLengthAsync(otherProduct, 10))
                .wasApplied())
        .isEqualTo(true);
  }

  @Test
  public void should_not_async_update_entity_if_condition_is_not_met() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            CompletableFutures.getUninterruptibly(dao.updateIfLengthAsync(otherProduct, 20))
                .wasApplied())
        .isEqualTo(false);
  }

  @Test
  public void should_throw_when_try_to_use_dao_with_update_only_pk() {
    assertThatThrownBy(() -> inventoryMapper.onlyPkDao(sessionRule.keyspace()))
        .hasCauseInstanceOf(UnsupportedOperationException.class)
        .hasStackTraceContaining("Entity onlyPK does not have any non PK columns.");
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
    ProductWithoutIdDao dao = inventoryMapper.productWithoutIdDao(sessionRule.keyspace());
    UUID idOne = UUID.randomUUID();
    UUID idTwo = UUID.randomUUID();
    sessionRule
        .session()
        .execute(
            SimpleStatement.newInstance(
                "INSERT INTO productwithoutid (id, clustering, description) VALUES (?,?,?)",
                idOne,
                1,
                "a"));
    sessionRule
        .session()
        .execute(
            SimpleStatement.newInstance(
                "INSERT INTO productwithoutid (id, clustering, description) VALUES (?,?,?)",
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
  public interface ProductDao {

    @Update
    void update(Product product);

    @Update(customWhereClause = "id = :id")
    void updateWhereId(Product product, UUID id);

    @Update(customWhereClause = "id IN (:id1, :id2)")
    void updateWhereIdIn(Product product, UUID id1, UUID id2);

    @Update(timestamp = ":timestamp")
    void updateWithBoundTimestamp(Product product, long timestamp);

    @Update(timestamp = "1000")
    void updateWithBoundTimestampLiteral(Product product);

    @Update(ttl = ":ttl")
    void updateWithBoundTtl(Product product, int ttl);

    @Update(ttl = "1000")
    void updateWithBoundTtlLiteral(Product product);

    @Update(ifExists = true)
    ResultSet updateIfExists(Product product);

    @Update
    CompletableFuture<Void> updateAsync(Product product);

    @Update(customIfClause = "dimensions.length = :length")
    ResultSet updateIfLength(Product product, int length);

    @Update(customIfClause = "dimensions.length = :length")
    CompletableFuture<AsyncResultSet> updateIfLengthAsync(Product product, int length);

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
  public interface OnlyPKDao {
    @Update
    void update(OnlyPK onlyPK);
  }

  @Dao
  public interface ProductWithoutIdDao {
    @Update(customWhereClause = "id IN (:id, :id2) AND clustering = 1")
    void updateWhereIdInSetWithoutPKPlaceholders(ProductWithoutId product, UUID id, UUID id2);

    @Select(customWhereClause = "id = :productId")
    ProductWithoutId findById(UUID productId);
  }
}
