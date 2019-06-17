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
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
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
public class InsertEntityIT extends InventoryITBase {

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

    InventoryMapper inventoryMapper = new InsertEntityIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Before
  public void clearProductData() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE product")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
  }

  @Test
  public void should_insert_entity() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.save(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    CompletableFutures.getUninterruptibly(dao.saveAsync(FLAMETHROWER));
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_with_custom_clause() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    long timestamp = 1234;
    dao.saveWithBoundTimestamp(FLAMETHROWER, timestamp);

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
  public void should_insert_entity_with_custom_clause_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    long timestamp = 1234;
    CompletableFutures.getUninterruptibly(dao.saveAsyncWithBoundTimestamp(FLAMETHROWER, timestamp));

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
  public void should_insert_entity_if_not_exists() {
    assertThat(dao.saveIfNotExists(FLAMETHROWER)).isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.saveIfNotExists(otherProduct)).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_if_not_exists_asynchronously() {
    assertThat(CompletableFutures.getUninterruptibly(dao.saveAsyncIfNotExists(FLAMETHROWER)))
        .isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(CompletableFutures.getUninterruptibly(dao.saveAsyncIfNotExists(otherProduct)))
        .isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_if_not_exists_returning_optional() {
    assertThat(dao.saveIfNotExistsOptional(FLAMETHROWER)).isEmpty();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.saveIfNotExistsOptional(otherProduct)).contains(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_if_not_exists_returning_optional_asynchronously() {
    assertThat(
            CompletableFutures.getUninterruptibly(dao.saveAsyncIfNotExistsOptional(FLAMETHROWER)))
        .isEmpty();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            CompletableFutures.getUninterruptibly(dao.saveAsyncIfNotExistsOptional(otherProduct)))
        .contains(FLAMETHROWER);
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductDao {

    @Insert
    void save(Product product);

    @Insert(customUsingClause = "USING TIMESTAMP :timestamp")
    void saveWithBoundTimestamp(Product product, long timestamp);

    @Insert(ifNotExists = true)
    Product saveIfNotExists(Product product);

    @Insert(ifNotExists = true)
    Optional<Product> saveIfNotExistsOptional(Product product);

    @Insert
    CompletableFuture<Void> saveAsync(Product product);

    @Insert(customUsingClause = "USING TIMESTAMP :timestamp")
    CompletableFuture<Void> saveAsyncWithBoundTimestamp(Product product, long timestamp);

    @Insert(ifNotExists = true)
    CompletableFuture<Product> saveAsyncIfNotExists(Product product);

    @Insert(ifNotExists = true)
    CompletableFuture<Optional<Product>> saveAsyncIfNotExistsOptional(Product product);

    @Select
    Product findById(UUID productId);
  }
}
