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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.mapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
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
public class InsertIT extends InventoryITBase {

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

    InventoryMapper inventoryMapper = new InsertIT_InventoryMapperBuilder(session).build();
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
  public void should_insert_entity() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.save(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_returning_result_set() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    ResultSet rs = dao.saveReturningResultSet(FLAMETHROWER);
    assertThat(rs.getAvailableWithoutFetching()).isZero();
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_return_bound_statement_to_execute() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    BoundStatement bs = dao.saveReturningBoundStatement(FLAMETHROWER);
    ResultSet rs = SESSION_RULE.session().execute(bs);
    assertThat(rs.getAvailableWithoutFetching()).isZero();
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    CompletableFutures.getUninterruptibly(dao.saveAsync(FLAMETHROWER));
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_asynchronously_returning_result_set() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    AsyncResultSet rs =
        CompletableFutures.getUninterruptibly(dao.saveAsyncReturningAsyncResultSet(FLAMETHROWER));
    assertThat(rs.currentPage().iterator()).isExhausted();
    assertThat(dao.findById(FLAMETHROWER.getId())).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_with_bound_timestamp() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    long timestamp = 1234;
    dao.saveWithBoundTimestamp(FLAMETHROWER, timestamp);

    CqlSession session = SESSION_RULE.session();
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
  public void should_insert_entity_with_literal_timestamp() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.saveWithLiteralTimestamp(FLAMETHROWER);

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT WRITETIME(description) FROM product WHERE id = ?",
                    FLAMETHROWER.getId()))
            .one();
    long writeTime = row.getLong(0);
    assertThat(writeTime).isEqualTo(1234);
  }

  @Test
  public void should_insert_entity_with_bound_ttl() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    int insertedTtl = 86400;
    dao.saveWithBoundTtl(FLAMETHROWER, insertedTtl);

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT TTL(description) FROM product WHERE id = ?", FLAMETHROWER.getId()))
            .one();
    Integer retrievedTtl = row.get(0, Integer.class);
    assertThat(retrievedTtl).isNotNull().isLessThanOrEqualTo(insertedTtl);
  }

  @Test
  public void should_insert_entity_with_literal_ttl() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    dao.saveWithLiteralTtl(FLAMETHROWER);

    CqlSession session = SESSION_RULE.session();
    Row row =
        session
            .execute(
                SimpleStatement.newInstance(
                    "SELECT TTL(description) FROM product WHERE id = ?", FLAMETHROWER.getId()))
            .one();
    Integer retrievedTtl = row.get(0, Integer.class);
    assertThat(retrievedTtl).isNotNull().isLessThanOrEqualTo(86400);
  }

  @Test
  public void should_insert_entity_with_bound_timestamp_asynchronously() {
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();

    long timestamp = 1234;
    CompletableFutures.getUninterruptibly(dao.saveAsyncWithBoundTimestamp(FLAMETHROWER, timestamp));

    CqlSession session = SESSION_RULE.session();
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
    assumeThat(CcmBridge.SCYLLA_ENABLEMENT).isFalse(); // @IntegrationTestDisabledScyllaFailure
    assertThat(dao.saveIfNotExists(FLAMETHROWER)).isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.saveIfNotExists(otherProduct)).isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_if_not_exists_returning_boolean() {
    assertThat(dao.saveIfNotExistsReturningBoolean(FLAMETHROWER)).isTrue();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.saveIfNotExistsReturningBoolean(otherProduct)).isFalse();
  }

  @Test
  public void should_insert_entity_if_not_exists_asynchronously() {
    assumeThat(CcmBridge.SCYLLA_ENABLEMENT).isFalse(); // @IntegrationTestDisabledScyllaFailure
    assertThat(CompletableFutures.getUninterruptibly(dao.saveAsyncIfNotExists(FLAMETHROWER)))
        .isNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(CompletableFutures.getUninterruptibly(dao.saveAsyncIfNotExists(otherProduct)))
        .isEqualTo(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_if_not_exists_asynchronously_returning_boolean() {
    assertThat(
            CompletableFutures.getUninterruptibly(
                dao.saveAsyncIfNotExistsReturningBoolean(FLAMETHROWER)))
        .isTrue();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            CompletableFutures.getUninterruptibly(
                dao.saveAsyncIfNotExistsReturningBoolean(otherProduct)))
        .isFalse();
  }

  @Test
  public void should_insert_entity_if_not_exists_returning_optional() {
    assumeThat(CcmBridge.SCYLLA_ENABLEMENT).isFalse(); // @IntegrationTestDisabledScyllaFailure
    assertThat(dao.saveIfNotExistsOptional(FLAMETHROWER)).isEmpty();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.saveIfNotExistsOptional(otherProduct)).contains(FLAMETHROWER);
  }

  @Test
  public void should_insert_entity_if_not_exists_returning_optional_asynchronously() {
    assumeThat(CcmBridge.SCYLLA_ENABLEMENT).isFalse(); // @IntegrationTestDisabledScyllaFailure
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
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductDao {

    @Insert
    void save(Product product);

    @Insert
    ResultSet saveReturningResultSet(Product product);

    @Insert
    BoundStatement saveReturningBoundStatement(Product product);

    @Insert(timestamp = ":timestamp")
    void saveWithBoundTimestamp(Product product, long timestamp);

    @Insert(timestamp = "1234")
    void saveWithLiteralTimestamp(Product product);

    @Insert(ttl = ":ttl")
    void saveWithBoundTtl(Product product, int ttl);

    @Insert(ttl = "86400")
    void saveWithLiteralTtl(Product product);

    @Insert(ifNotExists = true)
    Product saveIfNotExists(Product product);

    @Insert(ifNotExists = true)
    boolean saveIfNotExistsReturningBoolean(Product product);

    @Insert(ifNotExists = true)
    Optional<Product> saveIfNotExistsOptional(Product product);

    @Insert
    CompletableFuture<Void> saveAsync(Product product);

    @Insert
    CompletableFuture<AsyncResultSet> saveAsyncReturningAsyncResultSet(Product product);

    @Insert(timestamp = ":\"TIMESTAMP\"")
    CompletableFuture<Void> saveAsyncWithBoundTimestamp(
        Product product, @CqlName("\"TIMESTAMP\"") long timestamp);

    @Insert(ifNotExists = true)
    CompletableFuture<Product> saveAsyncIfNotExists(Product product);

    @Insert(ifNotExists = true)
    CompletableFuture<Boolean> saveAsyncIfNotExistsReturningBoolean(Product product);

    @Insert(ifNotExists = true)
    CompletableFuture<Optional<Product>> saveAsyncIfNotExistsOptional(Product product);

    @Select
    Product findById(UUID productId);
  }
}
