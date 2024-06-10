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

import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.testinfra.ScyllaSkip;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
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
@ScyllaSkip(
    description = "@IntegrationTestDisabledScyllaFailure") // Test hangs in setup() in productDao()
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "3.11.0",
    description = "UDT fields in IF clause")
public class UpdateCustomIfClauseIT extends InventoryITBase {

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

    InventoryMapper inventoryMapper =
        new UpdateCustomIfClauseIT_InventoryMapperBuilder(session).build();
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
  public void should_update_entity_if_condition_is_met() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.updateIfLength(otherProduct, 10).wasApplied()).isEqualTo(true);
  }

  @Test
  public void should_update_entity_if_condition_is_met_statement() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            SESSION_RULE
                .session()
                .execute(dao.updateIfLengthStatement(otherProduct, 10))
                .wasApplied())
        .isEqualTo(true);
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
  public void should_not_update_entity_if_condition_is_not_met_statement() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(
            SESSION_RULE
                .session()
                .execute(dao.updateIfLengthStatement(otherProduct, 20))
                .wasApplied())
        .isEqualTo(false);
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
  public void should_update_entity_if_condition_is_met_using_ttl() {
    dao.update(
        new Product(FLAMETHROWER.getId(), "Description for length 10", new Dimensions(10, 1, 1)));
    assertThat(dao.findById(FLAMETHROWER.getId())).isNotNull();

    Product otherProduct =
        new Product(FLAMETHROWER.getId(), "Other description", new Dimensions(1, 1, 1));
    assertThat(dao.updateIfLengthUsingTtl(otherProduct, 10).wasApplied()).isEqualTo(true);
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductDao {

    @Update
    void update(Product product);

    @Update(customIfClause = "dimensions.length = :length")
    ResultSet updateIfLength(Product product, int length);

    @Update(customIfClause = "dimensions.length = :length", ttl = "20")
    ResultSet updateIfLengthUsingTtl(Product product, int length);

    @Update(customIfClause = "dimensions.length = :length")
    BoundStatement updateIfLengthStatement(Product product, int length);

    @Update(customIfClause = "dimensions.length = :\"Length\"")
    CompletableFuture<AsyncResultSet> updateIfLengthAsync(
        Product product, @CqlName("\"Length\"") int length);

    @Select
    Product findById(UUID productId);
  }
}
