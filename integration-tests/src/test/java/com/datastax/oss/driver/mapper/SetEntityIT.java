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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.mapper.model.EntityFixture;
import com.datastax.oss.driver.mapper.model.inventory.InventoryFixtures;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapper;
import com.datastax.oss.driver.mapper.model.inventory.InventoryMapperBuilder;
import com.datastax.oss.driver.mapper.model.inventory.Product;
import com.datastax.oss.driver.mapper.model.inventory.ProductDao;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SetEntityIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao productDao;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : InventoryFixtures.createStatements()) {
      session.execute(
          SimpleStatement.builder(query).withExecutionProfile(sessionRule.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new InventoryMapperBuilder(session).build();
    productDao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Test
  public void should_set_entity_on_bound_statement() {
    should_set_entity_on_bound_statement(InventoryFixtures.FLAMETHROWER);
    should_set_entity_on_bound_statement(InventoryFixtures.MP3_DOWNLOAD);
  }

  private void should_set_entity_on_bound_statement(EntityFixture<Product> entityFixture) {
    // Given
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatement boundStatement = preparedStatement.bind();

    // When
    boundStatement = productDao.set(entityFixture.entity, boundStatement);

    // Then
    entityFixture.assertMatches(boundStatement);
  }

  @Test
  public void should_set_entity_on_bound_statement_builder() {
    // Given
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    // When
    productDao.set(builder, InventoryFixtures.FLAMETHROWER.entity);
    BoundStatement boundStatement = builder.build();

    // Then
    InventoryFixtures.FLAMETHROWER.assertMatches(boundStatement);
  }

  @Test
  public void should_set_entity_on_udt_value() {
    // Given
    CqlSession session = sessionRule.session();
    UserDefinedType udtType =
        session
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
            .orElseThrow(AssertionError::new)
            .getUserDefinedType("dimensions")
            .orElseThrow(AssertionError::new);
    UdtValue udtValue = udtType.newValue();

    // When
    productDao.set(InventoryFixtures.SAMPLE_DIMENSIONS.entity, udtValue);

    // Then
    InventoryFixtures.SAMPLE_DIMENSIONS.assertMatches(udtValue);
  }
}
