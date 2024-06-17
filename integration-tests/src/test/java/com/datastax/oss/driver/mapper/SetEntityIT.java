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
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class SetEntityIT extends InventoryITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();
  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static ProductDao dao;
  private static UserDefinedType dimensions2d;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query : createStatements(CCM_RULE)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    InventoryMapper inventoryMapper = new SetEntityIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(SESSION_RULE.keyspace());
    dimensions2d =
        session
            .getKeyspace()
            .flatMap(ks -> session.getMetadata().getKeyspace(ks))
            .flatMap(ks -> ks.getUserDefinedType("dimensions2d"))
            .orElseThrow(AssertionError::new);
  }

  @Test
  public void should_set_entity_on_bound_statement() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatement boundStatement = preparedStatement.bind();

    boundStatement = dao.set(FLAMETHROWER, boundStatement);

    assertMatches(boundStatement, FLAMETHROWER);
  }

  @Test
  public void should_set_entity_on_bound_statement_builder() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    dao.set(builder, FLAMETHROWER);
    BoundStatement boundStatement = builder.build();

    assertMatches(boundStatement, FLAMETHROWER);
  }

  @Test
  public void should_set_entity_on_bound_statement_setting_null() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    dao.setNullFields(
        builder, new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));
    BoundStatement boundStatement = builder.build();

    assertMatches(
        boundStatement, new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));
  }

  @Test
  public void should_set_entity_on_bound_statement_without_setting_null() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    dao.setDoNotSetNullFields(
        builder, new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));
    BoundStatement boundStatement = builder.build();

    // "" is in description because it was not set
    assertMatches(
        boundStatement, new Product(FLAMETHROWER.getId(), "", FLAMETHROWER.getDimensions()));
  }

  @Test
  public void should_set_entity_on_udt_value() {
    CqlSession session = SESSION_RULE.session();
    UserDefinedType udtType =
        session
            .getMetadata()
            .getKeyspace(SESSION_RULE.keyspace())
            .orElseThrow(AssertionError::new)
            .getUserDefinedType("dimensions")
            .orElseThrow(AssertionError::new);
    UdtValue udtValue = udtType.newValue();
    Dimensions dimensions = new Dimensions(30, 10, 8);

    dao.set(dimensions, udtValue);

    assertThat(udtValue.getInt("length")).isEqualTo(dimensions.getLength());
    assertThat(udtValue.getInt("width")).isEqualTo(dimensions.getWidth());
    assertThat(udtValue.getInt("height")).isEqualTo(dimensions.getHeight());
  }

  @Test
  public void should_set_entity_on_partial_statement_when_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps = session.prepare("INSERT INTO product (id, description) VALUES (?, ?)");
    BoundStatement bound = dao.setLenient(FLAMETHROWER, ps.bind());
    assertThat(bound.getUuid(0)).isEqualTo(FLAMETHROWER.getId());
    assertThat(bound.getString(1)).isEqualTo(FLAMETHROWER.getDescription());
  }

  @Test
  public void should_set_entity_on_partial_statement_builder_when_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps = session.prepare("INSERT INTO product (id, description) VALUES (?, ?)");
    BoundStatementBuilder builder = ps.boundStatementBuilder();
    dao.setLenient(FLAMETHROWER, builder);
    assertThat(builder.getUuid(0)).isEqualTo(FLAMETHROWER.getId());
    assertThat(builder.getString(1)).isEqualTo(FLAMETHROWER.getDescription());
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void should_set_entity_on_partial_udt_when_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps = session.prepare("INSERT INTO product2d (id, dimensions) VALUES (?, ?)");
    BoundStatementBuilder builder = ps.boundStatementBuilder();
    builder.setUuid(0, FLAMETHROWER.getId());
    UdtValue dimensionsUdt = dimensions2d.newValue();
    Dimensions dimensions = new Dimensions(12, 34, 56);
    dao.setLenient(dimensions, dimensionsUdt);
    builder.setUdtValue(1, dimensionsUdt);
    assertThat(dimensionsUdt.getInt("width")).isEqualTo(34);
    assertThat(dimensionsUdt.getInt("height")).isEqualTo(56);
  }

  @Test
  public void should_not_set_entity_on_partial_statement_when_not_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps = session.prepare("INSERT INTO product (id, description) VALUES (?, ?)");
    Throwable error = catchThrowable(() -> dao.set(FLAMETHROWER, ps.bind()));
    assertThat(error).hasMessage("dimensions is not a variable in this bound statement");
  }

  @Test
  public void should_not_set_entity_on_partial_statement_builder_when_not_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps = session.prepare("INSERT INTO product (id, description) VALUES (?, ?)");
    Throwable error = catchThrowable(() -> dao.set(ps.boundStatementBuilder(), FLAMETHROWER));
    assertThat(error).hasMessage("dimensions is not a variable in this bound statement");
  }

  @Test
  @SuppressWarnings("ResultOfMethodCallIgnored")
  public void should_not_set_entity_on_partial_udt_when_not_lenient() {
    CqlSession session = SESSION_RULE.session();
    PreparedStatement ps = session.prepare("INSERT INTO product2d (id, dimensions) VALUES (?, ?)");
    BoundStatementBuilder builder = ps.boundStatementBuilder();
    builder.setUuid(0, FLAMETHROWER.getId());
    UdtValue dimensionsUdt = dimensions2d.newValue();
    Dimensions dimensions = new Dimensions(12, 34, 56);
    Throwable error = catchThrowable(() -> dao.set(dimensions, dimensionsUdt));
    assertThat(error).hasMessage("length is not a field in this UDT");
  }

  private static void assertMatches(GettableByName data, Product entity) {
    assertThat(data.getUuid("id")).isEqualTo(entity.getId());
    assertThat(data.getString("description")).isEqualTo(entity.getDescription());
    UdtValue udtValue = data.getUdtValue("dimensions");
    assertThat(udtValue).isNotNull();
    assertThat(udtValue.getType().getName().asInternal()).isEqualTo("dimensions");
    assertThat(udtValue.getInt("length")).isEqualTo(entity.getDimensions().getLength());
    assertThat(udtValue.getInt("width")).isEqualTo(entity.getDimensions().getWidth());
    assertThat(udtValue.getInt("height")).isEqualTo(entity.getDimensions().getHeight());
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductDao {

    @SetEntity
    BoundStatement set(Product product, BoundStatement boundStatement);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void setNullFields(BoundStatementBuilder builder, Product product);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void setDoNotSetNullFields(BoundStatementBuilder builder, Product product);

    @SetEntity
    void set(BoundStatementBuilder builder, Product product);

    @SetEntity
    void set(Dimensions dimensions, UdtValue udtValue);

    @SetEntity(lenient = true)
    BoundStatement setLenient(Product product, BoundStatement boundStatement);

    @SetEntity(lenient = true)
    void setLenient(Product product, BoundStatementBuilder builder);

    @SetEntity(lenient = true)
    void setLenient(Dimensions dimensions, UdtValue udtValue);
  }
}
