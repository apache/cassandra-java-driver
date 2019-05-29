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
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
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
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class SetEntityIT extends InventoryITBase {

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

    inventoryMapper = new SetEntityIT_InventoryMapperBuilder(session).build();
    dao = inventoryMapper.productDao(sessionRule.keyspace());
  }

  @Test
  public void should_set_entity_on_bound_statement() {
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatement boundStatement = preparedStatement.bind();

    boundStatement = dao.set(FLAMETHROWER, boundStatement);

    assertMatches(boundStatement, FLAMETHROWER);
  }

  @Test
  public void should_set_entity_on_bound_statement_builder() {
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    dao.set(builder, FLAMETHROWER);
    BoundStatement boundStatement = builder.build();

    assertMatches(boundStatement, FLAMETHROWER);
  }

  @Test
  public void should_set_entity_on_bound_statement_setting_null() {
    CqlSession session = sessionRule.session();
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
    CqlSession session = sessionRule.session();
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
    CqlSession session = sessionRule.session();
    UserDefinedType udtType =
        session
            .getMetadata()
            .getKeyspace(sessionRule.keyspace())
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
  public void
      should_set_entity_and_set_null_field_preferring_default_strategy_when_specific_not_set() {
    // given
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    // when
    DefaultNullStrategyDao dao = inventoryMapper.defaultNullStrategyDao(sessionRule.keyspace());
    dao.set(builder, new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertMatches(
        builder.build(), new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));
  }

  @Test
  public void
      should_set_entity_and_not_set_null_field_preferring_method_strategy_when_both_are_set() {
    // given
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    // when
    DefaultNullStrategyDao dao = inventoryMapper.defaultNullStrategyDao(sessionRule.keyspace());
    dao.setOverrideDefaultNullSavingStrategy(
        builder, new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertMatches(
        builder.build(), new Product(FLAMETHROWER.getId(), "", FLAMETHROWER.getDimensions()));
  }

  @Test
  public void
      should_set_entity_and_do_not_set_field_when_both_default_and_method_level_not_explicitly_set() {
    // given
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    // when
    DoNotSetSavingStrategyDao dao = inventoryMapper.notSetNullStrategyDao(sessionRule.keyspace());
    dao.set(builder, new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertMatches(
        builder.build(), new Product(FLAMETHROWER.getId(), "", FLAMETHROWER.getDimensions()));
  }

  @Test
  public void should_insert_entity_and_set_null_field_preferring_specific_over_default() {
    // given
    CqlSession session = sessionRule.session();
    PreparedStatement preparedStatement =
        session.prepare("INSERT INTO product (id, description, dimensions) VALUES (?, ?, ?)");
    BoundStatementBuilder builder = preparedStatement.boundStatementBuilder();

    // when
    MethodOverrideNullSavingStrategyDao dao =
        inventoryMapper.methodOverrideNullStrategyDao(sessionRule.keyspace());
    dao.set(builder, new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertMatches(
        builder.build(), new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));
  }

  private static void assertMatches(GettableByName data, Product entity) {
    assertThat(data.getUuid("id")).isEqualTo(entity.getId());
    assertThat(data.getString("description")).isEqualTo(entity.getDescription());
    UdtValue udtValue = data.getUdtValue("dimensions");
    assertThat(udtValue.getType().getName().asInternal()).isEqualTo("dimensions");
    assertThat(udtValue.getInt("length")).isEqualTo(entity.getDimensions().getLength());
    assertThat(udtValue.getInt("width")).isEqualTo(entity.getDimensions().getWidth());
    assertThat(udtValue.getInt("height")).isEqualTo(entity.getDimensions().getHeight());
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    DefaultNullStrategyDao defaultNullStrategyDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    DoNotSetSavingStrategyDao notSetNullStrategyDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    MethodOverrideNullSavingStrategyDao methodOverrideNullStrategyDao(
        @DaoKeyspace CqlIdentifier keyspace);
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
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface DefaultNullStrategyDao {

    @SetEntity
    void set(BoundStatementBuilder builder, Product product);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void setOverrideDefaultNullSavingStrategy(BoundStatementBuilder builder, Product product);
  }

  @Dao
  public interface DoNotSetSavingStrategyDao {
    @SetEntity
    void set(BoundStatementBuilder builder, Product product);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.DO_NOT_SET)
  public interface MethodOverrideNullSavingStrategyDao {
    @SetEntity(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void set(BoundStatementBuilder builder, Product product);
  }
}
