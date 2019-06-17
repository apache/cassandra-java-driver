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

import static com.datastax.oss.driver.assertions.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(min = "2.2", description = "support for unset values")
public class NullSavingStrategyBehaviorIT extends InventoryITBase {
  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  private static ProductDao dao;
  private static InventoryMapper inventoryMapper;

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();

    for (String query : createStatements(ccm)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }

    inventoryMapper = new NullSavingStrategyBehaviorIT_InventoryMapperBuilder(session).build();
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
  public void should_insert_entity_and_do_not_set_null_field() {
    // given
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.save(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.saveDoNotSetNull(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();
  }

  @Test
  public void should_insert_entity_and_set_null_field() {
    // given
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.save(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.saveSetNull(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNull();
  }

  @Test
  public void
      should_insert_entity_and_set_null_field_preferring_default_strategy_when_specific_not_set() {
    // given
    DefaultNullStrategyDao dao = inventoryMapper.defaultNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.insert(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.insert(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNull();
  }

  @Test
  public void
      should_insert_entity_and_not_set_null_field_preferring_method_strategy_when_both_are_set() {
    // given
    DefaultNullStrategyDao dao = inventoryMapper.defaultNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.insert(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.insertOverrideDefaultNullSavingStrategy(
        new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();
  }

  @Test
  public void
      should_insert_entity_and_do_not_set_field_when_both_default_and_method_level_not_explicitly_set() {
    // given
    DoNotSetSavingStrategyDao dao = inventoryMapper.notSetNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.insert(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.insert(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();
  }

  @Test
  public void should_insert_entity_and_set_null_field_preferring_specific_over_default() {
    // given
    MethodOverrideNullSavingStrategyDao dao =
        inventoryMapper.methodOverrideNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.insert(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.insert(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNull();
  }

  @Test
  public void should_update_entity_and_do_not_set_null_field() {
    // given
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.updateDoNotSetNull(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();
  }

  @Test
  public void should_update_entity_udt_and_do_not_set_null_field() {
    // given
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDimensions()).isNotNull();

    // when
    dao.updateDoNotSetNull(new Product(FLAMETHROWER.getId(), "desc", null));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDimensions()).isNotNull();
  }

  @Test
  public void
      should_update_entity_and_set_null_field_preferring_default_strategy_when_specific_not_set() {
    // given
    DefaultNullStrategyDao dao = inventoryMapper.defaultNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.update(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNull();
  }

  @Test
  public void
      should_update_entity_and_not_set_null_field_preferring_method_strategy_when_both_are_set() {
    // given
    DefaultNullStrategyDao dao = inventoryMapper.defaultNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.updateOverrideDefaultNullSavingStrategy(
        new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();
  }

  @Test
  public void
      should_update_entity_and_do_not_set_null_field_when_both_default_and_method_level_not_explicitly_set() {
    // given
    DoNotSetSavingStrategyDao dao = inventoryMapper.notSetNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.update(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();
  }

  @Test
  public void should_update_entity_and_set_null_field_preferring_specific_over_default() {
    // given
    MethodOverrideNullSavingStrategyDao dao =
        inventoryMapper.methodOverrideNullStrategyDao(sessionRule.keyspace());
    assertThat(dao.findById(FLAMETHROWER.getId())).isNull();
    dao.update(FLAMETHROWER);
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNotNull();

    // when
    dao.update(new Product(FLAMETHROWER.getId(), null, FLAMETHROWER.getDimensions()));

    // then
    assertThat(dao.findById(FLAMETHROWER.getId()).getDescription()).isNull();
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
  public void should_set_entity_and_set_null_field_preferring_specific_over_default() {
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

    @Insert
    void save(Product product);

    @Insert(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void saveDoNotSetNull(Product product);

    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void saveSetNull(Product product);

    @Update
    void update(Product product);

    @Update(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void updateDoNotSetNull(Product product);

    @Select
    Product findById(UUID productId);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface DefaultNullStrategyDao {

    @Insert
    void insert(Product product);

    @Insert(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void insertOverrideDefaultNullSavingStrategy(Product product);

    @Update
    void update(Product product);

    @Update(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void updateOverrideDefaultNullSavingStrategy(Product product);

    @SetEntity
    void set(BoundStatementBuilder builder, Product product);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.DO_NOT_SET)
    void setOverrideDefaultNullSavingStrategy(BoundStatementBuilder builder, Product product);

    @Select
    Product findById(UUID productId);
  }

  @Dao
  public interface DoNotSetSavingStrategyDao {
    @Insert
    void insert(Product product);

    @Update
    void update(Product product);

    @SetEntity
    void set(BoundStatementBuilder builder, Product product);

    @Select
    Product findById(UUID productId);
  }

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.DO_NOT_SET)
  public interface MethodOverrideNullSavingStrategyDao {
    @Insert(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void insert(Product product);

    @Update(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void update(Product product);

    @SetEntity(nullSavingStrategy = NullSavingStrategy.SET_TO_NULL)
    void set(BoundStatementBuilder builder, Product product);

    @Select
    Product findById(UUID productId);
  }
}
