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

import static com.datastax.oss.driver.api.mapper.entity.naming.NamingConvention.UPPER_SNAKE_CASE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Runs simple queries for entities with various naming strategies:
 *
 * <ul>
 *   <li>{@link DefaultStrategyEntity default}
 *   <li>{@link UpperSnakeCaseEntity non-default built-in convention}
 *   <li>{@link NameConverterEntity custom name converter class}
 *   <li>{@link CustomNamesEntity custom names provided through annotations}
 * </ul>
 *
 * <p>See each entity's corresponding table schema in {@link #setup()}.
 */
@Category(ParallelizableTests.class)
public class NamingStrategyIT {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE = SessionRule.builder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private static TestMapper mapper;

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();

    for (String query :
        ImmutableList.of(
            "CREATE TABLE default_strategy_entity(entity_id int primary key)",
            "CREATE TABLE \"UPPER_SNAKE_CASE_ENTITY\"(\"ENTITY_ID\" int primary key)",
            "CREATE TABLE test_NameConverterEntity(test_entityId int primary key)",
            "CREATE TABLE custom_entity(custom_id int primary key)")) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(SESSION_RULE.slowProfile()).build());
    }

    mapper = new NamingStrategyIT_TestMapperBuilder(session).build();
  }

  @Test
  public void should_map_entity_with_default_naming_strategy() {
    DefaultStrategyEntityDao dao = mapper.defaultStrategyEntityDao(SESSION_RULE.keyspace());
    DefaultStrategyEntity entity = new DefaultStrategyEntity(1);

    dao.save(entity);
    DefaultStrategyEntity retrievedEntity = dao.findById(1);
    assertThat(retrievedEntity.getEntityId()).isEqualTo(1);
  }

  @Test
  public void should_map_entity_with_non_default_convention() {
    UpperSnakeCaseEntityDao dao = mapper.upperSnakeCaseEntityDao(SESSION_RULE.keyspace());
    UpperSnakeCaseEntity entity = new UpperSnakeCaseEntity(1);

    dao.save(entity);
    UpperSnakeCaseEntity retrievedEntity = dao.findById(1);
    assertThat(retrievedEntity.getEntityId()).isEqualTo(1);
  }

  @Test
  public void should_map_entity_with_name_converter() {
    NameConverterEntityDao dao = mapper.nameConverterEntityDao(SESSION_RULE.keyspace());
    NameConverterEntity entity = new NameConverterEntity(1);

    dao.save(entity);
    NameConverterEntity retrievedEntity = dao.findById(1);
    assertThat(retrievedEntity.getEntityId()).isEqualTo(1);
  }

  @Test
  public void should_map_entity_with_custom_names() {
    CustomNamesEntityDao dao = mapper.customNamesEntityDao(SESSION_RULE.keyspace());
    CustomNamesEntity entity = new CustomNamesEntity(1);

    dao.save(entity);
    CustomNamesEntity retrievedEntity = dao.findById(1);
    assertThat(retrievedEntity.getEntityId()).isEqualTo(1);
  }

  @Entity
  public static class DefaultStrategyEntity {
    @PartitionKey private int entityId;

    public DefaultStrategyEntity() {}

    public DefaultStrategyEntity(int entityId) {
      this.entityId = entityId;
    }

    public int getEntityId() {
      return entityId;
    }

    public void setEntityId(int entityId) {
      this.entityId = entityId;
    }
  }

  @Entity
  @NamingStrategy(convention = UPPER_SNAKE_CASE)
  public static class UpperSnakeCaseEntity {

    @PartitionKey private int entityId;

    public UpperSnakeCaseEntity() {}

    public UpperSnakeCaseEntity(int entityId) {
      this.entityId = entityId;
    }

    public int getEntityId() {
      return entityId;
    }

    public void setEntityId(int entityId) {
      this.entityId = entityId;
    }
  }

  @Entity
  @NamingStrategy(customConverterClass = TestNameConverter.class)
  public static class NameConverterEntity {

    @PartitionKey private int entityId;

    public NameConverterEntity() {}

    public NameConverterEntity(int entityId) {
      this.entityId = entityId;
    }

    public int getEntityId() {
      return entityId;
    }

    public void setEntityId(int entityId) {
      this.entityId = entityId;
    }
  }

  public static class TestNameConverter implements NameConverter {

    @Override
    @NonNull
    public String toCassandraName(@NonNull String javaName) {
      // Pretty silly but we don't need this to be realistic
      return "test_" + javaName;
    }
  }

  @Entity
  @CqlName("custom_entity")
  public static class CustomNamesEntity {

    @PartitionKey
    @CqlName("custom_id")
    private int entityId;

    public CustomNamesEntity() {}

    public CustomNamesEntity(int entityId) {
      this.entityId = entityId;
    }

    public int getEntityId() {
      return entityId;
    }

    public void setEntityId(int entityId) {
      this.entityId = entityId;
    }
  }

  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  interface BaseDao<T> {
    @Select
    T findById(int id);

    @Insert
    void save(T entity);
  }

  @Dao
  public interface DefaultStrategyEntityDao extends BaseDao<DefaultStrategyEntity> {}

  @Dao
  public interface UpperSnakeCaseEntityDao extends BaseDao<UpperSnakeCaseEntity> {}

  @Dao
  public interface NameConverterEntityDao extends BaseDao<NameConverterEntity> {}

  @Dao
  public interface CustomNamesEntityDao extends BaseDao<CustomNamesEntity> {}

  @Mapper
  public interface TestMapper {
    @DaoFactory
    DefaultStrategyEntityDao defaultStrategyEntityDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    UpperSnakeCaseEntityDao upperSnakeCaseEntityDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    NameConverterEntityDao nameConverterEntityDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    CustomNamesEntityDao customNamesEntityDao(@DaoKeyspace CqlIdentifier keyspace);
  }
}
