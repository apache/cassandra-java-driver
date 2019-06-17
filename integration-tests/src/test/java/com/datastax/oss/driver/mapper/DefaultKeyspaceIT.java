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
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Objects;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
public class DefaultKeyspaceIT {
  private static final String DEFAULT_KEYSPACE = "default_keyspace";
  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  private static SessionRule<CqlSession> sessionWithNoKeyspaceRule =
      SessionRule.builder(ccm).withKeyspace(false).build();

  private static InventoryMapper mapper;

  @ClassRule
  public static TestRule chain =
      RuleChain.outerRule(ccm).around(sessionRule).around(sessionWithNoKeyspaceRule);

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder(
                String.format(
                    "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
                    DEFAULT_KEYSPACE))
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    session.execute(
        SimpleStatement.builder(
                String.format(
                    "CREATE TABLE %s.product_simple_default_ks(id uuid PRIMARY KEY, description text)",
                    DEFAULT_KEYSPACE))
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE product_simple_without_ks(id uuid PRIMARY KEY, description text)")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE product_simple_default_ks(id uuid PRIMARY KEY, description text)")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    mapper = new DefaultKeyspaceIT_InventoryMapperBuilder(session).build();
  }

  @Test
  public void should_insert_using_default_keyspace_on_entity_level() {
    // Given
    ProductSimpleDefaultKs product = new ProductSimpleDefaultKs(UUID.randomUUID(), "desc_1");
    ProductSimpleDaoDefaultKs dao = mapper.productDaoDefaultKs();
    assertThat(dao.findById(product.id)).isNull();

    // When
    dao.update(product);

    // Then
    assertThat(dao.findById(product.id)).isEqualTo(product);
  }

  @Test
  public void should_fail_to_insert_if_default_ks_and_dao_ks_not_provided() {
    // Given
    assertThatThrownBy(
            () -> {
              InventoryMapperKsNotSet mapper =
                  new DefaultKeyspaceIT_InventoryMapperKsNotSetBuilder(sessionRule.session())
                      .build();
              mapper.productDaoDefaultKsNotSet();
            })
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageMatching("unconfigured (columnfamily|table) product_simple_default_ks_not_set");
  }

  @Test
  public void should_insert_without_ks_if_table_is_created_for_session_default_ks() {
    // Given
    ProductSimpleWithoutKs product = new ProductSimpleWithoutKs(UUID.randomUUID(), "desc_1");
    ProductSimpleDaoWithoutKs dao = mapper.productDaoWithoutKs();
    assertThat(dao.findById(product.id)).isNull();

    // When
    dao.update(product);

    // Then
    assertThat(dao.findById(product.id)).isEqualTo(product);
  }

  @Test
  public void should_insert_preferring_dao_factory_ks_over_entity_default_ks() {
    // Given
    ProductSimpleDefaultKs product = new ProductSimpleDefaultKs(UUID.randomUUID(), "desc_1");
    ProductSimpleDaoDefaultKs dao =
        mapper.productDaoEntityDefaultOverridden(sessionRule.keyspace());
    assertThat(dao.findById(product.id)).isNull();

    // When
    dao.update(product);

    // Then
    assertThat(dao.findById(product.id)).isEqualTo(product);
  }

  @Test
  public void should_fail_dao_initialization_if_keyspace_not_specified() {
    // Given
    assertThatThrownBy(
            () -> {
              // session has no keyspace
              // dao has no keyspace
              // entity has no keyspace
              InventoryMapperKsNotSet mapper =
                  new DefaultKeyspaceIT_InventoryMapperKsNotSetBuilder(
                          sessionWithNoKeyspaceRule.session())
                      .build();
              mapper.productDaoDefaultKsNotSet();
            })
        .isInstanceOf(MapperException.class)
        .hasMessage(
            "Missing keyspace. Suggestions: use SessionBuilder.withKeyspace() "
                + "when creating your session, specify a default keyspace on "
                + "ProductSimpleDefaultKsNotSet with @Entity(defaultKeyspace), or use a "
                + "@DaoFactory method with a @DaoKeyspace parameter");
  }

  @Test
  public void should_initialize_dao_if_keyspace_not_specified_but_not_needed() {
    // session has no keyspace
    // dao has no keyspace
    // entity has no keyspace
    // but dao methods don't require keyspace (GetEntity, SetEntity)
    InventoryMapperKsNotSet mapper =
        new DefaultKeyspaceIT_InventoryMapperKsNotSetBuilder(sessionWithNoKeyspaceRule.session())
            .build();
    mapper.productDaoGetAndSetOnly();
  }

  @Test
  public void should_initialize_dao_if_default_ks_provided() {
    InventoryMapper mapper =
        new DefaultKeyspaceIT_InventoryMapperBuilder(sessionWithNoKeyspaceRule.session()).build();
    // session has no keyspace, but entity does
    mapper.productDaoDefaultKs();
    mapper.productDaoEntityDefaultOverridden(sessionRule.keyspace());
  }

  @Test
  public void should_initialize_dao_if_dao_ks_provided() {
    InventoryMapperKsNotSet mapper =
        new DefaultKeyspaceIT_InventoryMapperKsNotSetBuilder(sessionWithNoKeyspaceRule.session())
            .build();
    // session has no keyspace, but dao has parameter
    mapper.productDaoDefaultKsNotSetOverridden(
        sessionRule.keyspace(), CqlIdentifier.fromCql("product_simple_default_ks"));
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductSimpleDaoDefaultKs productDaoDefaultKs();

    @DaoFactory
    ProductSimpleDaoWithoutKs productDaoWithoutKs();

    @DaoFactory
    ProductSimpleDaoDefaultKs productDaoEntityDefaultOverridden(
        @DaoKeyspace CqlIdentifier keyspace);
  }

  @Mapper
  public interface InventoryMapperKsNotSet {

    @DaoFactory
    ProductSimpleDaoDefaultKsNotSet productDaoDefaultKsNotSet();

    @DaoFactory
    ProductSimpleDaoDefaultKsNotSet productDaoDefaultKsNotSetOverridden(
        @DaoKeyspace CqlIdentifier keyspace, @DaoTable CqlIdentifier table);

    @DaoFactory
    ProductSimpleDaoDefaultKsNotSetGetAndSetOnly productDaoGetAndSetOnly();
  }

  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface BaseDao<T> {
    @Update
    void update(T product);

    @Select
    T findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleDaoDefaultKs extends BaseDao<ProductSimpleDefaultKs> {}

  @Dao
  public interface ProductSimpleDaoWithoutKs extends BaseDao<ProductSimpleWithoutKs> {}

  @Dao
  public interface ProductSimpleDaoDefaultKsNotSet extends BaseDao<ProductSimpleDefaultKsNotSet> {}

  @Dao
  @DefaultNullSavingStrategy(NullSavingStrategy.SET_TO_NULL)
  public interface ProductSimpleDaoDefaultKsNotSetGetAndSetOnly {
    @SetEntity
    void set(BoundStatementBuilder builder, ProductSimpleDefaultKsNotSet product);

    @GetEntity
    ProductSimpleDefaultKsNotSet get(Row row);
  }

  @Entity(defaultKeyspace = DEFAULT_KEYSPACE)
  public static class ProductSimpleDefaultKs {
    @PartitionKey private UUID id;
    private String description;

    public ProductSimpleDefaultKs() {}

    public ProductSimpleDefaultKs(UUID id, String description) {
      this.id = id;
      this.description = description;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    @Override
    public boolean equals(Object o) {

      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductSimpleDefaultKs that = (ProductSimpleDefaultKs) o;
      return Objects.equals(id, that.id) && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description);
    }

    @Override
    public String toString() {
      return "ProductSimple{" + "id=" + id + ", description='" + description + '\'' + '}';
    }
  }

  @Entity
  public static class ProductSimpleDefaultKsNotSet {
    @PartitionKey private UUID id;
    private String description;

    public ProductSimpleDefaultKsNotSet() {}

    public ProductSimpleDefaultKsNotSet(UUID id, String description) {
      this.id = id;
      this.description = description;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    @Override
    public boolean equals(Object o) {

      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductSimpleDefaultKsNotSet that = (ProductSimpleDefaultKsNotSet) o;
      return Objects.equals(id, that.id) && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description);
    }

    @Override
    public String toString() {
      return "ProductSimple{" + "id=" + id + ", description='" + description + '\'' + '}';
    }
  }

  @Entity
  public static class ProductSimpleWithoutKs {
    @PartitionKey private UUID id;
    private String description;

    public ProductSimpleWithoutKs() {}

    public ProductSimpleWithoutKs(UUID id, String description) {
      this.id = id;
      this.description = description;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    @Override
    public boolean equals(Object o) {

      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductSimpleWithoutKs that = (ProductSimpleWithoutKs) o;
      return Objects.equals(id, that.id) && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description);
    }

    @Override
    public String toString() {
      return "ProductSimple{" + "id=" + id + ", description='" + description + '\'' + '}';
    }
  }
}
