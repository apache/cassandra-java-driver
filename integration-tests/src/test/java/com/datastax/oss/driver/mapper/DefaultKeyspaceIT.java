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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
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

  private static InventoryMapper mapper;
  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

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
        .hasMessage("unconfigured table product_simple_default_ks_not_set");
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
  }

  @Dao
  public interface ProductSimpleDaoDefaultKs {

    @Update
    void update(ProductSimpleDefaultKs product);

    @Select
    ProductSimpleDefaultKs findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleDaoWithoutKs {

    @Update
    void update(ProductSimpleWithoutKs product);

    @Select
    ProductSimpleWithoutKs findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleDaoDefaultKsNotSet {

    @Update
    void update(ProductSimpleDefaultKsNotSet product);

    @Select
    ProductSimpleDefaultKsNotSet findById(UUID productId);
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
