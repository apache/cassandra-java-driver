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
public class SchemaValidationIT {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  private static InventoryMapper mapper;

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder(
                "CREATE TABLE product_simple(id uuid PRIMARY KEY, description text, unmapped text)")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());

    mapper = new SchemaValidationIT_InventoryMapperBuilder(session).build();
  }

  @Test
  public void should_throw_when_use_not_properly_mapped_entity() {
    assertThatThrownBy(() -> mapper.productDao(sessionRule.keyspace()))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasStackTraceContaining(
            "The CQL ks.table: ks_0.product_simple has missing columns: [description_with_incorrect_name, some_other_not_mapped_field] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductSimple");
  }

  @Test
  public void should_throw_when_entity_has_no_corresponding_cql_table() {
    assertThatThrownBy(() -> mapper.productCqlTableMissingDao(sessionRule.keyspace()))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasStackTraceContaining(
            "There is no ks.table: ks_0.product_cql_table_missing for the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductCqlTableMissing");
  }

  @Test
  public void should_throw_general_driver_exception_when_schema_validation_check_is_disabled() {
    assertThatThrownBy(() -> mapper.productDaoValidationDisabled(sessionRule.keyspace()))
        .isInstanceOf(InvalidQueryException.class)
        .hasStackTraceContaining("Undefined column name description_with_incorrect_name");
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductSimpleDao productDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleCqlTableMissingDao productCqlTableMissingDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleDaoValidationDisabledDao productDaoValidationDisabled(
        @DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductSimpleDao {

    @Update
    void update(ProductSimple product);

    @Select
    ProductSimple findById(UUID productId);
  }

  @Dao(enableEntitySchemaValidation = false)
  public interface ProductSimpleDaoValidationDisabledDao {

    @Update
    void update(ProductSimple product);

    @Select
    ProductSimple findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleCqlTableMissingDao {

    @Update
    void update(ProductCqlTableMissing product);

    @Select
    ProductCqlTableMissing findById(UUID productId);
  }

  @Entity
  public static class ProductCqlTableMissing {
    @PartitionKey private UUID id;

    public ProductCqlTableMissing() {}

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }
  }

  @Entity
  public static class ProductSimple {
    @PartitionKey private UUID id;
    private String descriptionWithIncorrectName;
    private Integer someOtherNotMappedField;

    public ProductSimple() {}

    public ProductSimple(
        UUID id, String descriptionWithIncorrectName, Integer someOtherNotMappedField) {
      this.id = id;
      this.descriptionWithIncorrectName = descriptionWithIncorrectName;
      this.someOtherNotMappedField = someOtherNotMappedField;
    }

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public String getDescriptionWithIncorrectName() {
      return descriptionWithIncorrectName;
    }

    public void setDescriptionWithIncorrectName(String descriptionWithIncorrectName) {
      this.descriptionWithIncorrectName = descriptionWithIncorrectName;
    }

    public Integer getSomeOtherNotMappedField() {
      return someOtherNotMappedField;
    }

    public void setSomeOtherNotMappedField(Integer someOtherNotMappedField) {
      this.someOtherNotMappedField = someOtherNotMappedField;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ProductSimple that = (ProductSimple) o;
      return Objects.equals(id, that.id)
          && Objects.equals(descriptionWithIncorrectName, that.descriptionWithIncorrectName)
          && Objects.equals(someOtherNotMappedField, that.someOtherNotMappedField);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, descriptionWithIncorrectName, someOtherNotMappedField);
    }

    @Override
    public String toString() {
      return "ProductSimple{"
          + "id="
          + id
          + ", descriptionWithIncorrectName='"
          + descriptionWithIncorrectName
          + '\''
          + ", someOtherNotMappedField="
          + someOtherNotMappedField
          + '}';
    }
  }
}
