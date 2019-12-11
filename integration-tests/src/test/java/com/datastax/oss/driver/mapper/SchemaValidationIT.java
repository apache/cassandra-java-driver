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

import static com.datastax.oss.driver.api.mapper.annotations.SchemaHint.*;
import static org.assertj.core.api.Assertions.assertThatCode;
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
import com.datastax.oss.driver.api.mapper.annotations.SchemaHint;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.testinfra.CassandraRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(ParallelizableTests.class)
@CassandraRequirement(min = "3.4", description = "Creates a SASI index")
public class SchemaValidationIT extends InventoryITBase {

  private static CcmRule ccm = CcmRule.getInstance();

  private static SessionRule<CqlSession> sessionRule = SessionRule.builder(ccm).build();

  private static InventoryMapper mapper;
  private static InventoryMapper mapperDisabledValidation;

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccm).around(sessionRule);

  @BeforeClass
  public static void setup() {
    CqlSession session = sessionRule.session();
    List<String> statements =
        Arrays.asList(
            "CREATE TABLE product_simple(id uuid PRIMARY KEY, description text, unmapped text)",
            "CREATE TYPE dimensions_with_incorrect_name(length int, width int, height int)",
            "CREATE TYPE dimensions_with_incorrect_name_schema_hint_udt(length int, width int, height int)",
            "CREATE TYPE dimensions_with_incorrect_name_schema_hint_table(length int, width int, height int)",
            "CREATE TABLE product_with_incorrect_udt(id uuid PRIMARY KEY, description text, dimensions dimensions_with_incorrect_name)",
            "CREATE TABLE product_with_incorrect_udt_schema_hint_udt(id uuid PRIMARY KEY, description text, dimensions dimensions_with_incorrect_name_schema_hint_udt)",
            "CREATE TABLE product_with_incorrect_udt_schema_hint_table(id uuid PRIMARY KEY, description text, dimensions dimensions_with_incorrect_name_schema_hint_table)");

    for (String query : statements) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }
    for (String query : createStatements(ccm)) {
      session.execute(
          SimpleStatement.builder(query).setExecutionProfile(sessionRule.slowProfile()).build());
    }
    mapper =
        new SchemaValidationIT_InventoryMapperBuilder(session)
            .withSchemaValidationEnabled(true)
            .build();
    mapperDisabledValidation =
        new SchemaValidationIT_InventoryMapperBuilder(session)
            .withSchemaValidationEnabled(false)
            .build();
  }

  @Before
  public void clearData() {
    CqlSession session = sessionRule.session();
    session.execute(
        SimpleStatement.builder("TRUNCATE product_simple")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE product")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE product_with_incorrect_udt")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE product_with_incorrect_udt_schema_hint_udt")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE product_with_incorrect_udt_schema_hint_table")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
  }

  @Test
  public void should_throw_when_use_not_properly_mapped_entity() {
    assertThatThrownBy(() -> mapper.productSimpleDao(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The CQL ks.table: %s.product_simple has missing columns: [description_with_incorrect_name, some_other_not_mapped_field] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductSimple",
                sessionRule.keyspace()));
  }

  @Test
  public void
      should_throw_when_use_not_properly_mapped_entity_when_ks_is_passed_as_null_extracting_ks_from_session() {
    assertThatThrownBy(() -> mapper.productSimpleDao(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The CQL ks.table: %s.product_simple has missing columns: [description_with_incorrect_name, some_other_not_mapped_field] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductSimple",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_when_entity_has_no_corresponding_cql_table() {
    assertThatThrownBy(() -> mapper.productCqlTableMissingDao(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "There is no ks.table: %s.product_cql_table_missing for the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductCqlTableMissing",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_general_driver_exception_when_schema_validation_check_is_disabled() {
    assertThatThrownBy(
            () -> mapperDisabledValidation.productDaoValidationDisabled(sessionRule.keyspace()))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining("Undefined column name description_with_incorrect_name");
  }

  @Test
  public void should_not_throw_on_table_with_properly_mapped_udt_field() {
    assertThatCode(() -> mapper.productDao(sessionRule.keyspace())).doesNotThrowAnyException();
  }

  @Test
  public void should_throw_when_use_not_properly_mapped_entity_with_udt() {
    assertThatThrownBy(() -> mapper.productWithIncorrectUdtDao(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasStackTraceContaining(
            String.format(
                "The CQL ks.udt: %s.dimensions_with_incorrect_name has missing columns: [length_not_present] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.DimensionsWithIncorrectName",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_when_use_not_properly_mapped_entity_with_udt_with_udt_schema_hint() {
    assertThatThrownBy(() -> mapper.productWithIncorrectUdtSchemaHintUdt(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasStackTraceContaining(
            String.format(
                "The CQL ks.udt: %s.dimensions_with_incorrect_name_schema_hint_udt has missing columns: [length_not_present] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.DimensionsWithIncorrectNameSchemaHintUdt",
                sessionRule.keyspace()));
  }

  @Test
  public void
      should_throw_missing_table_when_use_not_properly_mapped_entity_with_udt_with_table_schema_hint() {
    assertThatThrownBy(() -> mapper.productWithIncorrectUdtSchemaHintTable(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "There is no ks.table: %s.dimensions_with_incorrect_name_schema_hint_table for the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.DimensionsWithIncorrectNameSchemaHintTable",
                sessionRule.keyspace()));
  }

  @Mapper
  public interface InventoryMapper {
    @DaoFactory
    ProductSimpleDao productSimpleDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleCqlTableMissingDao productCqlTableMissingDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleDaoValidationDisabledDao productDaoValidationDisabled(
        @DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductWithIncorrectUdtDao productWithIncorrectUdtDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductWithIncorrectUdtSchemaHintUdtDao productWithIncorrectUdtSchemaHintUdt(
        @DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductWithIncorrectUdtSchemaHintTableDao productWithIncorrectUdtSchemaHintTable(
        @DaoKeyspace CqlIdentifier keyspace);
  }

  @Dao
  public interface ProductWithIncorrectUdtDao {

    @Update(customWhereClause = "id = :id")
    void updateWhereId(ProductWithIncorrectUdt product, UUID id);
  }

  @Dao
  public interface ProductWithIncorrectUdtSchemaHintUdtDao {

    @Update(customWhereClause = "id = :id")
    void updateWhereId(ProductWithIncorrectUdtSchemaHintUdt product, UUID id);
  }

  @Dao
  public interface ProductWithIncorrectUdtSchemaHintTableDao {

    @Update(customWhereClause = "id = :id")
    void updateWhereId(ProductWithIncorrectUdtSchemaHintTable product, UUID id);
  }

  @Dao
  public interface ProductDao {

    @Update(customWhereClause = "id = :id")
    void updateWhereId(Product product, UUID id);
  }

  @Dao
  public interface ProductSimpleDao {

    @Select
    ProductSimple findById(UUID productId);
  }

  @Dao()
  public interface ProductSimpleDaoValidationDisabledDao {

    @Select
    ProductSimple findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleCqlTableMissingDao {

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

      if (this == o) {
        return true;
      }
      if (!(o instanceof ProductSimple)) {
        return false;
      }
      ProductSimple that = (ProductSimple) o;
      return this.id.equals(that.id)
          && this.someOtherNotMappedField.equals(that.someOtherNotMappedField)
          && this.descriptionWithIncorrectName.equals(that.descriptionWithIncorrectName);
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

  @Entity
  public static class ProductWithIncorrectUdt {

    @PartitionKey private UUID id;
    private String description;
    private DimensionsWithIncorrectName dimensions;

    public ProductWithIncorrectUdt() {}

    public ProductWithIncorrectUdt(
        UUID id, String description, DimensionsWithIncorrectName dimensions) {
      this.id = id;
      this.description = description;
      this.dimensions = dimensions;
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

    public DimensionsWithIncorrectName getDimensions() {
      return dimensions;
    }

    public void setDimensions(DimensionsWithIncorrectName dimensions) {
      this.dimensions = dimensions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProductWithIncorrectUdt)) {
        return false;
      }
      ProductWithIncorrectUdt that = (ProductWithIncorrectUdt) o;
      return this.id.equals(that.id)
          && this.description.equals(that.description)
          && this.dimensions.equals(that.dimensions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description, dimensions);
    }

    @Override
    public String toString() {
      return "ProductWithIncorrectUdt{"
          + "id="
          + id
          + ", description='"
          + description
          + '\''
          + ", dimensions="
          + dimensions
          + '}';
    }
  }

  @Entity
  public static class ProductWithIncorrectUdtSchemaHintUdt {

    @PartitionKey private UUID id;
    private String description;
    private DimensionsWithIncorrectNameSchemaHintUdt dimensions;

    public ProductWithIncorrectUdtSchemaHintUdt() {}

    public ProductWithIncorrectUdtSchemaHintUdt(
        UUID id, String description, DimensionsWithIncorrectNameSchemaHintUdt dimensions) {
      this.id = id;
      this.description = description;
      this.dimensions = dimensions;
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

    public DimensionsWithIncorrectNameSchemaHintUdt getDimensions() {
      return dimensions;
    }

    public void setDimensions(DimensionsWithIncorrectNameSchemaHintUdt dimensions) {
      this.dimensions = dimensions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProductWithIncorrectUdtSchemaHintUdt)) {
        return false;
      }
      ProductWithIncorrectUdtSchemaHintUdt that = (ProductWithIncorrectUdtSchemaHintUdt) o;
      return this.id.equals(that.id)
          && this.description.equals(that.description)
          && this.dimensions.equals(that.dimensions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description, dimensions);
    }

    @Override
    public String toString() {
      return "ProductWithIncorrectUdtSchemaHint{"
          + "id="
          + id
          + ", description='"
          + description
          + '\''
          + ", dimensions="
          + dimensions
          + '}';
    }
  }

  @Entity
  public static class ProductWithIncorrectUdtSchemaHintTable {

    @PartitionKey private UUID id;
    private String description;
    private DimensionsWithIncorrectNameSchemaHintTable dimensions;

    public ProductWithIncorrectUdtSchemaHintTable() {}

    public ProductWithIncorrectUdtSchemaHintTable(
        UUID id, String description, DimensionsWithIncorrectNameSchemaHintTable dimensions) {
      this.id = id;
      this.description = description;
      this.dimensions = dimensions;
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

    public DimensionsWithIncorrectNameSchemaHintTable getDimensions() {
      return dimensions;
    }

    public void setDimensions(DimensionsWithIncorrectNameSchemaHintTable dimensions) {
      this.dimensions = dimensions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProductWithIncorrectUdtSchemaHintTable)) {
        return false;
      }
      ProductWithIncorrectUdtSchemaHintTable that = (ProductWithIncorrectUdtSchemaHintTable) o;
      return this.id.equals(that.id)
          && this.description.equals(that.description)
          && this.dimensions.equals(that.dimensions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, description, dimensions);
    }

    @Override
    public String toString() {
      return "ProductWithIncorrectUdtSchemaHintTable{"
          + "id="
          + id
          + ", description='"
          + description
          + '\''
          + ", dimensions="
          + dimensions
          + '}';
    }
  }

  @Entity
  public static class DimensionsWithIncorrectName {

    private int lengthNotPresent;
    private int width;
    private int height;

    public DimensionsWithIncorrectName() {}

    public DimensionsWithIncorrectName(int lengthNotPresent, int width, int height) {
      this.lengthNotPresent = lengthNotPresent;
      this.width = width;
      this.height = height;
    }

    public int getLengthNotPresent() {
      return lengthNotPresent;
    }

    public void setLengthNotPresent(int lengthNotPresent) {
      this.lengthNotPresent = lengthNotPresent;
    }

    public int getWidth() {
      return width;
    }

    public void setWidth(int width) {
      this.width = width;
    }

    public int getHeight() {
      return height;
    }

    public void setHeight(int height) {
      this.height = height;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DimensionsWithIncorrectName)) {
        return false;
      }
      DimensionsWithIncorrectName that = (DimensionsWithIncorrectName) o;
      return this.lengthNotPresent == that.lengthNotPresent
          && this.height == that.height
          && this.width == that.width;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lengthNotPresent, width, height);
    }

    @Override
    public String toString() {
      return "DimensionsWithIncorrectName{"
          + "lengthNotPresent="
          + lengthNotPresent
          + ", width="
          + width
          + ", height="
          + height
          + '}';
    }
  }

  @Entity
  @SchemaHint(targetElement = TargetElement.UDT)
  public static class DimensionsWithIncorrectNameSchemaHintUdt {

    private int lengthNotPresent;
    private int width;
    private int height;

    public DimensionsWithIncorrectNameSchemaHintUdt() {}

    public DimensionsWithIncorrectNameSchemaHintUdt(int lengthNotPresent, int width, int height) {
      this.lengthNotPresent = lengthNotPresent;
      this.width = width;
      this.height = height;
    }

    public int getLengthNotPresent() {
      return lengthNotPresent;
    }

    public void setLengthNotPresent(int lengthNotPresent) {
      this.lengthNotPresent = lengthNotPresent;
    }

    public int getWidth() {
      return width;
    }

    public void setWidth(int width) {
      this.width = width;
    }

    public int getHeight() {
      return height;
    }

    public void setHeight(int height) {
      this.height = height;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DimensionsWithIncorrectNameSchemaHintUdt)) {
        return false;
      }
      DimensionsWithIncorrectNameSchemaHintUdt that = (DimensionsWithIncorrectNameSchemaHintUdt) o;
      return this.lengthNotPresent == that.lengthNotPresent
          && this.height == that.height
          && this.width == that.width;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lengthNotPresent, width, height);
    }

    @Override
    public String toString() {
      return "DimensionsWithIncorrectNameSchemaHintUdt{"
          + "lengthNotPresent="
          + lengthNotPresent
          + ", width="
          + width
          + ", height="
          + height
          + '}';
    }
  }

  @Entity
  @SchemaHint(targetElement = TargetElement.TABLE)
  public static class DimensionsWithIncorrectNameSchemaHintTable {

    private int lengthNotPresent;
    private int width;
    private int height;

    public DimensionsWithIncorrectNameSchemaHintTable() {}

    public DimensionsWithIncorrectNameSchemaHintTable(int lengthNotPresent, int width, int height) {
      this.lengthNotPresent = lengthNotPresent;
      this.width = width;
      this.height = height;
    }

    public int getLengthNotPresent() {
      return lengthNotPresent;
    }

    public void setLengthNotPresent(int lengthNotPresent) {
      this.lengthNotPresent = lengthNotPresent;
    }

    public int getWidth() {
      return width;
    }

    public void setWidth(int width) {
      this.width = width;
    }

    public int getHeight() {
      return height;
    }

    public void setHeight(int height) {
      this.height = height;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof DimensionsWithIncorrectNameSchemaHintTable)) {
        return false;
      }
      DimensionsWithIncorrectNameSchemaHintTable that =
          (DimensionsWithIncorrectNameSchemaHintTable) o;
      return this.lengthNotPresent == that.lengthNotPresent
          && this.height == that.height
          && this.width == that.width;
    }

    @Override
    public int hashCode() {
      return Objects.hash(lengthNotPresent, width, height);
    }

    @Override
    public String toString() {
      return "DimensionsWithIncorrectNameSchemaHintTable{"
          + "lengthNotPresent="
          + lengthNotPresent
          + ", width="
          + width
          + ", height="
          + height
          + '}';
    }
  }
}
