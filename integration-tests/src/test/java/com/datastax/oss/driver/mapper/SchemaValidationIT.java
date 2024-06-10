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

import static com.datastax.oss.driver.api.mapper.annotations.SchemaHint.TargetElement;
import static com.datastax.oss.driver.internal.core.util.LoggerTest.setupTestLogger;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import ch.qos.logback.classic.Level;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.SchemaHint;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.testinfra.ccm.CcmBridge;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.util.LoggerTest;
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
@BackendRequirement(
    type = BackendType.CASSANDRA,
    minInclusive = "3.4",
    description = "Creates a SASI index")
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
            "CREATE TABLE product_simple_missing_p_k(id uuid PRIMARY KEY, description text, unmapped text)",
            "CREATE TABLE product_simple_missing_clustering_column(id uuid PRIMARY KEY, description text, unmapped text)",
            "CREATE TABLE product_pk_and_clustering(id uuid, c_id uuid, PRIMARY KEY (id, c_id))",
            "CREATE TABLE product_wrong_type(id uuid PRIMARY KEY, wrong_type_column text)",
            "CREATE TYPE dimensions_with_incorrect_name(length int, width int, height int)",
            "CREATE TYPE dimensions_with_wrong_type(length int, width int, height text)",
            "CREATE TYPE dimensions_with_incorrect_name_schema_hint_udt(length int, width int, height int)",
            "CREATE TYPE dimensions_with_incorrect_name_schema_hint_table(length int, width int, height int)",
            "CREATE TABLE product_with_incorrect_udt(id uuid PRIMARY KEY, description text, dimensions dimensions_with_incorrect_name)",
            "CREATE TABLE product_with_incorrect_udt_schema_hint_udt(id uuid PRIMARY KEY, description text, dimensions dimensions_with_incorrect_name_schema_hint_udt)",
            "CREATE TABLE product_with_incorrect_udt_schema_hint_table(id uuid PRIMARY KEY, description text, dimensions dimensions_with_incorrect_name_schema_hint_table)",
            "CREATE TABLE product_with_udt_wrong_type(id uuid PRIMARY KEY, description text, dimensions dimensions_with_wrong_type)");

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
    session.execute(
        SimpleStatement.builder("TRUNCATE product_wrong_type")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE product_pk_and_clustering")
            .setExecutionProfile(sessionRule.slowProfile())
            .build());
    session.execute(
        SimpleStatement.builder("TRUNCATE product_with_udt_wrong_type")
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
  public void should_log_warn_when_entity_has_no_corresponding_cql_table() {
    LoggerTest.LoggerSetup logger =
        setupTestLogger(
            SchemaValidationIT_ProductCqlTableMissingHelper__MapperGenerated.class, Level.WARN);
    try {
      assertThatThrownBy(() -> mapper.productCqlTableMissingDao(sessionRule.keyspace()))
          .isInstanceOf(InvalidQueryException.class);

      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              String.format(
                  "There is no ks.table or UDT: %s.product_cql_table_missing for the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductCqlTableMissing, or metadata is out of date.",
                  sessionRule.keyspace()));

    } finally {
      logger.close();
    }
  }

  @Test
  public void should_throw_general_driver_exception_when_schema_validation_check_is_disabled() {
    assumeThat(CcmBridge.SCYLLA_ENABLEMENT).isFalse(); // @IntegrationTestDisabledScyllaFailure
    // @IntegrationTestDisabledScyllaDifferentText
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
      should_warn_about_missing_table_when_use_not_properly_mapped_entity_with_udt_with_table_schema_hint() {
    LoggerTest.LoggerSetup logger =
        setupTestLogger(
            SchemaValidationIT_DimensionsWithIncorrectNameSchemaHintTableHelper__MapperGenerated
                .class,
            Level.WARN);
    try {
      // when
      mapper.productWithIncorrectUdtSchemaHintTable(sessionRule.keyspace());

      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              String.format(
                  "There is no ks.table or UDT: %s.dimensions_with_incorrect_name_schema_hint_table for the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.DimensionsWithIncorrectNameSchemaHintTable, or metadata is out of date.",
                  sessionRule.keyspace()));
    } finally {
      logger.close();
    }
  }

  @Test
  public void should_throw_when_table_is_missing_PKs() {
    assertThatThrownBy(() -> mapper.productSimpleMissingPKDao(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The CQL ks.table: %s.product_simple_missing_p_k has missing Primary Key columns: [id_not_present] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductSimpleMissingPK",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_when_table_is_missing_clustering_column() {
    assertThatThrownBy(() -> mapper.productSimpleMissingClusteringColumn(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The CQL ks.table: %s.product_simple_missing_clustering_column has missing Clustering columns: [not_existing_clustering_column] that are defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductSimpleMissingClusteringColumn",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_when_type_defined_in_table_does_not_match_type_from_entity() {
    assertThatThrownBy(() -> mapper.productDaoWrongType(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The CQL ks.table: %s.product_wrong_type defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductWrongType declares type mappings that are not supported by the codec registry:\n"
                    + "Field: wrong_type_column, Entity Type: java.lang.Integer, CQL type: TEXT",
                sessionRule.keyspace()));
  }

  @Test
  public void should_throw_when_type_defined_in_udt_does_not_match_type_from_entity() {
    assertThatThrownBy(() -> mapper.productWithUdtWrongTypeDao(sessionRule.keyspace()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "The CQL ks.udt: %s.dimensions_with_wrong_type defined in the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.DimensionsWithWrongType declares type mappings that are not supported by the codec registry:\n"
                    + "Field: height, Entity Type: java.lang.Integer, CQL type: TEXT",
                sessionRule.keyspace()));
  }

  @Test
  public void should_not_throw_when_have_correct_pk_and_clustering() {
    assertThatCode(() -> mapper.productPkAndClusteringDao(sessionRule.keyspace()))
        .doesNotThrowAnyException();
  }

  @Test
  public void should_log_warning_when_passing_not_existing_keyspace() {
    LoggerTest.LoggerSetup logger =
        setupTestLogger(SchemaValidationIT_ProductSimpleHelper__MapperGenerated.class, Level.WARN);
    try {
      // when
      assertThatThrownBy(
              () -> mapper.productSimpleDao(CqlIdentifier.fromCql("not_existing_keyspace")))
          .isInstanceOf(InvalidQueryException.class)
          .hasMessageContaining("not_existing_keyspace does not exist");

      // then
      verify(logger.appender, timeout(500).times(1)).doAppend(logger.loggingEventCaptor.capture());
      assertThat(logger.loggingEventCaptor.getValue().getMessage()).isNotNull();
      assertThat(logger.loggingEventCaptor.getValue().getFormattedMessage())
          .contains(
              "Unable to validate table: product_simple for the entity class: com.datastax.oss.driver.mapper.SchemaValidationIT.ProductSimple because the session metadata has no information about the keyspace: not_existing_keyspace.");
    } finally {
      logger.close();
    }
  }

  @Test
  public void should_not_warn_or_throw_when_target_element_is_NONE() {
    LoggerTest.LoggerSetup logger =
        setupTestLogger(
            SchemaValidationIT_DoesNotExistNoValidationHelper__MapperGenerated.class, Level.WARN);

    // when
    mapper.noValidationDao(sessionRule.keyspace());

    // then
    // no exceptions, no logs
    verify(logger.appender, never()).doAppend(any());
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

    @DaoFactory
    ProductWithUdtWrongTypeDao productWithUdtWrongTypeDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleMissingPKDao productSimpleMissingPKDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductSimpleMissingClusteringColumnDao productSimpleMissingClusteringColumn(
        @DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductDaoWrongTypeDao productDaoWrongType(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    ProductPkAndClusteringDao productPkAndClusteringDao(@DaoKeyspace CqlIdentifier keyspace);

    @DaoFactory
    NoValidationDao noValidationDao(@DaoKeyspace CqlIdentifier keyspace);
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
  public interface ProductWithUdtWrongTypeDao {

    @Update(customWhereClause = "id = :id")
    void updateWhereId(ProductWithUdtWrongType product, UUID id);
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

  @Dao
  public interface ProductSimpleDaoValidationDisabledDao {

    @Select
    ProductSimple findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleCqlTableMissingDao {

    @Select
    ProductCqlTableMissing findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleMissingPKDao {
    @Select
    ProductSimpleMissingPK findById(UUID productId);
  }

  @Dao
  public interface ProductSimpleMissingClusteringColumnDao {
    @Select
    ProductSimpleMissingClusteringColumn findById(UUID productId);
  }

  @Dao
  public interface ProductDaoWrongTypeDao {

    @Select
    ProductWrongType findById(UUID productId);
  }

  @Dao
  public interface ProductPkAndClusteringDao {

    @Select
    ProductPkAndClustering findById(UUID productId);
  }

  @Dao
  public interface NoValidationDao {
    // Not a real query, we just need to reference the entities
    @QueryProvider(
        providerClass = DummyProvider.class,
        entityHelpers = {DoesNotExistNoValidation.class, ProductCqlTableMissingNoValidation.class})
    void doNothing();
  }

  @SuppressWarnings("unused")
  static class DummyProvider {
    DummyProvider(
        MapperContext context,
        EntityHelper<DoesNotExistNoValidation> helper1,
        EntityHelper<ProductCqlTableMissingNoValidation> helper2) {}

    void doNothing() {}
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
  public static class ProductSimpleMissingPK {
    @PartitionKey private UUID idNotPresent;

    public ProductSimpleMissingPK() {}

    public UUID getIdNotPresent() {
      return idNotPresent;
    }

    public void setIdNotPresent(UUID idNotPresent) {
      this.idNotPresent = idNotPresent;
    }
  }

  @Entity
  public static class ProductWrongType {
    @PartitionKey private UUID id;
    private Integer wrongTypeColumn;

    public ProductWrongType() {}

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public Integer getWrongTypeColumn() {
      return wrongTypeColumn;
    }

    public void setWrongTypeColumn(Integer wrongTypeColumn) {
      this.wrongTypeColumn = wrongTypeColumn;
    }
  }

  @Entity
  public static class ProductSimpleMissingClusteringColumn {
    @PartitionKey private UUID id;
    @ClusteringColumn private Integer notExistingClusteringColumn;

    public ProductSimpleMissingClusteringColumn() {}

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public Integer getNotExistingClusteringColumn() {
      return notExistingClusteringColumn;
    }

    public void setNotExistingClusteringColumn(Integer notExistingClusteringColumn) {
      this.notExistingClusteringColumn = notExistingClusteringColumn;
    }
  }

  @Entity
  public static class ProductPkAndClustering {
    @PartitionKey private UUID id;
    @ClusteringColumn private UUID cId;

    public ProductPkAndClustering() {}

    public UUID getId() {
      return id;
    }

    public void setId(UUID id) {
      this.id = id;
    }

    public UUID getcId() {
      return cId;
    }

    public void setcId(UUID cId) {
      this.cId = cId;
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
  public static class ProductWithUdtWrongType {

    @PartitionKey private UUID id;
    private String description;
    private DimensionsWithWrongType dimensions;

    public ProductWithUdtWrongType() {}

    public ProductWithUdtWrongType(
        UUID id, String description, DimensionsWithWrongType dimensions) {
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

    public DimensionsWithWrongType getDimensions() {
      return dimensions;
    }

    public void setDimensions(DimensionsWithWrongType dimensions) {
      this.dimensions = dimensions;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ProductWithUdtWrongType)) {
        return false;
      }
      ProductWithUdtWrongType that = (ProductWithUdtWrongType) o;
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
      return "ProductWithUdtWrongType{"
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
  public static class DimensionsWithWrongType {

    private int length;
    private int width;
    private int height;

    public DimensionsWithWrongType() {}

    public DimensionsWithWrongType(int length, int width, int height) {
      this.length = length;
      this.width = width;
      this.height = height;
    }

    public int getLength() {
      return length;
    }

    public void setLength(int length) {
      this.length = length;
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
      if (!(o instanceof DimensionsWithWrongType)) {
        return false;
      }
      DimensionsWithWrongType that = (DimensionsWithWrongType) o;
      return this.length == that.length && this.height == that.height && this.width == that.width;
    }

    @Override
    public int hashCode() {
      return Objects.hash(length, width, height);
    }

    @Override
    public String toString() {
      return "DimensionsWithWrongType{"
          + "length="
          + length
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

  @Entity
  @SchemaHint(targetElement = TargetElement.NONE)
  public static class DoesNotExistNoValidation {
    private int k;

    public int getK() {
      return k;
    }

    public void setK(int k) {
      this.k = k;
    }
  }

  @Entity
  @SchemaHint(targetElement = TargetElement.NONE)
  public static class ProductCqlTableMissingNoValidation extends ProductCqlTableMissing {}
}
