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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import static com.datastax.oss.driver.api.mapper.annotations.SchemaHint.TargetElement;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.annotations.SchemaHint;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.dao.LoggingGenerator;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class EntityHelperSchemaValidationMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;
  private TypeElement entityTypeElement;
  private LoggingGenerator loggingGenerator;
  private EntityHelperGenerator entityHelperGenerator;

  public EntityHelperSchemaValidationMethodGenerator(
      EntityDefinition entityDefinition,
      TypeElement entityTypeElement,
      LoggingGenerator loggingGenerator,
      EntityHelperGenerator entityHelperGenerator) {
    this.entityDefinition = entityDefinition;
    this.entityTypeElement = entityTypeElement;
    this.loggingGenerator = loggingGenerator;
    this.entityHelperGenerator = entityHelperGenerator;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder methodBuilder =
        MethodSpec.methodBuilder("validateEntityFields")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID);

    Optional<TargetElement> targetElement =
        Optional.ofNullable(entityTypeElement.getAnnotation(SchemaHint.class))
            .map(SchemaHint::targetElement);

    if (targetElement.isPresent() && targetElement.get() == TargetElement.NONE) {
      methodBuilder.addComment(
          "Nothing to do, validation was disabled with @SchemaHint(targetElement = NONE)");
    } else {
      // get keyspaceId from context, and if not present fallback to keyspace set on session
      methodBuilder.addStatement(
          "$1T keyspaceId = this.keyspaceId != null ? this.keyspaceId : context.getSession().getKeyspace().orElse(null)",
          CqlIdentifier.class);

      methodBuilder.addStatement("String entityClassName = $S", entityDefinition.getClassName());
      generateKeyspaceNull(methodBuilder);

      generateKeyspaceNameWrong(methodBuilder);

      methodBuilder.addStatement(
          "$1T<$2T> keyspace = context.getSession().getMetadata().getKeyspace(keyspaceId)",
          Optional.class,
          KeyspaceMetadata.class);

      // Generates expected names to be present in cql (table or udt)
      List<CodeBlock> expectedCqlNames =
          entityDefinition.getAllColumns().stream()
              .map(PropertyDefinition::getCqlName)
              .collect(Collectors.toList());
      methodBuilder.addStatement(
          "$1T<$2T> expectedCqlNames = new $3T<>()",
          List.class,
          CqlIdentifier.class,
          ArrayList.class);
      for (CodeBlock expectedCqlName : expectedCqlNames) {
        methodBuilder.addStatement(
            "expectedCqlNames.add($1T.fromCql($2L))", CqlIdentifier.class, expectedCqlName);
      }

      methodBuilder.addStatement(
          "$1T<$2T> tableMetadata = keyspace.flatMap(v -> v.getTable(tableId))",
          Optional.class,
          TableMetadata.class);

      // Generated UserDefineTypes metadata
      methodBuilder.addStatement(
          "$1T<$2T> userDefinedType = keyspace.flatMap(v -> v.getUserDefinedType(tableId))",
          Optional.class,
          UserDefinedType.class);

      generateValidationChecks(methodBuilder, targetElement);

      logMissingMetadata(methodBuilder);
    }
    return Optional.of(methodBuilder.build());
  }

  private void logMissingMetadata(MethodSpec.Builder methodBuilder) {
    methodBuilder.addComment(
        "warn if there is not keyspace.table for defined entity - it means that table is missing, or schema it out of date.");
    methodBuilder.beginControlFlow("else");
    loggingGenerator.warn(
        methodBuilder,
        "[{}] There is no ks.table or UDT: {}.{} for the entity class: {}, or metadata is out of date.",
        CodeBlock.of("context.getSession().getName()"),
        CodeBlock.of("keyspaceId"),
        CodeBlock.of("tableId"),
        CodeBlock.of("entityClassName"));
    methodBuilder.endControlFlow();
  }

  // handle case where keyspace name is not present in metadata keyspaces
  private void generateKeyspaceNameWrong(MethodSpec.Builder methodBuilder) {
    methodBuilder.beginControlFlow(
        "if(!keyspaceNamePresent(context.getSession().getMetadata().getKeyspaces(), keyspaceId))");
    loggingGenerator.warn(
        methodBuilder,
        "[{}] Unable to validate table: {} for the entity class: {} "
            + "because the session metadata has no information about the keyspace: {}.",
        CodeBlock.of("context.getSession().getName()"),
        CodeBlock.of("tableId"),
        CodeBlock.of("entityClassName"),
        CodeBlock.of("keyspaceId"));
    methodBuilder.addStatement("return");
    methodBuilder.endControlFlow();
  }

  // Handle case where keyspaceId = null.
  // In such case we cannot infer and validate schema for table or udt
  private void generateKeyspaceNull(MethodSpec.Builder methodBuilder) {
    methodBuilder.beginControlFlow("if (keyspaceId == null)");
    loggingGenerator.warn(
        methodBuilder,
        "[{}] Unable to validate table: {} for the entity class: {} because the keyspace "
            + "is unknown (the entity does not declare a default keyspace, and neither the "
            + "session nor the DAO were created with a keyspace). The DAO will only work if it "
            + "uses fully-qualified queries with @Query or @QueryProvider.",
        CodeBlock.of("context.getSession().getName()"),
        CodeBlock.of("tableId"),
        CodeBlock.of("entityClassName"));
    methodBuilder.addStatement("return");
    methodBuilder.endControlFlow();
  }

  private void generateValidationChecks(
      MethodSpec.Builder methodBuilder, Optional<TargetElement> targetElement) {
    // if SchemaHint was not provided explicitly try to match TABLE, then fallback to UDT
    if (!targetElement.isPresent()) {
      validateColumnsInTable(methodBuilder);
      validateColumnsInUdt(methodBuilder, true);
    }
    // if explicitly provided SchemaHint is TABLE, then generate only TABLE check
    else if (targetElement.get().equals(TargetElement.TABLE)) {
      validateColumnsInTable(methodBuilder);
    }
    // if explicitly provided SchemaHint is UDT, then generate only UDT check
    else if (targetElement.get().equals(TargetElement.UDT)) {
      validateColumnsInUdt(methodBuilder, false);
    }
  }

  private void validateColumnsInTable(MethodSpec.Builder methodBuilder) {
    methodBuilder.beginControlFlow("if (tableMetadata.isPresent())");

    generateMissingClusteringColumnsCheck(methodBuilder);

    generateMissingPKsCheck(methodBuilder);

    generateMissingColumnsCheck(methodBuilder);

    generateColumnsTypeCheck(methodBuilder);

    methodBuilder.endControlFlow();
  }

  private void generateColumnsTypeCheck(MethodSpec.Builder methodBuilder) {
    methodBuilder.addComment("validation of types");
    generateExpectedTypesPerColumn(methodBuilder);

    methodBuilder.addStatement(
        "$1T<$2T> missingTableTypes = findTypeMismatches(expectedTypesPerColumn, tableMetadata.get().getColumns(), context.getSession().getContext().getCodecRegistry())",
        List.class,
        String.class);
    methodBuilder.addStatement(
        "throwMissingTableTypesIfNotEmpty(missingTableTypes, keyspaceId, tableId, entityClassName)");
  }

  private void generateMissingColumnsCheck(MethodSpec.Builder methodBuilder) {
    methodBuilder.addComment("validation of all columns");

    methodBuilder.addStatement(
        "$1T<$2T> missingTableCqlNames = findMissingCqlIdentifiers(expectedCqlNames, tableMetadata.get().getColumns().keySet())",
        List.class,
        CqlIdentifier.class);

    // Throw if there are any missingTableCqlNames
    CodeBlock missingCqlColumnExceptionMessage =
        CodeBlock.of(
            "String.format(\"The CQL ks.table: %s.%s has missing columns: %s that are defined in the entity class: %s\", "
                + "keyspaceId, tableId, missingTableCqlNames, entityClassName)");
    methodBuilder.beginControlFlow("if (!missingTableCqlNames.isEmpty())");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingCqlColumnExceptionMessage);
    methodBuilder.endControlFlow();
  }

  private void generateMissingPKsCheck(MethodSpec.Builder methodBuilder) {
    methodBuilder.addComment("validation of missing PKs");
    List<CodeBlock> expectedCqlPKs =
        entityDefinition.getPartitionKey().stream()
            .map(PropertyDefinition::getCqlName)
            .collect(Collectors.toList());

    methodBuilder.addStatement(
        "$1T<$2T> expectedCqlPKs = new $3T<>()", List.class, CqlIdentifier.class, ArrayList.class);
    for (CodeBlock expectedCqlName : expectedCqlPKs) {
      methodBuilder.addStatement(
          "expectedCqlPKs.add($1T.fromCql($2L))", CqlIdentifier.class, expectedCqlName);
    }
    methodBuilder.addStatement(
        "$1T<$2T> missingTablePksNames = findMissingColumns(expectedCqlPKs, tableMetadata.get().getPartitionKey())",
        List.class,
        CqlIdentifier.class);

    // throw if there are any missing PK columns
    CodeBlock missingCqlColumnExceptionMessage =
        CodeBlock.of(
            "String.format(\"The CQL ks.table: %s.%s has missing Primary Key columns: %s that are defined in the entity class: %s\", "
                + "keyspaceId, tableId, missingTablePksNames, entityClassName)");
    methodBuilder.beginControlFlow("if (!missingTablePksNames.isEmpty())");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingCqlColumnExceptionMessage);
    methodBuilder.endControlFlow();
  }

  private void generateMissingClusteringColumnsCheck(MethodSpec.Builder methodBuilder) {
    List<CodeBlock> expectedCqlClusteringColumns =
        entityDefinition.getClusteringColumns().stream()
            .map(PropertyDefinition::getCqlName)
            .collect(Collectors.toList());

    if (!expectedCqlClusteringColumns.isEmpty()) {
      methodBuilder.addComment("validation of missing Clustering Columns");
      methodBuilder.addStatement(
          "$1T<$2T> expectedCqlClusteringColumns = new $3T<>()",
          List.class,
          CqlIdentifier.class,
          ArrayList.class);
      for (CodeBlock expectedCqlName : expectedCqlClusteringColumns) {
        methodBuilder.addStatement(
            "expectedCqlClusteringColumns.add($1T.fromCql($2L))",
            CqlIdentifier.class,
            expectedCqlName);
      }

      methodBuilder.addStatement(
          "$1T<$2T> missingTableClusteringColumnNames = findMissingColumns(expectedCqlClusteringColumns, tableMetadata.get().getClusteringColumns().keySet())",
          List.class,
          CqlIdentifier.class);

      // throw if there are any missing Clustering Columns columns
      CodeBlock missingCqlColumnExceptionMessage =
          CodeBlock.of(
              "String.format(\"The CQL ks.table: %s.%s has missing Clustering columns: %s that are defined in the entity class: %s\", "
                  + "keyspaceId, tableId, missingTableClusteringColumnNames, entityClassName)");
      methodBuilder.beginControlFlow("if (!missingTableClusteringColumnNames.isEmpty())");
      methodBuilder.addStatement(
          "throw new $1T($2L)", IllegalArgumentException.class, missingCqlColumnExceptionMessage);
      methodBuilder.endControlFlow();
    }
  }

  // Finds out missingTableCqlNames - columns that are present in Entity Mapping but NOT present in
  // UDT table
  private void validateColumnsInUdt(MethodSpec.Builder methodBuilder, boolean generateElse) {
    if (generateElse) {
      methodBuilder.beginControlFlow("else if (userDefinedType.isPresent())");
    } else {
      methodBuilder.beginControlFlow("if (userDefinedType.isPresent())");
    }

    generateUdtMissingColumnsCheck(methodBuilder);

    generateUdtColumnsTypeCheck(methodBuilder);

    methodBuilder.endControlFlow();
  }

  private void generateUdtColumnsTypeCheck(MethodSpec.Builder methodBuilder) {
    methodBuilder.addComment("validation of UDT types");
    generateExpectedTypesPerColumn(methodBuilder);

    methodBuilder.addStatement(
        "$1T<$2T> expectedColumns = userDefinedType.get().getFieldNames()",
        List.class,
        CqlIdentifier.class);
    methodBuilder.addStatement(
        "$1T<$2T> expectedTypes = userDefinedType.get().getFieldTypes()",
        List.class,
        DataType.class);

    methodBuilder.addStatement(
        "$1T<$2T> missingTableTypes = findTypeMismatches(expectedTypesPerColumn, expectedColumns, expectedTypes, context.getSession().getContext().getCodecRegistry())",
        List.class,
        String.class);
    methodBuilder.addStatement(
        "throwMissingUdtTypesIfNotEmpty(missingTableTypes, keyspaceId, tableId, entityClassName)");
  }

  private void generateUdtMissingColumnsCheck(MethodSpec.Builder methodBuilder) {
    methodBuilder.addComment("validation of UDT columns");
    methodBuilder.addStatement(
        "$1T<$2T> columns = userDefinedType.get().getFieldNames()",
        List.class,
        CqlIdentifier.class);

    methodBuilder.addStatement(
        "$1T<$2T> missingTableCqlNames = findMissingCqlIdentifiers(expectedCqlNames, columns)",
        List.class,
        CqlIdentifier.class);

    // Throw if there are any missingTableCqlNames
    CodeBlock missingCqlUdtExceptionMessage =
        CodeBlock.of(
            "String.format(\"The CQL ks.udt: %s.%s has missing columns: %s that are defined in the entity class: %s\", "
                + "keyspaceId, tableId, missingTableCqlNames, entityClassName)");
    methodBuilder.beginControlFlow("if (!missingTableCqlNames.isEmpty())");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingCqlUdtExceptionMessage);
    methodBuilder.endControlFlow();
  }

  private void generateExpectedTypesPerColumn(MethodSpec.Builder methodBuilder) {
    methodBuilder.addStatement(
        "$1T<$2T, $3T<?>> expectedTypesPerColumn = new $4T<>()",
        Map.class,
        CqlIdentifier.class,
        GenericType.class,
        LinkedHashMap.class);

    Map<CodeBlock, TypeName> expectedTypesPerColumn =
        entityDefinition.getAllColumns().stream()
            .collect(
                Collectors.toMap(PropertyDefinition::getCqlName, v -> v.getType().asRawTypeName()));

    for (Map.Entry<CodeBlock, TypeName> expected : expectedTypesPerColumn.entrySet()) {
      methodBuilder.addStatement(
          "expectedTypesPerColumn.put($1T.fromCql($2L), $3L)",
          CqlIdentifier.class,
          expected.getKey(),
          entityHelperGenerator.addGenericTypeConstant(expected.getValue().box()));
    }
  }
}
