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
package com.datastax.oss.driver.internal.mapper.processor.entity;

import static com.datastax.oss.driver.api.mapper.annotations.SchemaHint.*;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.mapper.annotations.SchemaHint;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class EntityHelperSchemaValidationMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;
  private TypeElement entityTypeElement;

  public EntityHelperSchemaValidationMethodGenerator(
      EntityDefinition entityDefinition, TypeElement entityTypeElement) {
    this.entityDefinition = entityDefinition;
    this.entityTypeElement = entityTypeElement;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder methodBuilder =
        MethodSpec.methodBuilder("validateEntityFields")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID);

    // get keyspaceId from context, and if not present fallback to keyspace set on session
    methodBuilder.addStatement(
        "$1T keyspaceId = context.getKeyspaceId() != null ? context.getKeyspaceId() : context.getSession().getKeyspace().orElse(null)",
        CqlIdentifier.class);

    // Handle case where keyspaceId = null. In such case we cannot infer and validate schema for
    // table
    // or udt
    methodBuilder.beginControlFlow(
        "if (!context.getSession().getMetadata().getKeyspace(keyspaceId).isPresent())");
    methodBuilder.addStatement(
        "return"); // todo maybe we should do log.warn("we cannot validate schema because keyspace
    // is null")?
    methodBuilder.endControlFlow();

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
        "$1T<$2T> tableMetadata = context.getSession().getMetadata().getKeyspace(keyspaceId).flatMap(v -> v.getTable(tableId))",
        Optional.class,
        TableMetadata.class);

    // Generated UserDefineTypes metadata
    methodBuilder.addStatement(
        "$1T<$2T> userDefinedType = context.getSession().getMetadata().getKeyspace(keyspaceId).flatMap(v -> v.getUserDefinedType(tableId))",
        Optional.class,
        UserDefinedType.class);

    methodBuilder.addStatement("String entityClassName = $S", entityDefinition.getClassName());

    generateFindMissingChecks(methodBuilder);

    // Throw if there is not keyspace.table for defined entity
    CodeBlock missingKeyspaceTableExceptionMessage =
        CodeBlock.of(
            "String.format(\"There is no ks.table: %s.%s for the entity class: %s\", keyspaceId, tableId, entityClassName)");
    methodBuilder.beginControlFlow("else");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingKeyspaceTableExceptionMessage);
    methodBuilder.endControlFlow();

    return Optional.of(methodBuilder.build());
  }

  private void generateFindMissingChecks(MethodSpec.Builder methodBuilder) {
    Optional<TargetElement> targetElement =
        Optional.ofNullable(entityTypeElement.getAnnotation(SchemaHint.class))
            .map(SchemaHint::targetElement);

    // if SchemaHint was not provided explicitly try to match TABLE, then fallback to UDT
    if (!targetElement.isPresent()) {
      findMissingColumnsInTable(methodBuilder);
      findMissingColumnsInUdt(methodBuilder, true);
    }
    // if explicitly provided SchemaHint is TABLE, then generate only TABLE check
    else if (targetElement.get().equals(TargetElement.TABLE)) {
      findMissingColumnsInTable(methodBuilder);
    }
    // if explicitly provided SchemaHint is UDT, then generate only UDT check
    else if (targetElement.get().equals(TargetElement.UDT)) {
      findMissingColumnsInUdt(methodBuilder, false);
    }
  }

  // Finds out missingTableCqlNames - columns that are present in Entity Mapping but NOT present in
  // CQL table
  private void findMissingColumnsInTable(MethodSpec.Builder methodBuilder) {
    methodBuilder.beginControlFlow("if (tableMetadata.isPresent())");

    // handle missing Clustering Columns
    generateMissingClusteringColumnsCheck(methodBuilder);

    // handle missing PKs
    generateMissingPKsCheck(methodBuilder);

    // handle all columns
    generateMissingColumnsCheck(methodBuilder);

    methodBuilder.endControlFlow();
  }

  private void generateMissingColumnsCheck(MethodSpec.Builder methodBuilder) {

    methodBuilder.addStatement(
        "$1T<$2T> missingTableCqlNames = findMissingColumnsCql(expectedCqlNames, (($3T) tableMetadata.get()).getColumns().keySet())",
        List.class,
        CqlIdentifier.class,
        DefaultTableMetadata.class);

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
    List<CodeBlock> expectedCqlPKs =
        entityDefinition.getPrimaryKey().stream()
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

  // Finds out missingTableCqlNames - columns that are present in Entity Mapping but NOT present in
  // UDT table
  private void findMissingColumnsInUdt(MethodSpec.Builder methodBuilder, boolean generateElse) {
    if (generateElse) {
      methodBuilder.beginControlFlow("else if (userDefinedType.isPresent())");
    } else {
      methodBuilder.beginControlFlow("if (userDefinedType.isPresent())");
    }

    methodBuilder.addStatement(
        "$1T<$2T> columns = userDefinedType.get().getFieldNames()",
        List.class,
        CqlIdentifier.class);

    methodBuilder.addStatement(
        "$1T<$2T> missingTableCqlNames = findMissingColumnsCql(expectedCqlNames, columns)",
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

    methodBuilder.endControlFlow();
  }
}
