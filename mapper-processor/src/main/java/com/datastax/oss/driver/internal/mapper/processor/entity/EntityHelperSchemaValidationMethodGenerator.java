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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.lang.model.element.Modifier;

public class EntityHelperSchemaValidationMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  public EntityHelperSchemaValidationMethodGenerator(
      EntityDefinition entityDefinition,
      EntityHelperGenerator entityHelperGenerator,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder methodBuilder =
        MethodSpec.methodBuilder("validateEntityFields")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(TypeName.VOID);

    methodBuilder
        .addStatement("$T tableId = context.getTableId()", CqlIdentifier.class)
        .beginControlFlow("if (tableId == null)")
        .addStatement("tableId = defaultTableId")
        .endControlFlow();

    // Generates expected names to be present in cql (table or udt)
    List<CodeBlock> expectedCqlNames =
        entityDefinition
            .getAllColumns()
            .stream()
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

    // Generates TableMetadata - Assumes that MapperContext context is already defined
    methodBuilder.addStatement("$T finalTableId = tableId", CqlIdentifier.class);
    methodBuilder.addStatement(
        "$1T<$2T> tableMetadata = context.getSession().getMetadata().getKeyspace(context.getKeyspaceId()).flatMap(v -> v.getTable(finalTableId))",
        Optional.class,
        TableMetadata.class);

    // Generated UserDefineTypes metadata
    methodBuilder.addStatement(
        "$1T<$2T> userDefinedType = context.getSession().getMetadata().getKeyspace(context.getKeyspaceId()).flatMap(v -> v.getUserDefinedType(finalTableId))",
        Optional.class,
        UserDefinedType.class);

    methodBuilder.addStatement("String entityClassName = $S", entityDefinition.getClassName());

    findMissingColumnsInTable(methodBuilder);
    findMissingColumnsInUdt(methodBuilder);

    // Throw if there is not keyspace.table for defined entity
    CodeBlock missingKeyspaceTableExceptionMessage =
        CodeBlock.of(
            "String.format(\"There is no ks.table: %s.%s for the entity class: %s\", context.getKeyspaceId(), tableId, entityClassName)");
    methodBuilder.beginControlFlow("else");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingKeyspaceTableExceptionMessage);
    methodBuilder.endControlFlow();

    return Optional.of(methodBuilder.build());
  }

  // Finds out missingTableCqlNames - columns that are present in Entity Mapping but NOT present in
  // CQL table
  private void findMissingColumnsInTable(MethodSpec.Builder methodBuilder) {
    methodBuilder.beginControlFlow("if (tableMetadata.isPresent())");
    methodBuilder.addStatement(
        "$1T<$2T, $3T> columns = (($4T) tableMetadata.get()).getColumns()",
        Map.class,
        CqlIdentifier.class,
        ColumnMetadata.class,
        DefaultTableMetadata.class);

    methodBuilder.addStatement(
        "$1T<$2T> missingTableCqlNames = new $3T<>();",
        List.class,
        CqlIdentifier.class,
        ArrayList.class);
    methodBuilder.beginControlFlow(
        "for ($1T cqlIdentifier : expectedCqlNames)", CqlIdentifier.class);
    methodBuilder.beginControlFlow("if (columns.get(cqlIdentifier) == null)");
    methodBuilder.addStatement("missingTableCqlNames.add(cqlIdentifier)");
    methodBuilder.endControlFlow();
    methodBuilder.endControlFlow();

    // Throw if there are any missingTableCqlNames
    CodeBlock missingCqlColumnExceptionMessage =
        CodeBlock.of(
            "String.format(\"The CQL ks.table: %s.%s has missing columns: %s that are defined in the entity class: %s\", "
                + "context.getKeyspaceId(), tableId, missingTableCqlNames, entityClassName)");
    methodBuilder.beginControlFlow("if (!missingTableCqlNames.isEmpty())");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingCqlColumnExceptionMessage);
    methodBuilder.endControlFlow();
    methodBuilder.endControlFlow();
  }

  // Finds out missingTableCqlNames - columns that are present in Entity Mapping but NOT present in
  // UDT table
  private void findMissingColumnsInUdt(MethodSpec.Builder methodBuilder) {
    methodBuilder.beginControlFlow("else if (userDefinedType.isPresent())");

    methodBuilder.addStatement(
        "$1T<$2T> columns = userDefinedType.get().getFieldNames()",
        List.class,
        CqlIdentifier.class);

    methodBuilder.addStatement(
        "$1T<$2T> missingTableCqlNames = new $3T<>();",
        List.class,
        CqlIdentifier.class,
        ArrayList.class);
    methodBuilder.beginControlFlow(
        "for ($1T cqlIdentifier : expectedCqlNames)", CqlIdentifier.class);
    methodBuilder.beginControlFlow("if (!columns.contains(cqlIdentifier))");
    methodBuilder.addStatement("missingTableCqlNames.add(cqlIdentifier)");
    methodBuilder.endControlFlow();
    methodBuilder.endControlFlow();

    // Throw if there are any missingTableCqlNames
    CodeBlock missingCqlUdtExceptionMessage =
        CodeBlock.of(
            "String.format(\"The CQL ks.udt: %s.%s has missing columns: %s that are defined in the entity class: %s\", "
                + "context.getKeyspaceId(), tableId, missingTableCqlNames, entityClassName)");
    methodBuilder.beginControlFlow("if (!missingTableCqlNames.isEmpty())");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingCqlUdtExceptionMessage);
    methodBuilder.endControlFlow();

    methodBuilder.endControlFlow();
  }
}
