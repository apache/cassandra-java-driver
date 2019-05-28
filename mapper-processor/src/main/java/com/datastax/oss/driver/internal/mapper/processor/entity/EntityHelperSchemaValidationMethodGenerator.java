package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
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

    // Generates TableMetadata - Assumes that MapperContext context is already defined
    methodBuilder.addStatement(
        "$1T<$2T> tableMetadata = context.getSession().getMetadata().getKeyspace(context.getKeyspaceId()).flatMap(v -> v.getTable(context.getTableId()))",
        Optional.class,
        TableMetadata.class);

    // Find out missingTableCqlNames - present in Entity Mapping but NOT present in CQL table
    methodBuilder.beginControlFlow("if (tableMetadata.isPresent())");
    methodBuilder.addStatement(
        "$1T<$2T,$3T> columns = (($4T) tableMetadata.get()).getColumns()",
        Map.class,
        CqlIdentifier.class,
        ColumnMetadata.class,
        DefaultTableMetadata.class);
    methodBuilder.addStatement(
        "$1T<$2T> expectedCqlNames = entityDefinition.getAllColumns().stream().map(v -> v.getCqlName()).collect($3T.toList())",
        List.class,
        CodeBlock.class,
        Collectors.class);
    methodBuilder.addStatement(
        "$1T<$2T> missingTableCqlNames = new $3T<>();",
        List.class,
        CqlIdentifier.class,
        ArrayList.class);
    methodBuilder.beginControlFlow("for ($1T cqlName : expectedCqlNames)", CodeBlock.class);
    methodBuilder.addStatement(
        "$1T cqlIdentifier = $1T.fromCql(cqlName.toString())", CqlIdentifier.class);
    methodBuilder.beginControlFlow("if (columns.get(cqlIdentifier) == null)");
    methodBuilder.addStatement("missingTableCqlNames.add(cqlIdentifier)");
    methodBuilder.endControlFlow();
    methodBuilder.endControlFlow();

    // Throw if there are any missingTableCqlNames
    CodeBlock missingCqlColumnExceptionMessage =
        CodeBlock.of(
            "String.format(\"The CQL ks.table: %s.%s has missing columns: %s that are defined in the entity class: %s\", "
                + "context.getKeyspaceId(), context.getTableId(), missingTableCqlNames, entityDefinition.getClassName())");
    methodBuilder.beginControlFlow("if (!missingTableCqlNames.isEmpty())");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingCqlColumnExceptionMessage);
    methodBuilder.endControlFlow();
    methodBuilder.endControlFlow();

    // Throw if there is not keyspace.table for defined entity
    CodeBlock missingKeyspaceTableExceptionMessage =
        CodeBlock.of(
            "String.format(\"There is no ks.table: %s.%s\", context.getKeyspaceId(), context.getTableId())");
    methodBuilder.beginControlFlow("else");
    methodBuilder.addStatement(
        "throw new $1T($2L)", IllegalArgumentException.class, missingKeyspaceTableExceptionMessage);
    methodBuilder.endControlFlow();

    return Optional.of(methodBuilder.build());
  }
}
