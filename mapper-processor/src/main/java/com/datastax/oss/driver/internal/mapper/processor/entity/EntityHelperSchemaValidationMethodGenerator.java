package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.internal.core.metadata.schema.DefaultTableMetadata;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.Map;
import java.util.Optional;
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

    methodBuilder.beginControlFlow("if (tableMetadata.isPresent())");
    methodBuilder.addStatement(
        "$1T<$2T,$3T> columns = (($4T) tableMetadata.get()).getColumns()",
        Map.class,
        CqlIdentifier.class,
        ColumnMetadata.class,
        DefaultTableMetadata.class);
    methodBuilder.endControlFlow();

    return Optional.of(methodBuilder.build());
  }
}
