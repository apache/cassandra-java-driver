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
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperDeleteByPrimaryKeyMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  public EntityHelperDeleteByPrimaryKeyMethodGenerator(
      EntityDefinition entityDefinition,
      EntityHelperGenerator enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder deleteByPrimaryKeyBuilder =
        MethodSpec.methodBuilder("deleteByPrimaryKey")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(Delete.class);

    if (entityDefinition.getPrimaryKey().isEmpty()) {
      deleteByPrimaryKeyBuilder.addStatement(
          "throw new $T($S)",
          UnsupportedOperationException.class,
          String.format(
              "Entity %s does not declare a primary key",
              entityDefinition.getClassName().simpleName()));
    } else {
      deleteByPrimaryKeyBuilder
          .addStatement("$T keyspaceId = context.getKeyspaceId()", CqlIdentifier.class)
          .addStatement("$T tableId = context.getTableId()", CqlIdentifier.class)
          .beginControlFlow("if (tableId == null)")
          .addStatement("tableId = defaultTableId")
          .endControlFlow()
          .addStatement(
              "$1T delete = (keyspaceId == null)\n"
                  + "? $2T.deleteFrom(tableId)\n"
                  + ": $2T.deleteFrom(keyspaceId, tableId)",
              DeleteSelection.class,
              QueryBuilder.class)
          .addCode("$[return delete");

      for (PropertyDefinition property : entityDefinition.getPrimaryKey()) {
        deleteByPrimaryKeyBuilder.addCode(
            "\n.whereColumn($1L).isEqualTo($2T.bindMarker($1L))",
            property.getCqlName(),
            QueryBuilder.class);
      }
      deleteByPrimaryKeyBuilder.addCode("$];\n");
    }
    return Optional.of(deleteByPrimaryKeyBuilder.build());
  }
}
