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
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.querybuilder.update.DefaultUpdate;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperUpdateStartMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  EntityHelperUpdateStartMethodGenerator(
      EntityDefinition entityDefinition,
      EntityHelperGenerator enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder updateBuilder =
        MethodSpec.methodBuilder("updateStart")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(DefaultUpdate.class);

    if (!entityDefinition.getRegularColumns().iterator().hasNext()) {
      updateBuilder.addStatement(
          "throw new $T($S)",
          UnsupportedOperationException.class,
          String.format(
              "Entity %s does not have any non PK columns. %s is not possible",
              entityDefinition.getCqlName(), Update.class.getSimpleName()));
    } else {
      updateBuilder
          .addStatement("$T keyspaceId = context.getKeyspaceId()", CqlIdentifier.class)
          .addStatement("$T tableId = context.getTableId()", CqlIdentifier.class)
          .beginControlFlow("if (tableId == null)")
          .addStatement("tableId = DEFAULT_TABLE_ID")
          .endControlFlow()
          .addStatement(
              "$1T update = (keyspaceId == null)\n"
                  + "? $2T.update(tableId)\n"
                  + ": $2T.update(keyspaceId, tableId)",
              UpdateStart.class,
              QueryBuilder.class)
          .addCode("$[return (($1T)update", DefaultUpdate.class);

      for (PropertyDefinition property : entityDefinition.getRegularColumns()) {
        // we cannot use getAllColumns because update cannot SET for PKs
        updateBuilder.addCode(
            "\n.setColumn($1S, $2T.bindMarker($1S))", property.getCqlName(), QueryBuilder.class);
      }
      updateBuilder.addCode(")");
      updateBuilder.addCode("$];\n");
    }
    return Optional.of(updateBuilder.build());
  }
}
