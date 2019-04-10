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
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.internal.mapper.processor.PartialClassGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import javax.lang.model.element.Modifier;

public class EntityHelperInsertMethodGenerator implements PartialClassGenerator {

  private final EntityDefinition entityDefinition;

  public EntityHelperInsertMethodGenerator(
      EntityDefinition entityDefinition,
      EntityHelperGenerator enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public void addMembers(TypeSpec.Builder classBuilder) {

    MethodSpec.Builder insertBuilder =
        MethodSpec.methodBuilder("insert")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(BuildableQuery.class)
            .addStatement("$T keyspaceId = context.getKeyspaceId()", CqlIdentifier.class)
            .addStatement("$T tableId = context.getTableId()", CqlIdentifier.class)
            .beginControlFlow("if (tableId == null)")
            .addStatement("tableId = DEFAULT_TABLE_ID")
            .endControlFlow()
            .addStatement(
                "$1T insertInto = (keyspaceId == null)\n"
                    + "? $2T.insertInto(tableId)\n"
                    + ": $2T.insertInto(keyspaceId, tableId)",
                InsertInto.class,
                QueryBuilder.class)
            .addCode("$[return insertInto");

    for (PropertyDefinition property : entityDefinition.getProperties()) {
      insertBuilder.addCode(
          "\n.value($1S, $2T.bindMarker($1S))", property.getCqlName(), QueryBuilder.class);
    }
    insertBuilder.addCode("$];\n");

    classBuilder.addMethod(insertBuilder.build());
  }

  @Override
  public void addConstructorInstructions(MethodSpec.Builder constructorBuilder) {
    // nothing to do
  }
}
