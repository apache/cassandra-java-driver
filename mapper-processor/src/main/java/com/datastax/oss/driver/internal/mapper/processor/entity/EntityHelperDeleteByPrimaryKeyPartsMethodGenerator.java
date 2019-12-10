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

import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperDeleteByPrimaryKeyPartsMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  public EntityHelperDeleteByPrimaryKeyPartsMethodGenerator(EntityDefinition entityDefinition) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder deleteByPrimaryKeyPartsBuilder =
        MethodSpec.methodBuilder("deleteByPrimaryKeyParts")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.INT, "parameterCount")
            .returns(Delete.class);

    if (entityDefinition.getPrimaryKey().isEmpty()) {
      deleteByPrimaryKeyPartsBuilder.addStatement(
          "throw new $T($S)",
          MapperException.class,
          String.format(
              "Entity %s does not declare a primary key",
              entityDefinition.getClassName().simpleName()));
    } else {
      deleteByPrimaryKeyPartsBuilder.beginControlFlow("if (parameterCount <= 0)");
      deleteByPrimaryKeyPartsBuilder.addStatement(
          "throw new $T($S)", MapperException.class, "parameterCount must be greater than 0");
      deleteByPrimaryKeyPartsBuilder.endControlFlow();

      deleteByPrimaryKeyPartsBuilder.addStatement(
          "$1T deleteSelection = deleteStart()", DeleteSelection.class);

      deleteByPrimaryKeyPartsBuilder.addStatement(
          "$1T columnName = primaryKeys.get(0)", String.class);
      deleteByPrimaryKeyPartsBuilder.addStatement(
          "$1T delete = deleteSelection.whereColumn(columnName).isEqualTo($2T.bindMarker"
              + "(columnName))",
          Delete.class,
          QueryBuilder.class);
      deleteByPrimaryKeyPartsBuilder.beginControlFlow(
          "for (int i = 1; i < parameterCount && i < " + "primaryKeys.size(); i++)");
      deleteByPrimaryKeyPartsBuilder.addStatement("columnName = primaryKeys.get(i)");
      deleteByPrimaryKeyPartsBuilder.addStatement(
          "delete = delete.whereColumn(columnName).isEqualTo($1T.bindMarker(columnName))",
          QueryBuilder.class);
      deleteByPrimaryKeyPartsBuilder.endControlFlow();
      deleteByPrimaryKeyPartsBuilder.addStatement("return delete");
    }
    return Optional.of(deleteByPrimaryKeyPartsBuilder.build());
  }
}
