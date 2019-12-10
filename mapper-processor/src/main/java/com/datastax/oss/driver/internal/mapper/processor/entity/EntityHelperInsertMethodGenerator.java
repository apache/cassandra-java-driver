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

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperInsertMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  public EntityHelperInsertMethodGenerator(EntityDefinition entityDefinition) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder insertBuilder =
        MethodSpec.methodBuilder("insert")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(RegularInsert.class)
            .addStatement("throwIfKeyspaceMissing()")
            .addStatement(
                "$1T insertInto = (keyspaceId == null)\n"
                    + "? $2T.insertInto(tableId)\n"
                    + ": $2T.insertInto(keyspaceId, tableId)",
                InsertInto.class,
                QueryBuilder.class)
            .addCode("$[return insertInto");

    for (PropertyDefinition property : entityDefinition.getAllColumns()) {
      insertBuilder.addCode(
          "\n.value($1L, $2T.bindMarker($1L))", property.getCqlName(), QueryBuilder.class);
    }
    insertBuilder.addCode("$];\n");
    return Optional.of(insertBuilder.build());
  }
}
