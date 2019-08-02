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
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperSelectStartMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  public EntityHelperSelectStartMethodGenerator(EntityDefinition entityDefinition) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder selectStartBuilder =
        MethodSpec.methodBuilder("selectStart")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(Select.class)
            .addStatement("throwIfKeyspaceMissing()")
            .addStatement(
                "$1T selectFrom = (keyspaceId == null)\n"
                    + "? $2T.selectFrom(tableId)\n"
                    + ": $2T.selectFrom(keyspaceId, tableId)",
                SelectFrom.class,
                QueryBuilder.class)
            .addCode("$[return selectFrom");

    for (PropertyDefinition property : entityDefinition.getAllColumns()) {
      selectStartBuilder.addCode("\n.column($L)", property.getSelector());
    }
    for (PropertyDefinition property : entityDefinition.getComputedValues()) {
      selectStartBuilder.addCode(
          "\n.raw($L).as($L)", property.getSelector(), property.getCqlName());
    }
    selectStartBuilder.addCode("$];\n");
    return Optional.of(selectStartBuilder.build());
  }
}
