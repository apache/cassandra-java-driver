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
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperDeleteStartMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  public EntityHelperDeleteStartMethodGenerator(
      EntityDefinition entityDefinition,
      EntityHelperGenerator enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder deleteStartBuilder =
        MethodSpec.methodBuilder("deleteStart")
            .addModifiers(Modifier.PUBLIC)
            .returns(DeleteSelection.class)
            .addStatement("throwIfKeyspaceMissing()")
            .addStatement(
                "return (keyspaceId == null)\n"
                    + "? $1T.deleteFrom(tableId)\n"
                    + ": $1T.deleteFrom(keyspaceId, tableId)",
                QueryBuilder.class);
    return Optional.of(deleteStartBuilder.build());
  }
}
