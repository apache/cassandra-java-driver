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

import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperDeleteByPrimaryKeyMethodGenerator implements MethodGenerator {

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder deleteByPrimaryKeyBuilder =
        MethodSpec.methodBuilder("deleteByPrimaryKey")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(Delete.class)
            .addStatement("return deleteByPrimaryKeyParts(primaryKeys.size())");

    return Optional.of(deleteByPrimaryKeyBuilder.build());
  }
}
