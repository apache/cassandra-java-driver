/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.processor.entity;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.querybuilder.update.DefaultUpdate;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperUpdateByPrimaryKeyMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;

  EntityHelperUpdateByPrimaryKeyMethodGenerator(EntityDefinition entityDefinition) {
    this.entityDefinition = entityDefinition;
  }

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder methodBuilder =
        MethodSpec.methodBuilder("updateByPrimaryKey")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(DefaultUpdate.class)
            .addCode("$[return (($T)updateStart()", DefaultUpdate.class);

    for (PropertyDefinition property : entityDefinition.getPrimaryKey()) {
      methodBuilder.addCode(
          "\n.where($1T.column($2L).isEqualTo($3T.bindMarker($2L)))",
          Relation.class,
          property.getCqlName(),
          QueryBuilder.class);
    }

    methodBuilder.addCode(")");
    methodBuilder.addCode("$];\n");
    return Optional.of(methodBuilder.build());
  }
}
