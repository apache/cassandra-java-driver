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
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperSelectByPrimaryKeyPartsMethodGenerator implements MethodGenerator {

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder selectByPrimaryKeyPartsBuilder =
        MethodSpec.methodBuilder("selectByPrimaryKeyParts")
            .addModifiers(Modifier.PUBLIC)
            .addParameter(TypeName.INT, "parameterCount")
            .returns(Select.class);

    selectByPrimaryKeyPartsBuilder.addStatement("$1T select = selectStart()", Select.class);
    selectByPrimaryKeyPartsBuilder.beginControlFlow(
        "for (int i = 0; i < parameterCount && i < " + "primaryKeys.size(); i++)");
    selectByPrimaryKeyPartsBuilder.addStatement(
        "$1T columnName = primaryKeys.get(i)", String.class);
    selectByPrimaryKeyPartsBuilder.addStatement(
        "select = select.whereColumn(columnName).isEqualTo($1T.bindMarker(columnName))",
        QueryBuilder.class);
    selectByPrimaryKeyPartsBuilder.endControlFlow();
    selectByPrimaryKeyPartsBuilder.addStatement("return select");

    return Optional.of(selectByPrimaryKeyPartsBuilder.build());
  }
}
