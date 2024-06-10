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

import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperSelectByPrimaryKeyMethodGenerator implements MethodGenerator {

  @Override
  public Optional<MethodSpec> generate() {
    MethodSpec.Builder selectByPrimaryKeyBuilder =
        MethodSpec.methodBuilder("selectByPrimaryKey")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(Select.class)
            .addStatement("return selectByPrimaryKeyParts(primaryKeys.size())");

    return Optional.of(selectByPrimaryKeyBuilder.build());
  }
}
