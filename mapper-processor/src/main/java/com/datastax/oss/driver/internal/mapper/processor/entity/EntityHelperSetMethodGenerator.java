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

import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.BindableHandlingSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeVariableName;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperSetMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;
  private final BindableHandlingSharedCode enclosingClass;

  public EntityHelperSetMethodGenerator(
      EntityDefinition entityDefinition, BindableHandlingSharedCode enclosingClass) {
    this.entityDefinition = entityDefinition;
    this.enclosingClass = enclosingClass;
  }

  @Override
  public Optional<MethodSpec> generate() {

    // The method's type variable: <SettableT extends SettableByName<SettableT>>
    TypeVariableName settableT = TypeVariableName.get("SettableT");
    settableT =
        settableT.withBounds(
            ParameterizedTypeName.get(ClassName.get(SettableByName.class), settableT));

    MethodSpec.Builder injectBuilder =
        MethodSpec.methodBuilder("set")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addTypeVariable(settableT)
            .addParameter(ParameterSpec.builder(entityDefinition.getClassName(), "entity").build())
            .addParameter(ParameterSpec.builder(settableT, "target").build())
            .addParameter(
                ParameterSpec.builder(NullSavingStrategy.class, "nullSavingStrategy").build())
            .addParameter(ParameterSpec.builder(TypeName.BOOLEAN, "lenient").build())
            .returns(settableT);

    CodeBlock.Builder injectBodyBuilder = CodeBlock.builder();
    for (PropertyDefinition property : entityDefinition.getAllColumns()) {

      injectBodyBuilder.beginControlFlow(
          "if (!lenient || hasProperty(target, $L))", property.getCqlName());

      GeneratedCodePatterns.setValue(
          property.getCqlName(),
          property.getType(),
          CodeBlock.of("entity.$L()", property.getGetterName()),
          "target",
          injectBodyBuilder,
          enclosingClass,
          true,
          true);

      injectBodyBuilder.endControlFlow();
    }
    injectBodyBuilder.addStatement("return target");
    return Optional.of(injectBuilder.addCode(injectBodyBuilder.build()).build());
  }
}
