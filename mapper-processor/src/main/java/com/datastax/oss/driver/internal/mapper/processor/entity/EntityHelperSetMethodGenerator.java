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

import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.internal.mapper.processor.PartialClassGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.BindableHandlingSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import javax.lang.model.element.Modifier;

public class EntityHelperSetMethodGenerator implements PartialClassGenerator {

  private final EntityDefinition entityDefinition;
  private final BindableHandlingSharedCode enclosingClass;

  public EntityHelperSetMethodGenerator(
      EntityDefinition entityDefinition,
      BindableHandlingSharedCode enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
    this.enclosingClass = enclosingClass;
  }

  @Override
  public void addMembers(TypeSpec.Builder classBuilder) {

    // TODO add an ignore mechanism? this fails if a property is missing on the target.
    // TODO different strategies for null values? (null vs unset)

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
            .returns(settableT);

    for (PropertyDefinition property : entityDefinition.getProperties()) {
      GeneratedCodePatterns.setValue(
          property.getCqlName(),
          property.getType(),
          property.getEntityElement(),
          CodeBlock.of("entity.$L()", property.getGetterName()),
          "target",
          injectBuilder,
          enclosingClass);
    }
    injectBuilder.addCode("\n").addStatement("return target");
    classBuilder.addMethod(injectBuilder.build());
  }

  @Override
  public void addConstructorInstructions(MethodSpec.Builder constructorBuilder) {
    // nothing to do
  }
}
