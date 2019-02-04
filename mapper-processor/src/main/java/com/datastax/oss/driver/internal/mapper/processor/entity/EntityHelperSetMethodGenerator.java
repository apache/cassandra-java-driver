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

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.mapper.processor.PartialClassGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import com.squareup.javapoet.TypeVariableName;
import java.util.Map;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class EntityHelperSetMethodGenerator implements PartialClassGenerator {

  /**
   * The names of the primitive getters/setters on {@link GettableByName} and {@link
   * SettableByName}.
   */
  private static final Map<TypeName, String> PRIMITIVE_ACCESSORS =
      ImmutableMap.<TypeName, String>builder()
          .put(TypeName.BOOLEAN, "Boolean")
          .put(TypeName.BYTE, "Byte")
          .put(TypeName.DOUBLE, "Double")
          .put(TypeName.FLOAT, "Float")
          .put(TypeName.INT, "Int")
          .put(TypeName.LONG, "Long")
          .build();

  private final EntityDefinition entityDefinition;
  private final EntityHelperGenerator enclosingClass;

  public EntityHelperSetMethodGenerator(
      EntityDefinition entityDefinition,
      EntityHelperGenerator enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
    this.enclosingClass = enclosingClass;
  }

  @Override
  public void addMembers(TypeSpec.Builder classBuilder) {

    // TODO add an ignore mechanism? this fails if a property is missing on the target.
    // TODO handle collections of UDTs (JAVA-2129)
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

    int udtIndex = 0;
    for (PropertyDefinition property : entityDefinition.getProperties()) {
      TypeName type = property.getType();
      String cqlName = property.getCqlName();
      String getterName = property.getGetterName();
      TypeElement childEntityElement = property.getEntityElement();
      injectBuilder.addCode("\n");
      if (childEntityElement != null) {
        // Other entity class: the CQL column is a mapped UDT
        udtIndex += 1;
        String udtTypeName = "udtType" + udtIndex;
        String udtValueName = "udtValue" + udtIndex;
        String valueName = "value" + udtIndex;
        // Extract the child entity, make sure it's not null
        injectBuilder
            .addStatement("$T $L = entity.$L()", type, valueName, getterName)
            .beginControlFlow("if ($L != null)", valueName);
        // Retrieve the UDT type
        injectBuilder.addStatement(
            "$1T $2L = ($1T) target.getType($3S)", UserDefinedType.class, udtTypeName, cqlName);
        // Create a new UdtValue from it
        injectBuilder.addStatement(
            "$T $L = $L.newValue()", UdtValue.class, udtValueName, udtTypeName);
        // Inject the child entity into the UdtValue
        String childHelper = enclosingClass.addChildHelper(childEntityElement);
        injectBuilder.addStatement("$L.set($L, $L)", childHelper, valueName, udtValueName);
        // Set the UdtValue into the target
        injectBuilder
            .addStatement("target = target.setUdtValue($S, $L)", cqlName, udtValueName)
            .endControlFlow();
      } else {
        String primitiveAccessor = PRIMITIVE_ACCESSORS.get(type);
        if (primitiveAccessor != null) {
          // Primitive type: use dedicated setter, since it is optimized to avoid boxing
          injectBuilder.addStatement(
              "target = target.set$L($S, entity.$L())", primitiveAccessor, cqlName, getterName);
        } else if (type instanceof ClassName) {
          // Unparameterized class: use the generic, class-based setter:
          injectBuilder.addStatement(
              "target = target.set($S, entity.$L(), $T.class)", cqlName, getterName, type);
        } else {
          // Parameterized type: create a GenericType constant
          // Note that lists, sets and maps of unparameterized classes also fall under that
          // category. Their setter creates a GenericType under the hood, so there's no performance
          // advantage in calling them instead of the generic set().
          injectBuilder.addStatement(
              "target = target.set($S, entity.$L(), $L)",
              cqlName,
              getterName,
              enclosingClass.addGenericTypeConstant(type));
        }
      }
    }
    injectBuilder.addCode("\n").addStatement("return target");
    classBuilder.addMethod(injectBuilder.build());
  }

  @Override
  public void addConstructorInstructions(MethodSpec.Builder constructorBuilder) {
    // nothing to do
  }
}
