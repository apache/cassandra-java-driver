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
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.internal.mapper.processor.PartialClassGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.Map;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;

public class EntityHelperGetMethodGenerator implements PartialClassGenerator {

  private final EntityDefinition entityDefinition;

  private static final Map<TypeName, String> PRIMITIVE_ACCESSORS =
      ImmutableMap.<TypeName, String>builder()
          .put(TypeName.BOOLEAN, "Boolean")
          .put(TypeName.BYTE, "Byte")
          .put(TypeName.DOUBLE, "Double")
          .put(TypeName.FLOAT, "Float")
          .put(TypeName.INT, "Int")
          .put(TypeName.LONG, "Long")
          .build();

  private final EntityHelperGenerator enclosingClass;

  public EntityHelperGetMethodGenerator(
      EntityDefinition entityDefinition,
      EntityHelperGenerator enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
    this.enclosingClass = enclosingClass;
  }

  @Override
  public void addMembers(TypeSpec.Builder classBuilder) {

    MethodSpec.Builder getBuilder =
        MethodSpec.methodBuilder("get")
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .addParameter(
                ParameterSpec.builder(ClassName.get(GettableByName.class), "source").build())
            .returns(entityDefinition.getClassName());

    TypeName returnType = entityDefinition.getClassName();
    String returnName = "returnValue";
    getBuilder.addStatement("$1T $2L = new $1T()", returnType, returnName);

    int udtIndex = 0;
    for (PropertyDefinition property : entityDefinition.getProperties()) {
      TypeName type = property.getType();
      String cqlName = property.getCqlName();
      String setterName = property.getSetterName();
      TypeElement childEntityElement = property.getEntityElement();
      getBuilder.addCode("\n");
      if (childEntityElement != null) {
        udtIndex += 1;
        // Populate udtInformation
        String udtValueName = "udtValue" + udtIndex;
        String valueName = "value" + udtIndex;
        // Extract UdtValue to pass it on to underlying helper method
        getBuilder.addStatement(
            "$T $L = source.getUdtValue($S)", UdtValue.class, udtValueName, cqlName);
        getBuilder.beginControlFlow("if ($L != null)", udtValueName);
        // Get underlying udt object and set it on return type
        String childHelper = enclosingClass.addChildHelper(childEntityElement);
        getBuilder.addStatement("$T $L = $L.get($L)", type, valueName, childHelper, udtValueName);
        getBuilder.addStatement("returnValue.$L($L)", setterName, valueName);
        getBuilder.endControlFlow();
      } else {
        String primitiveAccessor = PRIMITIVE_ACCESSORS.get(type);
        if (primitiveAccessor != null) {
          // Primitive type: use dedicated getter, since it is optimized to avoid boxing
          getBuilder.addStatement(
              "returnValue.$L(source.get$L($S))", setterName, primitiveAccessor, cqlName);
        } else if (type instanceof ClassName) {
          // Unparameterized class: use the generic, class-based getter:
          getBuilder.addStatement(
              "returnValue.$L(source.get($S, $T.class))", setterName, cqlName, type);
        } else {
          // Parameterized type: create a GenericType constant
          getBuilder.addStatement(
              "returnValue.$L(source.get($S, $T))",
              setterName,
              cqlName,
              enclosingClass.addGenericTypeConstant(type));
        }
      }
    }
    getBuilder.addStatement("return returnValue");

    classBuilder.addMethod(getBuilder.build());
  }

  @Override
  public void addConstructorInstructions(MethodSpec.Builder constructorBuilder) {
    // nothing to do
  }
}
