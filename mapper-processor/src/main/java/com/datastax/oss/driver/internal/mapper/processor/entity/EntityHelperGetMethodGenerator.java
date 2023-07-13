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

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.BindableHandlingSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.PropertyType;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.util.Map;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperGetMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;
  private final BindableHandlingSharedCode enclosingClass;

  public EntityHelperGetMethodGenerator(
      EntityDefinition entityDefinition,
      BindableHandlingSharedCode enclosingClass,
      ProcessorContext context) {
    this.entityDefinition = entityDefinition;
    this.enclosingClass = enclosingClass;
  }

  @Override
  public Optional<MethodSpec> generate() {
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

    for (PropertyDefinition property : entityDefinition.getAllValues()) {
      PropertyType type = property.getType();
      CodeBlock cqlName = property.getCqlName();
      String setterName = property.getSetterName();
      getBuilder.addCode("\n");
      if (type instanceof PropertyType.Simple) {
        TypeName typeName = ((PropertyType.Simple) type).typeName;
        String primitiveAccessor = GeneratedCodePatterns.PRIMITIVE_ACCESSORS.get(typeName);
        if (primitiveAccessor != null) {
          // Primitive type: use dedicated getter, since it is optimized to avoid boxing
          //     returnValue.setLength(source.getInt("length"));
          getBuilder.addStatement(
              "returnValue.$L(source.get$L($L))", setterName, primitiveAccessor, cqlName);
        } else if (typeName instanceof ClassName) {
          // Unparameterized class: use the generic, class-based getter:
          //     returnValue.setId(source.get("id", UUID.class));
          getBuilder.addStatement(
              "returnValue.$L(source.get($L, $T.class))", setterName, cqlName, typeName);
        } else {
          // Parameterized type: create a constant and use the GenericType-based getter:
          //     private static final GenericType<List<String>> GENERIC_TYPE =
          //         new GenericType<List<String>>(){};
          //     returnValue.setNames(source.get("names", GENERIC_TYPE));
          // Note that lists, sets and maps of unparameterized classes also fall under that
          // category. Their getter creates a GenericType under the hood, so there's no performance
          // advantage in calling them instead of the generic get().
          getBuilder.addStatement(
              "returnValue.$L(source.get($L, $L))",
              setterName,
              cqlName,
              enclosingClass.addGenericTypeConstant(typeName));
        }
      } else if (type instanceof PropertyType.SingleEntity) {
        ClassName entityClass = ((PropertyType.SingleEntity) type).entityName;
        // Other entity class: the CQL column is a mapped UDT. Example of generated code:
        //     UdtValue udtValue1 = source.getUdtValue("dimensions");
        //     if (udtValue1 != null) {
        //       Dimensions value1 = dimensionsHelper.get(udtValue1);
        //       returnValue.setDimensions(value1);
        //     }

        // Populate udtInformation
        String udtValueName = enclosingClass.getNameIndex().uniqueField("udtValue");
        String valueName = enclosingClass.getNameIndex().uniqueField("value");
        // Extract UdtValue to pass it on to underlying helper method
        getBuilder.addStatement(
            "$T $L = source.getUdtValue($L)", UdtValue.class, udtValueName, cqlName);
        getBuilder.beginControlFlow("if ($L != null)", udtValueName);
        // Get underlying udt object and set it on return type
        String childHelper = enclosingClass.addEntityHelperField(entityClass);
        getBuilder.addStatement(
            "$T $L = $L.get($L)", entityClass, valueName, childHelper, udtValueName);
        getBuilder.addStatement("returnValue.$L($L)", setterName, valueName);
        getBuilder.endControlFlow();
      } else {
        // Collection of other entity class(es): the CQL column is a collection of mapped UDTs
        // Build a copy of the value, decoding all UdtValue instances into entities on the fly.
        String mappedCollectionName = enclosingClass.getNameIndex().uniqueField("mappedCollection");
        String rawCollectionName = enclosingClass.getNameIndex().uniqueField("rawCollection");
        TypeName rawCollectionType = type.asRawTypeName();
        getBuilder
            .addStatement(
                "$T $L = source.get($L, $L)",
                rawCollectionType,
                rawCollectionName,
                cqlName,
                enclosingClass.addGenericTypeConstant(rawCollectionType))
            .beginControlFlow("if ($L != null)", rawCollectionName);
        convertUdtsIntoEntities(rawCollectionName, mappedCollectionName, type, getBuilder);
        getBuilder
            .addStatement("returnValue.$L($L)", setterName, mappedCollectionName)
            .endControlFlow();
      }
    }
    getBuilder.addStatement("return returnValue");
    return Optional.of(getBuilder.build());
  }

  /**
   * Generates the code to convert a collection of UDT instances, for example a {@code Map<String,
   * UdtValue>} into a {@code Map<String, Product>}.
   *
   * @param rawObjectName the name of the local variable containing the value to convert.
   * @param mappedObjectName the name of the local variable that will hold the converted value (it
   *     does not exist yet, this method must generate the declaration).
   * @param type the type of the value.
   * @param getBuilder the method where the generated code will be appended.
   */
  private void convertUdtsIntoEntities(
      String rawObjectName,
      String mappedObjectName,
      PropertyType type,
      MethodSpec.Builder getBuilder) {

    if (type instanceof PropertyType.SingleEntity) {
      ClassName entityClass = ((PropertyType.SingleEntity) type).entityName;
      String entityHelperName = enclosingClass.addEntityHelperField(entityClass);
      getBuilder.addStatement(
          "$T $L = $L.get($L)",
          type.asTypeName(),
          mappedObjectName,
          entityHelperName,
          rawObjectName);
    } else if (type instanceof PropertyType.EntityList) {
      getBuilder.addStatement(
          "$T $L = $T.newArrayListWithExpectedSize($L.size())",
          type.asTypeName(),
          mappedObjectName,
          Lists.class,
          rawObjectName);
      PropertyType mappedElementType = ((PropertyType.EntityList) type).elementType;
      TypeName rawElementType = mappedElementType.asRawTypeName();
      String rawElementName = enclosingClass.getNameIndex().uniqueField("rawElement");
      getBuilder.beginControlFlow("for ($T $L: $L)", rawElementType, rawElementName, rawObjectName);
      String mappedElementName = enclosingClass.getNameIndex().uniqueField("mappedElement");
      convertUdtsIntoEntities(rawElementName, mappedElementName, mappedElementType, getBuilder);
      getBuilder.addStatement("$L.add($L)", mappedObjectName, mappedElementName).endControlFlow();
    } else if (type instanceof PropertyType.EntitySet) {
      getBuilder.addStatement(
          "$T $L = $T.newLinkedHashSetWithExpectedSize($L.size())",
          type.asTypeName(),
          mappedObjectName,
          Sets.class,
          rawObjectName);
      PropertyType mappedElementType = ((PropertyType.EntitySet) type).elementType;
      TypeName rawElementType = mappedElementType.asRawTypeName();
      String rawElementName = enclosingClass.getNameIndex().uniqueField("rawElement");
      getBuilder.beginControlFlow("for ($T $L: $L)", rawElementType, rawElementName, rawObjectName);
      String mappedElementName = enclosingClass.getNameIndex().uniqueField("mappedElement");
      convertUdtsIntoEntities(rawElementName, mappedElementName, mappedElementType, getBuilder);
      getBuilder.addStatement("$L.add($L)", mappedObjectName, mappedElementName).endControlFlow();
    } else if (type instanceof PropertyType.EntityMap) {
      getBuilder.addStatement(
          "$T $L = $T.newLinkedHashMapWithExpectedSize($L.size())",
          type.asTypeName(),
          mappedObjectName,
          Maps.class,
          rawObjectName);
      PropertyType mappedKeyType = ((PropertyType.EntityMap) type).keyType;
      PropertyType mappedValueType = ((PropertyType.EntityMap) type).valueType;
      String rawEntryName = enclosingClass.getNameIndex().uniqueField("rawEntry");
      getBuilder.beginControlFlow(
          "for ($T $L: $L.entrySet())",
          ParameterizedTypeName.get(
              ClassName.get(Map.Entry.class),
              mappedKeyType.asRawTypeName(),
              mappedValueType.asRawTypeName()),
          rawEntryName,
          rawObjectName);
      String rawKeyName = CodeBlock.of("$L.getKey()", rawEntryName).toString();
      String mappedKeyName;
      if (mappedKeyType instanceof PropertyType.Simple) {
        mappedKeyName = rawKeyName; // no conversion, use the instance as-is
      } else {
        mappedKeyName = enclosingClass.getNameIndex().uniqueField("mappedKey");
        convertUdtsIntoEntities(rawKeyName, mappedKeyName, mappedKeyType, getBuilder);
      }
      String rawValueName = CodeBlock.of("$L.getValue()", rawEntryName).toString();
      String mappedValueName;
      if (mappedValueType instanceof PropertyType.Simple) {
        mappedValueName = rawValueName;
      } else {
        mappedValueName = enclosingClass.getNameIndex().uniqueField("mappedValue");
        convertUdtsIntoEntities(rawValueName, mappedValueName, mappedValueType, getBuilder);
      }
      getBuilder
          .addStatement("$L.put($L, $L)", mappedObjectName, mappedKeyName, mappedValueName)
          .endControlFlow();
    } else {
      throw new AssertionError("Unsupported type " + type.asTypeName());
    }
  }
}
