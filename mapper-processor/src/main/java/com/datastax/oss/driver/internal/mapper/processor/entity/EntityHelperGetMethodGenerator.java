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
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.lang.model.element.Modifier;

public class EntityHelperGetMethodGenerator implements MethodGenerator {

  private final EntityDefinition entityDefinition;
  private final BindableHandlingSharedCode enclosingClass;

  public EntityHelperGetMethodGenerator(
      EntityDefinition entityDefinition, BindableHandlingSharedCode enclosingClass) {
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
            .addParameter(ParameterSpec.builder(TypeName.BOOLEAN, "lenient").build())
            .returns(entityDefinition.getClassName());

    TypeName returnType = entityDefinition.getClassName();
    String resultName = "returnValue";
    boolean mutable = entityDefinition.isMutable();
    if (mutable) {
      // Create an instance now, we'll call the setters as we go through the properties
      getBuilder.addStatement("$1T $2L = new $1T()", returnType, resultName);
    }

    // We store each read property into a local variable, store the names here (this is only used if
    // the entity is immutable, we'll call the all-arg constructor at the end).
    List<String> propertyValueNames = new ArrayList<>();

    for (PropertyDefinition property : entityDefinition.getAllValues()) {
      PropertyType type = property.getType();
      CodeBlock cqlName = property.getCqlName();
      String setterName = property.getSetterName();
      String propertyValueName = enclosingClass.getNameIndex().uniqueField("propertyValue");
      propertyValueNames.add(propertyValueName);

      if (type instanceof PropertyType.Simple) {
        TypeName typeName = ((PropertyType.Simple) type).typeName;
        String primitiveAccessor = GeneratedCodePatterns.PRIMITIVE_ACCESSORS.get(typeName);
        if (primitiveAccessor != null) {
          // Primitive type: use dedicated getter, since it is optimized to avoid boxing
          //     int propertyValue1 = source.getInt("length");
          if (mutable) {
            getBuilder
                .beginControlFlow("if (!lenient || hasProperty(source, $L))", cqlName)
                .addStatement(
                    "$T $L = source.get$L($L)",
                    typeName,
                    propertyValueName,
                    primitiveAccessor,
                    cqlName)
                .addStatement("$L.$L($L)", resultName, setterName, propertyValueName)
                .endControlFlow();
          } else {
            getBuilder.addStatement(
                "$T $L = !lenient || hasProperty(source, $L) ? source.get$L($L) : $L",
                typeName,
                propertyValueName,
                cqlName,
                primitiveAccessor,
                cqlName,
                typeName.equals(TypeName.BOOLEAN) ? false : 0);
          }
        } else if (typeName instanceof ClassName) {
          // Unparameterized class: use the generic, class-based getter:
          //     UUID propertyValue1 = source.get("id", UUID.class);
          if (mutable) {
            getBuilder
                .beginControlFlow("if (!lenient || hasProperty(source, $L))", cqlName)
                .addStatement(
                    "$T $L = source.get($L, $T.class)",
                    typeName,
                    propertyValueName,
                    cqlName,
                    typeName)
                .addStatement("$L.$L($L)", resultName, setterName, propertyValueName)
                .endControlFlow();
          } else {
            getBuilder.addStatement(
                "$T $L = !lenient || hasProperty(source, $L) ? source.get($L, $T.class) : null",
                typeName,
                propertyValueName,
                cqlName,
                cqlName,
                typeName);
          }
        } else {
          // Parameterized type: create a constant and use the GenericType-based getter:
          //     private static final GenericType<List<String>> GENERIC_TYPE =
          //         new GenericType<List<String>>(){};
          //     List<String> propertyValue1 = source.get("names", GENERIC_TYPE);
          // Note that lists, sets and maps of unparameterized classes also fall under that
          // category. Their getter creates a GenericType under the hood, so there's no performance
          // advantage in calling them instead of the generic get().
          if (mutable) {
            getBuilder
                .beginControlFlow("if (!lenient || hasProperty(source, $L))", cqlName)
                .addStatement(
                    "$T $L = source.get($L, $L)",
                    typeName,
                    propertyValueName,
                    cqlName,
                    enclosingClass.addGenericTypeConstant(typeName))
                .addStatement("$L.$L($L)", resultName, setterName, propertyValueName)
                .endControlFlow();
          } else {
            getBuilder.addStatement(
                "$T $L = !lenient || hasProperty(source, $L) ? source.get($L, $L) : null",
                typeName,
                propertyValueName,
                cqlName,
                cqlName,
                enclosingClass.addGenericTypeConstant(typeName));
          }
        }
      } else if (type instanceof PropertyType.SingleEntity) {
        ClassName entityClass = ((PropertyType.SingleEntity) type).entityName;
        // Other entity class: the CQL column is a mapped UDT:
        //     Dimensions propertyValue1;
        //     UdtValue udtValue1 = source.getUdtValue("dimensions");
        //     propertyValue1 = udtValue1 == null ? null : dimensionsHelper.get(udtValue1);
        String udtValueName = enclosingClass.getNameIndex().uniqueField("udtValue");
        if (mutable) {
          getBuilder.beginControlFlow("if (!lenient || hasProperty(source, $L))", cqlName);
          getBuilder.addStatement("$T $L", entityClass, propertyValueName);
        } else {
          getBuilder.addStatement("$T $L = null", entityClass, propertyValueName);
          getBuilder.beginControlFlow("if (!lenient || hasProperty(source, $L))", cqlName);
        }
        getBuilder.addStatement(
            "$T $L = source.getUdtValue($L)", UdtValue.class, udtValueName, cqlName);

        // Get underlying udt object and set it on return type
        String childHelper = enclosingClass.addEntityHelperField(entityClass);
        getBuilder.addStatement(
            "$L = $L == null ? null : $L.get($L, lenient)",
            propertyValueName,
            udtValueName,
            childHelper,
            udtValueName);

        if (mutable) {
          getBuilder.addStatement("$L.$L($L)", resultName, setterName, propertyValueName);
        }
        getBuilder.endControlFlow();
      } else {
        // Collection of other entity class(es): the CQL column is a collection of mapped UDTs
        // Build a copy of the value, decoding all UdtValue instances into entities on the fly.
        //     CollectionTypeT propertyValue1;
        //     RawCollectionTypeT rawCollection1 = source.get("column", GENERIC_TYPE);
        //     if (rawCollection1 == null) {
        //       propertyValue1 = null;
        //     } else {
        //       traverse rawCollection1 and convert all UdtValue into entity classes, recursing
        //       into nested collections if necessary
        //     }
        if (mutable) {
          getBuilder.beginControlFlow("if (!lenient || hasProperty(source, $L))", cqlName);
          getBuilder.addStatement("$T $L", type.asTypeName(), propertyValueName);
        } else {
          getBuilder.addStatement("$T $L = null", type.asTypeName(), propertyValueName);
          getBuilder.beginControlFlow("if (!lenient || hasProperty(source, $L))", cqlName);
        }

        String rawCollectionName = enclosingClass.getNameIndex().uniqueField("rawCollection");
        TypeName rawCollectionType = type.asRawTypeName();
        getBuilder.addStatement(
            "$T $L = source.get($L, $L)",
            rawCollectionType,
            rawCollectionName,
            cqlName,
            enclosingClass.addGenericTypeConstant(rawCollectionType));

        getBuilder
            .beginControlFlow("if ($L == null)", rawCollectionName)
            .addStatement("$L = null", propertyValueName)
            .nextControlFlow("else");
        convertUdtsIntoEntities(rawCollectionName, propertyValueName, type, getBuilder);
        getBuilder.endControlFlow();

        if (mutable) {
          getBuilder.addStatement("$L.$L($L)", resultName, setterName, propertyValueName);
        }
        getBuilder.endControlFlow();
      }
    }

    if (mutable) {
      // We've already created an instance and filled the properties as we went
      getBuilder.addStatement("return returnValue");
    } else {
      // Assume an all-arg constructor exists, and call it with all the temporary variables
      getBuilder.addCode("$[return new $T(", returnType);
      for (int i = 0; i < propertyValueNames.size(); i++) {
        getBuilder.addCode((i == 0 ? "\n$L" : ",\n$L"), propertyValueNames.get(i));
      }
      getBuilder.addCode(")$];");
    }
    return Optional.of(getBuilder.build());
  }

  /**
   * Generates the code to convert a collection of UDT instances, for example a {@code Map<String,
   * UdtValue>} into a {@code Map<String, Product>}.
   *
   * @param rawObjectName the name of the local variable containing the value to convert.
   * @param mappedObjectName the name of the local variable that will hold the converted value (it
   *     already exists).
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
          "$L = $L.get($L, lenient)", mappedObjectName, entityHelperName, rawObjectName);
    } else if (type instanceof PropertyType.EntityList) {
      getBuilder.addStatement(
          "$L = $T.newArrayListWithExpectedSize($L.size())",
          mappedObjectName,
          Lists.class,
          rawObjectName);
      PropertyType mappedElementType = ((PropertyType.EntityList) type).elementType;
      TypeName rawElementType = mappedElementType.asRawTypeName();
      String rawElementName = enclosingClass.getNameIndex().uniqueField("rawElement");
      getBuilder.beginControlFlow("for ($T $L: $L)", rawElementType, rawElementName, rawObjectName);
      String mappedElementName = enclosingClass.getNameIndex().uniqueField("mappedElement");
      getBuilder.addStatement("$T $L", mappedElementType.asTypeName(), mappedElementName);
      convertUdtsIntoEntities(rawElementName, mappedElementName, mappedElementType, getBuilder);
      getBuilder.addStatement("$L.add($L)", mappedObjectName, mappedElementName).endControlFlow();
    } else if (type instanceof PropertyType.EntitySet) {
      getBuilder.addStatement(
          "$L = $T.newLinkedHashSetWithExpectedSize($L.size())",
          mappedObjectName,
          Sets.class,
          rawObjectName);
      PropertyType mappedElementType = ((PropertyType.EntitySet) type).elementType;
      TypeName rawElementType = mappedElementType.asRawTypeName();
      String rawElementName = enclosingClass.getNameIndex().uniqueField("rawElement");
      getBuilder.beginControlFlow("for ($T $L: $L)", rawElementType, rawElementName, rawObjectName);
      String mappedElementName = enclosingClass.getNameIndex().uniqueField("mappedElement");
      getBuilder.addStatement("$T $L", mappedElementType.asTypeName(), mappedElementName);
      convertUdtsIntoEntities(rawElementName, mappedElementName, mappedElementType, getBuilder);
      getBuilder.addStatement("$L.add($L)", mappedObjectName, mappedElementName).endControlFlow();
    } else if (type instanceof PropertyType.EntityMap) {
      getBuilder.addStatement(
          "$L = $T.newLinkedHashMapWithExpectedSize($L.size())",
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
        getBuilder.addStatement("$T $L", mappedKeyType.asTypeName(), mappedKeyName);
        convertUdtsIntoEntities(rawKeyName, mappedKeyName, mappedKeyType, getBuilder);
      }
      String rawValueName = CodeBlock.of("$L.getValue()", rawEntryName).toString();
      String mappedValueName;
      if (mappedValueType instanceof PropertyType.Simple) {
        mappedValueName = rawValueName;
      } else {
        mappedValueName = enclosingClass.getNameIndex().uniqueField("mappedValue");
        getBuilder.addStatement("$T $L", mappedValueType.asTypeName(), mappedValueName);
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
