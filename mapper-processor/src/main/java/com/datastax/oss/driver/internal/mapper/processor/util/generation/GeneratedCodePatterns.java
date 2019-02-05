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
package com.datastax.oss.driver.internal.mapper.processor.util.generation;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import java.beans.Introspector;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.VariableElement;

/** A collection of recurring patterns in our generated sources. */
public class GeneratedCodePatterns {

  /**
   * The names of the primitive getters/setters on {@link GettableByName} and {@link
   * SettableByName}.
   */
  public static final Map<TypeName, String> PRIMITIVE_ACCESSORS =
      ImmutableMap.<TypeName, String>builder()
          .put(TypeName.BOOLEAN, "Boolean")
          .put(TypeName.BYTE, "Byte")
          .put(TypeName.DOUBLE, "Double")
          .put(TypeName.FLOAT, "Float")
          .put(TypeName.INT, "Int")
          .put(TypeName.LONG, "Long")
          .build();

  /**
   * Treats a list of method parameters as bind variables in a query.
   *
   * <p>The generated code assumes that a {@code BoundStatementBuilder boundStatementBuilder} local
   * variable already exists.
   */
  public static void bindParameters(
      List<? extends VariableElement> parameters,
      MethodSpec.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass,
      ProcessorContext context) {

    for (VariableElement parameter : parameters) {
      String parameterName = parameter.getSimpleName().toString();
      PropertyType type = PropertyType.parse(parameter.asType(), context);
      setValue(
          parameterName,
          type,
          CodeBlock.of("$L", parameterName),
          "boundStatementBuilder",
          methodBuilder,
          enclosingClass);
    }
  }

  /**
   * Generates the code to set a value on a {@link SettableByName} instance.
   *
   * <p>Example:
   *
   * <pre>{@code
   * target = target.set("id", entity.getId(), UUID.class);
   * }</pre>
   *
   * @param cqlName the CQL name to set ({@code "id"})
   * @param type the type of the value ({@code UUID})
   * @param valueExtractor the code snippet to extract the value ({@code entity.getId()}
   * @param targetName the name of the target {@link SettableByName} instance ({@code target})
   * @param methodBuilder where to add the code
   * @param enclosingClass a reference to the parent generator (in case type constants or entity
   *     helpers are needed)
   */
  public static void setValue(
      String cqlName,
      PropertyType type,
      CodeBlock valueExtractor,
      String targetName,
      MethodSpec.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass) {

    methodBuilder.addComment("$L:", cqlName);

    if (type instanceof PropertyType.Simple) {
      TypeName typeName = ((PropertyType.Simple) type).typeName;
      String primitiveAccessor = GeneratedCodePatterns.PRIMITIVE_ACCESSORS.get(typeName);
      if (primitiveAccessor != null) {
        // Primitive type: use dedicated setter, since it is optimized to avoid boxing.
        //     target = target.setInt("length", entity.getLength());
        methodBuilder.addStatement(
            "$1L = $1L.set$2L($3S, $4L)", targetName, primitiveAccessor, cqlName, valueExtractor);
      } else if (typeName instanceof ClassName) {
        // Unparameterized class: use the generic, class-based setter.
        //     target = target.set("id", entity.getId(), UUID.class);
        methodBuilder.addStatement(
            "$1L = $1L.set($2S, $3L, $4T.class)", targetName, cqlName, valueExtractor, typeName);
      } else {
        // Parameterized type: create a constant and use the GenericType-based setter.
        //     private static final GenericType<List<String>> GENERIC_TYPE =
        //         new GenericType<List<String>>(){};
        //     target = target.set("names", entity.getNames(), GENERIC_TYPE);
        // Note that lists, sets and maps of unparameterized classes also fall under that
        // category. Their setter creates a GenericType under the hood, so there's no performance
        // advantage in calling them instead of the generic set().
        methodBuilder.addStatement(
            "$1L = $1L.set($2S, $3L, $4L)",
            targetName,
            cqlName,
            valueExtractor,
            enclosingClass.addGenericTypeConstant(typeName));
      }
    } else if (type instanceof PropertyType.SingleEntity) {
      ClassName entityClass = ((PropertyType.SingleEntity) type).entityName;
      // Other entity class: the CQL column is a mapped UDT. Example of generated code:
      //     Dimensions value = entity.getDimensions();
      //     if (value != null) {
      //       UserDefinedType udtType = (UserDefinedType) target.getType("dimensions");
      //       UdtValue udtValue = udtType.newValue();
      //       dimensionsHelper.set(value, udtValue);
      //       target = target.setUdtValue("dimensions", udtValue);
      //     }

      // Generate unique names for our temporary variables. Note that they are local so we don't
      // strictly need class-wide uniqueness, but it's simpler to reuse the NameIndex
      String udtTypeName = enclosingClass.getNameIndex().uniqueField("udtType");
      String udtValueName = enclosingClass.getNameIndex().uniqueField("udtValue");
      String valueName = enclosingClass.getNameIndex().uniqueField("value");

      methodBuilder
          .addStatement("$T $L = $L", entityClass, valueName, valueExtractor)
          .beginControlFlow("if ($L != null)", valueName)
          .addStatement(
              "$1T $2L = ($1T) $3L.getType($4S)",
              UserDefinedType.class,
              udtTypeName,
              targetName,
              cqlName)
          .addStatement("$T $L = $L.newValue()", UdtValue.class, udtValueName, udtTypeName);
      String childHelper = enclosingClass.addEntityHelperField(entityClass);
      methodBuilder
          .addStatement("$L.set($L, $L)", childHelper, valueName, udtValueName)
          .addStatement("$1L = $1L.setUdtValue($2S, $3L)", targetName, cqlName, udtValueName)
          .endControlFlow();
    } else {
      // Collection of other entity class(es): the CQL column is a collection of mapped UDTs
      // Build a copy of the value, encoding all entities into UdtValue instances on the fly.
      String mappedCollectionName = enclosingClass.getNameIndex().uniqueField("mappedCollection");
      String rawCollectionName = enclosingClass.getNameIndex().uniqueField("rawCollection");
      methodBuilder
          .addStatement("$T $L = $L", type.asTypeName(), mappedCollectionName, valueExtractor)
          .beginControlFlow("if ($L != null)", mappedCollectionName);

      CodeBlock currentCqlType = CodeBlock.of("$L.getType($S)", targetName, cqlName);
      CodeBlock.Builder udtTypesBuilder = CodeBlock.builder();
      CodeBlock.Builder conversionCodeBuilder = CodeBlock.builder();
      convertEntitiesIntoUdts(
          mappedCollectionName,
          rawCollectionName,
          type,
          currentCqlType,
          udtTypesBuilder,
          conversionCodeBuilder,
          enclosingClass);

      methodBuilder
          .addCode(udtTypesBuilder.build())
          .addCode(conversionCodeBuilder.build())
          .addStatement(
              "$1L = $1L.set($2S, $3L, $4L)",
              targetName,
              cqlName,
              rawCollectionName,
              enclosingClass.addGenericTypeConstant(type.asRawTypeName()))
          .endControlFlow();
    }
  }

  /**
   * Generates the code to convert a collection of mapped entities, for example a {@code Map<String,
   * Product>} into a {@code Map<String, UdtValue>}.
   *
   * @param mappedObjectName the name of the local variable containing the value to convert.
   * @param rawObjectName the name of the local variable that will hold the converted value (it does
   *     not exist yet, this method must generate the declaration).
   * @param type the type of the value.
   * @param currentCqlType a code snippet to extract the CQL type corresponding to {@code type}.
   * @param udtTypesBuilder a code block that comes before the conversion code, and creates local
   *     variables that extract the required {@link UserDefinedType} instances from the target
   *     container.
   * @param conversionBuilder the code block to generate the conversion code into.
   */
  private static void convertEntitiesIntoUdts(
      String mappedObjectName,
      String rawObjectName,
      PropertyType type,
      CodeBlock currentCqlType,
      CodeBlock.Builder udtTypesBuilder,
      CodeBlock.Builder conversionBuilder,
      BindableHandlingSharedCode enclosingClass) {

    if (type instanceof PropertyType.SingleEntity) {
      ClassName entityClass = ((PropertyType.SingleEntity) type).entityName;
      String udtTypeName =
          enclosingClass
              .getNameIndex()
              .uniqueField(Introspector.decapitalize(entityClass.simpleName()) + "UdtType");
      udtTypesBuilder.addStatement(
          "$1T $2L = ($1T) $3L", UserDefinedType.class, udtTypeName, currentCqlType);

      String entityHelperName = enclosingClass.addEntityHelperField(entityClass);
      conversionBuilder
          .addStatement("$T $L = $L.newValue()", UdtValue.class, rawObjectName, udtTypeName)
          .addStatement("$L.set($L, $L)", entityHelperName, mappedObjectName, rawObjectName);
    } else if (type instanceof PropertyType.EntityList) {
      TypeName rawCollectionType = type.asRawTypeName();
      conversionBuilder.addStatement(
          "$T $L = $T.newArrayListWithExpectedSize($L.size())",
          rawCollectionType,
          rawObjectName,
          Lists.class,
          mappedObjectName);
      PropertyType mappedElementType = ((PropertyType.EntityList) type).elementType;
      String mappedElementName = enclosingClass.getNameIndex().uniqueField("mappedElement");
      conversionBuilder.beginControlFlow(
          "for ($T $L: $L)", mappedElementType.asTypeName(), mappedElementName, mappedObjectName);
      String rawElementName = enclosingClass.getNameIndex().uniqueField("rawElement");
      convertEntitiesIntoUdts(
          mappedElementName,
          rawElementName,
          mappedElementType,
          CodeBlock.of("(($T) $L).getElementType()", ListType.class, currentCqlType),
          udtTypesBuilder,
          conversionBuilder,
          enclosingClass);
      conversionBuilder.addStatement("$L.add($L)", rawObjectName, rawElementName).endControlFlow();
    } else if (type instanceof PropertyType.EntitySet) {
      TypeName rawCollectionType = type.asRawTypeName();
      conversionBuilder.addStatement(
          "$T $L = $T.newLinkedHashSetWithExpectedSize($L.size())",
          rawCollectionType,
          rawObjectName,
          Sets.class,
          mappedObjectName);
      PropertyType mappedElementType = ((PropertyType.EntitySet) type).elementType;
      String mappedElementName = enclosingClass.getNameIndex().uniqueField("mappedElement");
      conversionBuilder.beginControlFlow(
          "for ($T $L: $L)", mappedElementType.asTypeName(), mappedElementName, mappedObjectName);
      String rawElementName = enclosingClass.getNameIndex().uniqueField("rawElement");
      convertEntitiesIntoUdts(
          mappedElementName,
          rawElementName,
          mappedElementType,
          CodeBlock.of("(($T) $L).getElementType()", SetType.class, currentCqlType),
          udtTypesBuilder,
          conversionBuilder,
          enclosingClass);
      conversionBuilder.addStatement("$L.add($L)", rawObjectName, rawElementName).endControlFlow();
    } else if (type instanceof PropertyType.EntityMap) {
      TypeName rawCollectionType = type.asRawTypeName();
      conversionBuilder.addStatement(
          "$T $L = $T.newLinkedHashMapWithExpectedSize($L.size())",
          rawCollectionType,
          rawObjectName,
          Maps.class,
          mappedObjectName);
      PropertyType mappedKeyType = ((PropertyType.EntityMap) type).keyType;
      PropertyType mappedValueType = ((PropertyType.EntityMap) type).valueType;
      String mappedEntryName = enclosingClass.getNameIndex().uniqueField("mappedEntry");
      conversionBuilder.beginControlFlow(
          "for ($T $L: $L.entrySet())",
          ParameterizedTypeName.get(
              ClassName.get(Map.Entry.class),
              mappedKeyType.asTypeName(),
              mappedValueType.asTypeName()),
          mappedEntryName,
          mappedObjectName);
      String mappedKeyName = CodeBlock.of("$L.getKey()", mappedEntryName).toString();
      String rawKeyName;
      if (mappedKeyType instanceof PropertyType.Simple) {
        rawKeyName = mappedKeyName; // no conversion, use the instance as-is
      } else {
        rawKeyName = enclosingClass.getNameIndex().uniqueField("rawKey");
        convertEntitiesIntoUdts(
            mappedKeyName,
            rawKeyName,
            mappedKeyType,
            CodeBlock.of("(($T) $L).getKeyType()", MapType.class, currentCqlType),
            udtTypesBuilder,
            conversionBuilder,
            enclosingClass);
      }
      String mappedValueName = CodeBlock.of("$L.getValue()", mappedEntryName).toString();
      String rawValueName;
      if (mappedValueType instanceof PropertyType.Simple) {
        rawValueName = mappedValueName;
      } else {
        rawValueName = enclosingClass.getNameIndex().uniqueField("rawValue");
        convertEntitiesIntoUdts(
            mappedValueName,
            rawValueName,
            mappedValueType,
            CodeBlock.of("(($T) $L).getValueType()", MapType.class, currentCqlType),
            udtTypesBuilder,
            conversionBuilder,
            enclosingClass);
      }
      conversionBuilder
          .addStatement("$L.put($L, $L)", rawObjectName, rawKeyName, rawValueName)
          .endControlFlow();
    } else {
      throw new AssertionError("Unsupported type " + type.asTypeName());
    }
  }
}
