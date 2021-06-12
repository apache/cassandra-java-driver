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
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.Capitalizer;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;

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
          .put(TypeName.SHORT, "Short")
          .build();

  private static final String NULL_SAVING_STRATEGY = "nullSavingStrategy";

  /** Starts the generation of a method that overrides an interface method. */
  public static MethodSpec.Builder override(ExecutableElement interfaceMethod) {
    return override(interfaceMethod, Collections.emptyMap());
  }

  public static MethodSpec.Builder override(
      ExecutableElement interfaceMethod, Map<Name, TypeElement> typeParameters) {
    MethodSpec.Builder result =
        MethodSpec.methodBuilder(interfaceMethod.getSimpleName().toString())
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(getTypeName(interfaceMethod.getReturnType(), typeParameters));
    for (VariableElement parameterElement : interfaceMethod.getParameters()) {
      TypeName type = getTypeName(parameterElement.asType(), typeParameters);
      result.addParameter(type, parameterElement.getSimpleName().toString());
    }
    for (TypeMirror thrownType : interfaceMethod.getThrownTypes()) {
      result.addException(TypeName.get(thrownType));
    }
    return result;
  }

  public static TypeName getTypeName(TypeMirror mirror, Map<Name, TypeElement> typeParameters) {
    if (mirror.getKind() == TypeKind.TYPEVAR) {
      TypeVariable typeVariable = (TypeVariable) mirror;
      Name name = typeVariable.asElement().getSimpleName();
      TypeElement element = typeParameters.get(name);
      return ClassName.get(element);
    } else if (mirror.getKind() == TypeKind.DECLARED) {
      DeclaredType declaredType = (DeclaredType) mirror;
      TypeElement element = (TypeElement) declaredType.asElement();
      if (declaredType.getTypeArguments().size() == 0) {
        return ClassName.get(element);
      } else {
        // resolve types for each type argument.
        TypeName[] types = new TypeName[declaredType.getTypeArguments().size()];
        for (int i = 0; i < declaredType.getTypeArguments().size(); i++) {
          TypeMirror typeArgument = declaredType.getTypeArguments().get(i);
          types[i] = getTypeName(typeArgument, typeParameters);
        }
        return ParameterizedTypeName.get(ClassName.get(element), types);
      }
    } else {
      return ClassName.get(mirror);
    }
  }

  /** Adds a private final field to a class, that gets initialized through its constructor. */
  public static void addFinalFieldAndConstructorArgument(
      TypeName fieldType,
      String fieldName,
      TypeSpec.Builder classBuilder,
      MethodSpec.Builder constructorBuilder) {

    classBuilder.addField(
        FieldSpec.builder(fieldType, fieldName, Modifier.PRIVATE, Modifier.FINAL).build());
    constructorBuilder.addParameter(fieldType, fieldName).addStatement("this.$1L = $1L", fieldName);
  }

  /**
   * Treats a list of method parameters as bind variables in a query, assuming that the bind markers
   * have the same names as the parameters, unless they are annotated with {@link CqlName}.
   *
   * <p>The generated code assumes that a {@code BoundStatementBuilder boundStatementBuilder} local
   * variable already exists.
   */
  public static void bindParameters(
      @NonNull List<? extends VariableElement> parameters,
      CodeBlock.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass,
      ProcessorContext context,
      boolean useNullSavingStrategy) {
    List<CodeBlock> bindMarkerNames = new ArrayList<>();
    for (VariableElement parameter : parameters) {
      CqlName cqlName = parameter.getAnnotation(CqlName.class);
      String parameterName;
      if (cqlName == null) {
        parameterName = parameter.getSimpleName().toString();
      } else {
        parameterName = cqlName.value();
      }
      bindMarkerNames.add(CodeBlock.of("$S", parameterName));
    }
    bindParameters(
        parameters, bindMarkerNames, methodBuilder, enclosingClass, context, useNullSavingStrategy);
  }

  /**
   * Treats a list of method parameters as bind variables in a query, using the provided bind
   * markers.
   *
   * <p>The generated code assumes that a {@code BoundStatementBuilder boundStatementBuilder} local
   * variable already exists.
   */
  public static void bindParameters(
      @NonNull List<? extends VariableElement> parameters,
      @NonNull List<CodeBlock> bindMarkerNames,
      CodeBlock.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass,
      ProcessorContext context,
      boolean useNullSavingStrategy) {

    assert bindMarkerNames.size() == parameters.size();
    for (int i = 0; i < parameters.size(); i++) {
      VariableElement parameter = parameters.get(i);
      String parameterName = parameter.getSimpleName().toString();
      PropertyType type = PropertyType.parse(parameter.asType(), context);
      setValue(
          bindMarkerNames.get(i),
          type,
          CodeBlock.of("$L", parameterName),
          "boundStatementBuilder",
          methodBuilder,
          enclosingClass,
          useNullSavingStrategy,
          false);
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
      CodeBlock cqlName,
      PropertyType type,
      CodeBlock valueExtractor,
      String targetName,
      CodeBlock.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass) {
    setValue(
        cqlName, type, valueExtractor, targetName, methodBuilder, enclosingClass, false, false);
  }

  public static void setValue(
      CodeBlock cqlName,
      PropertyType type,
      CodeBlock valueExtractor,
      String targetName,
      CodeBlock.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass,
      boolean useNullSavingStrategy,
      boolean useLeniency) {

    if (type instanceof PropertyType.Simple) {
      TypeName typeName = ((PropertyType.Simple) type).typeName;
      String primitiveAccessor = GeneratedCodePatterns.PRIMITIVE_ACCESSORS.get(typeName);
      if (primitiveAccessor != null) {
        // Primitive type: use dedicated setter, since it is optimized to avoid boxing.
        //     target = target.setInt("length", entity.getLength());
        methodBuilder.addStatement(
            "$1L = $1L.set$2L($3L, $4L)",
            targetName,
            primitiveAccessor,
            cqlName,
            valueExtractor); // null saving strategy for primitiveSet does not apply
      } else if (typeName instanceof ClassName) {
        // Unparameterized class: use the generic, class-based setter.
        //     target = target.set("id", entity.getId(), UUID.class);
        generateSetWithClass(
            cqlName, valueExtractor, targetName, methodBuilder, typeName, useNullSavingStrategy);
      } else {
        // Parameterized type: create a constant and use the GenericType-based setter.
        //     private static final GenericType<List<String>> GENERIC_TYPE =
        //         new GenericType<List<String>>(){};
        //     target = target.set("names", entity.getNames(), GENERIC_TYPE);
        // Note that lists, sets and maps of unparameterized classes also fall under that
        // category. Their setter creates a GenericType under the hood, so there's no performance
        // advantage in calling them instead of the generic set().
        generateParameterizedSet(
            cqlName,
            valueExtractor,
            targetName,
            methodBuilder,
            typeName,
            enclosingClass,
            useNullSavingStrategy);
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
              "$1T $2L = ($1T) $3L.getType($4L)",
              UserDefinedType.class,
              udtTypeName,
              targetName,
              cqlName)
          .addStatement("$T $L = $L.newValue()", UdtValue.class, udtValueName, udtTypeName);
      String childHelper = enclosingClass.addEntityHelperField(entityClass);
      methodBuilder
          // driver doesn't have the ability to send partial UDT, unset values values will be
          // serialized to null - set NullSavingStrategy.DO_NOT_SET explicitly
          .addStatement(
              "$L.set($L, $L,  $T.$L, $L)",
              childHelper,
              valueName,
              udtValueName,
              NullSavingStrategy.class,
              NullSavingStrategy.DO_NOT_SET,
              useLeniency ? "lenient" : false)
          .addStatement("$1L = $1L.setUdtValue($2L, $3L)", targetName, cqlName, udtValueName);
      if (useNullSavingStrategy) {
        methodBuilder.nextControlFlow(
            "else if ($L == $T.$L)",
            NULL_SAVING_STRATEGY,
            NullSavingStrategy.class,
            NullSavingStrategy.SET_TO_NULL);
      } else {
        methodBuilder.nextControlFlow("else");
      }
      methodBuilder
          .addStatement("$1L = $1L.setUdtValue($2L, null)", targetName, cqlName)
          .endControlFlow();
    } else {
      // Collection of other entity class(es): the CQL column is a collection of mapped UDTs
      // Build a copy of the value, encoding all entities into UdtValue instances on the fly.
      String mappedCollectionName = enclosingClass.getNameIndex().uniqueField("mappedCollection");
      String rawCollectionName = enclosingClass.getNameIndex().uniqueField("rawCollection");
      methodBuilder
          .addStatement("$T $L = $L", type.asTypeName(), mappedCollectionName, valueExtractor)
          .beginControlFlow("if ($L != null)", mappedCollectionName);

      CodeBlock currentCqlType = CodeBlock.of("$L.getType($L)", targetName, cqlName);
      CodeBlock.Builder udtTypesBuilder = CodeBlock.builder();
      CodeBlock.Builder conversionCodeBuilder = CodeBlock.builder();
      convertEntitiesIntoUdts(
          mappedCollectionName,
          rawCollectionName,
          type,
          currentCqlType,
          udtTypesBuilder,
          conversionCodeBuilder,
          enclosingClass,
          useLeniency);

      methodBuilder
          .add(udtTypesBuilder.build())
          .add(conversionCodeBuilder.build())
          .addStatement(
              "$1L = $1L.set($2L, $3L, $4L)",
              targetName,
              cqlName,
              rawCollectionName,
              enclosingClass.addGenericTypeConstant(type.asRawTypeName()));
      if (useNullSavingStrategy) {
        methodBuilder.nextControlFlow(
            "else if ($L == $T.$L)",
            NULL_SAVING_STRATEGY,
            NullSavingStrategy.class,
            NullSavingStrategy.SET_TO_NULL);
      } else {
        methodBuilder.nextControlFlow("else");
      }
      methodBuilder
          .addStatement(
              "$1L = $1L.set($2L, null, $3L)",
              targetName,
              cqlName,
              enclosingClass.addGenericTypeConstant(type.asRawTypeName()))
          .endControlFlow();
    }
  }

  private static void generateParameterizedSet(
      CodeBlock cqlName,
      CodeBlock valueExtractor,
      String targetName,
      CodeBlock.Builder methodBuilder,
      TypeName typeName,
      BindableHandlingSharedCode enclosingClass,
      boolean useNullSavingStrategy) {
    generateSetWithNullSavingStrategy(
        valueExtractor,
        methodBuilder,
        CodeBlock.of(
            "$1L = $1L.set($2L, $3L, $4L)",
            targetName,
            cqlName,
            valueExtractor,
            enclosingClass.addGenericTypeConstant(typeName)),
        useNullSavingStrategy);
  }

  private static void generateSetWithClass(
      CodeBlock cqlName,
      CodeBlock valueExtractor,
      String targetName,
      CodeBlock.Builder methodBuilder,
      TypeName typeName,
      boolean useNullSavingStrategy) {

    generateSetWithNullSavingStrategy(
        valueExtractor,
        methodBuilder,
        CodeBlock.of(
            "$1L = $1L.set($2L, $3L, $4T.class)", targetName, cqlName, valueExtractor, typeName),
        useNullSavingStrategy);
  }

  /**
   * If this method is invoked with useNullSavingStrategy = true it assumes that NullSavingStrategy
   * nullSavingStrategy = ...; variable is already defined on the MethodBuilder.
   */
  private static void generateSetWithNullSavingStrategy(
      CodeBlock valueExtractor,
      CodeBlock.Builder methodBuilder,
      CodeBlock nonNullStatement,
      boolean useNullSavingStrategy) {
    if (useNullSavingStrategy) {
      methodBuilder.beginControlFlow(
          "if ($1L != null || $2L == $3T.$4L)",
          valueExtractor,
          NULL_SAVING_STRATEGY,
          NullSavingStrategy.class,
          NullSavingStrategy.SET_TO_NULL);
      methodBuilder.addStatement(nonNullStatement);
      methodBuilder.endControlFlow();
    } else {
      methodBuilder.addStatement(nonNullStatement);
    }
  }

  /**
   * Shortcut for {@link #setValue(CodeBlock, PropertyType, CodeBlock, String, CodeBlock.Builder,
   * BindableHandlingSharedCode)} when the cqlName is a string known at compile time.
   */
  public static void setValue(
      String cqlName,
      PropertyType type,
      CodeBlock valueExtractor,
      String targetName,
      CodeBlock.Builder methodBuilder,
      BindableHandlingSharedCode enclosingClass,
      boolean useNullSavingStrategy) {
    setValue(
        CodeBlock.of("$S", cqlName),
        type,
        valueExtractor,
        targetName,
        methodBuilder,
        enclosingClass,
        useNullSavingStrategy,
        false);
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
   * @param useLeniency whether the 'lenient' boolean variable is in scope.
   */
  private static void convertEntitiesIntoUdts(
      String mappedObjectName,
      String rawObjectName,
      PropertyType type,
      CodeBlock currentCqlType,
      CodeBlock.Builder udtTypesBuilder,
      CodeBlock.Builder conversionBuilder,
      BindableHandlingSharedCode enclosingClass,
      boolean useLeniency) {

    if (type instanceof PropertyType.SingleEntity) {
      ClassName entityClass = ((PropertyType.SingleEntity) type).entityName;
      String udtTypeName =
          enclosingClass
              .getNameIndex()
              .uniqueField(Capitalizer.decapitalize(entityClass.simpleName()) + "UdtType");
      udtTypesBuilder.addStatement(
          "$1T $2L = ($1T) $3L", UserDefinedType.class, udtTypeName, currentCqlType);

      String entityHelperName = enclosingClass.addEntityHelperField(entityClass);
      conversionBuilder
          .addStatement("$T $L = $L.newValue()", UdtValue.class, rawObjectName, udtTypeName)
          // driver doesn't have the ability to send partial UDT, unset values values will be
          // serialized to null - set NullSavingStrategy.DO_NOT_SET explicitly
          .addStatement(
              "$L.set($L, $L, $T.$L, $L)",
              entityHelperName,
              mappedObjectName,
              rawObjectName,
              NullSavingStrategy.class,
              NullSavingStrategy.DO_NOT_SET,
              useLeniency ? "lenient" : false);
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
          enclosingClass,
          useLeniency);
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
          enclosingClass,
          useLeniency);
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
            enclosingClass,
            useLeniency);
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
            enclosingClass,
            useLeniency);
      }
      conversionBuilder
          .addStatement("$L.put($L, $L)", rawObjectName, rawKeyName, rawValueName)
          .endControlFlow();
    } else {
      throw new AssertionError("Unsupported type " + type.asTypeName());
    }
  }
}
