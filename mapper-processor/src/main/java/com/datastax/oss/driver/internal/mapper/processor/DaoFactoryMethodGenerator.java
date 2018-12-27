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
package com.datastax.oss.driver.internal.mapper.processor;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.internal.mapper.DaoCacheKey;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

/** Generate the methods of the main mapper that produce DAOs. */
public class DaoFactoryMethodGenerator implements PartialClassGenerator {

  /**
   * Tries to create a generator for the given method if it matches an expected signature, otherwise
   * returns null.
   */
  public static DaoFactoryMethodGenerator newInstance(
      ExecutableElement methodElement, GenerationContext context) {
    TypeMirror returnType = methodElement.getReturnType();
    if (returnType.getKind() != TypeKind.DECLARED) {
      return null;
    }
    DeclaredType declaredReturnType = (DeclaredType) returnType;
    if (declaredReturnType.getTypeArguments().isEmpty()) {
      Element returnTypeElement = declaredReturnType.asElement();
      return (returnTypeElement.getKind() != ElementKind.INTERFACE
              || returnTypeElement.getAnnotation(Dao.class) == null)
          ? null
          : new DaoFactoryMethodGenerator(
              methodElement,
              GeneratedNames.daoImplementation(((TypeElement) returnTypeElement)),
              false,
              context);
    } else if (isFuture(declaredReturnType.asElement(), context)
        && declaredReturnType.getTypeArguments().size() == 1) {
      TypeMirror typeArgument = declaredReturnType.getTypeArguments().get(0);
      if (typeArgument.getKind() != TypeKind.DECLARED) {
        return null;
      }
      Element typeArgumentElement = ((DeclaredType) typeArgument).asElement();
      return (typeArgumentElement.getKind() != ElementKind.INTERFACE
              || typeArgumentElement.getAnnotation(Dao.class) == null)
          ? null
          : new DaoFactoryMethodGenerator(
              methodElement,
              GeneratedNames.daoImplementation(((TypeElement) typeArgumentElement)),
              true,
              context);
    } else {
      return null;
    }
  }

  private static boolean isFuture(Element element, GenerationContext context) {
    return context.isSame(element, CompletionStage.class)
        || context.isSame(element, CompletableFuture.class);
  }

  private final ExecutableElement methodElement;
  private final CharSequence keyspaceArgumentName;
  private final CharSequence tableArgumentName;
  private final ClassName daoImplementationName;
  private final String fieldName;
  private final boolean isAsync;
  private final boolean isCachedByKeyspaceAndTable;

  private DaoFactoryMethodGenerator(
      ExecutableElement methodElement,
      ClassName daoImplementationName,
      boolean isAsync,
      GenerationContext context) {
    this.methodElement = methodElement;
    this.daoImplementationName = daoImplementationName;
    this.isAsync = isAsync;
    // TODO disambiguate if multiple methods share the same name?
    fieldName = GeneratedNames.mapperDaoCacheField(methodElement);

    VariableElement tmpKeyspace = null;
    VariableElement tmpTable = null;
    for (VariableElement parameterElement : methodElement.getParameters()) {
      if (parameterElement.getAnnotation(DaoKeyspace.class) != null) {
        tmpKeyspace = validateParameter(parameterElement, tmpKeyspace, DaoKeyspace.class, context);
      } else if (parameterElement.getAnnotation(DaoTable.class) != null) {
        tmpTable = validateParameter(parameterElement, tmpTable, DaoTable.class, context);
      } else {
        context
            .getMessager()
            .error(
                methodElement,
                "Only parameters annotated with @%s or @%s are allowed",
                DaoKeyspace.class.getSimpleName(),
                DaoTable.class.getSimpleName());
        throw new SkipGenerationException();
      }
    }
    isCachedByKeyspaceAndTable = (tmpKeyspace != null || tmpTable != null);
    keyspaceArgumentName =
        (tmpKeyspace == null) ? "(CqlIdentifier)null" : tmpKeyspace.getSimpleName();
    tableArgumentName = (tmpTable == null) ? "(CqlIdentifier)null" : tmpTable.getSimpleName();
  }

  private VariableElement validateParameter(
      VariableElement candidate,
      VariableElement previous,
      Class<?> annotation,
      GenerationContext context) {
    if (previous != null) {
      context
          .getMessager()
          .error(
              candidate,
              "Only one parameter can be annotated with @%s",
              annotation.getSimpleName());
      throw new SkipGenerationException();
    }
    TypeMirror type = candidate.asType();
    if (!context.isSame(type, String.class) && !context.isSame(type, CqlIdentifier.class)) {
      context
          .getMessager()
          .error(
              candidate,
              "@%s-annotated parameter must be of type %s or %s",
              annotation.getSimpleName(),
              String.class.getSimpleName(),
              CqlIdentifier.class.getSimpleName());
      throw new SkipGenerationException();
    }
    return candidate;
  }

  @Override
  public void addMembers(TypeSpec.Builder classBuilder) {
    TypeName returnTypeName = ClassName.get(methodElement.getReturnType());

    if (isCachedByKeyspaceAndTable) {
      classBuilder.addField(
          FieldSpec.builder(
                  ParameterizedTypeName.get(
                      ClassName.get(ConcurrentMap.class),
                      TypeName.get(DaoCacheKey.class),
                      returnTypeName),
                  fieldName,
                  Modifier.PRIVATE,
                  Modifier.FINAL)
              .initializer("new $T<>()", ConcurrentHashMap.class)
              .build());
    } else {
      classBuilder.addField(
          FieldSpec.builder(returnTypeName, fieldName, Modifier.PRIVATE, Modifier.FINAL).build());
    }

    MethodSpec.Builder overridingMethodBuilder =
        MethodSpec.methodBuilder(methodElement.getSimpleName().toString())
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(returnTypeName);
    for (VariableElement parameterElement : methodElement.getParameters()) {
      overridingMethodBuilder.addParameter(
          ClassName.get(parameterElement.asType()), parameterElement.getSimpleName().toString());
    }
    if (isCachedByKeyspaceAndTable) {
      overridingMethodBuilder
          .addStatement(
              "$1T key = new $1T($2L, $3L)",
              DaoCacheKey.class,
              keyspaceArgumentName,
              tableArgumentName)
          .addStatement(
              "return $L.computeIfAbsent(key, "
                  + "k -> $T.$L(context, k.getKeyspaceId(), k.getTableId()))",
              fieldName,
              daoImplementationName,
              isAsync ? "initAsync" : "init");
    } else {
      overridingMethodBuilder.addStatement("return $L", fieldName);
    }
    classBuilder.addMethod(overridingMethodBuilder.build());
  }

  @Override
  public void addConstructorInstructions(MethodSpec.Builder constructorBuilder) {
    if (!isCachedByKeyspaceAndTable) {
      constructorBuilder.addStatement(
          "this.$L = $T.$L(context, null, null)",
          fieldName,
          daoImplementationName,
          isAsync ? "initAsync" : "init");
    }
  }
}
