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
package com.datastax.oss.driver.internal.mapper.processor.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.internal.mapper.DaoCacheKey;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.PartialClassGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SkipGenerationException;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.ParameterizedTypeName;
import com.squareup.javapoet.TypeName;
import com.squareup.javapoet.TypeSpec;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;

public class DaoFactoryMethodGenerator implements PartialClassGenerator {

  private final ExecutableElement methodElement;
  private final CharSequence keyspaceArgumentName;
  private final CharSequence tableArgumentName;
  private final ClassName daoImplementationName;
  private final String fieldName;
  private final boolean isAsync;
  private final boolean isCachedByKeyspaceAndTable;

  public DaoFactoryMethodGenerator(
      ExecutableElement methodElement,
      ClassName daoImplementationName,
      boolean isAsync,
      ProcessorContext context) {
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
      ProcessorContext context) {
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
    if (!context.getClassUtils().isSame(type, String.class)
        && !context.getClassUtils().isSame(type, CqlIdentifier.class)) {
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
