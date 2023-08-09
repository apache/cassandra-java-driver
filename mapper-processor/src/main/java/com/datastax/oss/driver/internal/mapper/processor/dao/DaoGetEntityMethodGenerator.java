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
package com.datastax.oss.driver.internal.mapper.processor.dao;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.GetEntity;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoGetEntityMethodGenerator extends DaoMethodGenerator {

  /** The type of processing to do on the argument in addition to invoking the entity helper. */
  private enum Transformation {
    /** Single source element to single entity. */
    NONE,
    /** First row of iterable to single entity. */
    ONE,
    /** Iterable of rows to iterable of entity. */
    MAP,
    /** Iterable of rows to stream of entity. */
    STREAM,
  }

  private final boolean lenient;

  public DaoGetEntityMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
    lenient = methodElement.getAnnotation(GetEntity.class).lenient();
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the parameter: there must be exactly one, of type GettableByName or a ResultSet:
    if (methodElement.getParameters().size() != 1) {
      context
          .getMessager()
          .error(
              methodElement,
              "Wrong number of parameters: %s methods must have exactly one",
              GetEntity.class.getSimpleName());
      return Optional.empty();
    }
    VariableElement parameterElement = methodElement.getParameters().get(0);
    String parameterName = parameterElement.getSimpleName().toString();
    TypeMirror parameterType = parameterElement.asType();
    boolean parameterIsGettable = context.getClassUtils().implementsGettableByName(parameterType);
    boolean parameterIsResultSet = context.getClassUtils().isSame(parameterType, ResultSet.class);
    boolean parameterIsAsyncResultSet =
        context.getClassUtils().isSame(parameterType, AsyncResultSet.class);
    if (!parameterIsGettable && !parameterIsResultSet && !parameterIsAsyncResultSet) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid parameter type: %s methods must take a %s, %s or %s",
              GetEntity.class.getSimpleName(),
              GettableByName.class.getSimpleName(),
              ResultSet.class.getSimpleName(),
              AsyncResultSet.class.getSimpleName());
      return Optional.empty();
    }

    // Validate the return type. Make sure it matches the parameter type
    Transformation transformation = null;
    TypeMirror returnType = methodElement.getReturnType();
    TypeElement entityElement = EntityUtils.asEntityElement(returnType, typeParameters);
    if (entityElement != null) {
      transformation = parameterIsGettable ? Transformation.NONE : Transformation.ONE;
    } else if (returnType.getKind() == TypeKind.DECLARED) {
      Element element = ((DeclaredType) returnType).asElement();
      if (context.getClassUtils().isSame(element, PagingIterable.class)) {
        if (!parameterIsResultSet) {
          context
              .getMessager()
              .error(
                  methodElement,
                  "Invalid return type: %s methods returning %s must have an argument of type %s",
                  GetEntity.class.getSimpleName(),
                  PagingIterable.class.getSimpleName(),
                  ResultSet.class.getSimpleName());
          return Optional.empty();
        }
        entityElement = EntityUtils.typeArgumentAsEntityElement(returnType, typeParameters);
        transformation = Transformation.MAP;
      } else if (context.getClassUtils().isSame(element, Stream.class)) {
        if (!parameterIsResultSet) {
          context
              .getMessager()
              .error(
                  methodElement,
                  "Invalid return type: %s methods returning %s must have an argument of type %s",
                  GetEntity.class.getSimpleName(),
                  Stream.class.getSimpleName(),
                  ResultSet.class.getSimpleName());
          return Optional.empty();
        }
        entityElement = EntityUtils.typeArgumentAsEntityElement(returnType, typeParameters);
        transformation = Transformation.STREAM;
      } else if (context.getClassUtils().isSame(element, MappedAsyncPagingIterable.class)) {
        if (!parameterIsAsyncResultSet) {
          context
              .getMessager()
              .error(
                  methodElement,
                  "Invalid return type: %s methods returning %s must have an argument of type %s",
                  GetEntity.class.getSimpleName(),
                  MappedAsyncPagingIterable.class.getSimpleName(),
                  AsyncResultSet.class.getSimpleName());
          return Optional.empty();
        }
        entityElement = EntityUtils.typeArgumentAsEntityElement(returnType, typeParameters);
        transformation = Transformation.MAP;
      }
    }
    if (entityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type: "
                  + "%s methods must return a %s-annotated class, or a %s, a %s or %s thereof",
              GetEntity.class.getSimpleName(),
              Entity.class.getSimpleName(),
              PagingIterable.class.getSimpleName(),
              Stream.class.getSimpleName(),
              MappedAsyncPagingIterable.class.getSimpleName());
      return Optional.empty();
    }

    // Generate the implementation:
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));

    MethodSpec.Builder overridingMethodBuilder =
        GeneratedCodePatterns.override(methodElement, typeParameters);
    switch (transformation) {
      case NONE:
        overridingMethodBuilder.addStatement(
            "return $L.get($L, $L)", helperFieldName, parameterName, lenient);
        break;
      case ONE:
        overridingMethodBuilder
            .addStatement("$T row = $L.one()", Row.class, parameterName)
            .addStatement(
                "return (row == null) ? null : $L.get(row, $L)", helperFieldName, lenient);
        break;
      case MAP:
        overridingMethodBuilder.addStatement(
            "return $L.map(row -> $L.get(row, $L))", parameterName, helperFieldName, lenient);
        break;
      case STREAM:
        overridingMethodBuilder.addStatement(
            "return $T.stream($L.map(row -> $L.get(row, $L)).spliterator(), false)",
            StreamSupport.class,
            parameterName,
            helperFieldName,
            lenient);
        break;
    }
    return Optional.of(overridingMethodBuilder.build());
  }
}
