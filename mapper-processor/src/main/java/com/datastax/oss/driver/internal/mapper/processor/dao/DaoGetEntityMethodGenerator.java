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
import java.util.Optional;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
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
  }

  public DaoGetEntityMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, enclosingClass, context);
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
              parameterElement,
              "Invalid parameter type: %s methods must take a %s, %s or %s",
              GetEntity.class.getSimpleName(),
              GettableByName.class.getSimpleName(),
              ResultSet.class.getSimpleName(),
              AsyncResultSet.class.getSimpleName());
      return Optional.empty();
    }

    // Validate the return type. Make sure it matches the parameter type
    TypeElement entityElement = null;
    Transformation transformation = null;
    TypeMirror returnType = methodElement.getReturnType();
    if (returnType.getKind() == TypeKind.DECLARED) {
      Element element = ((DeclaredType) returnType).asElement();
      // Simple case return type is an entity type
      if (element.getKind() == ElementKind.CLASS && element.getAnnotation(Entity.class) != null) {
        entityElement = (TypeElement) element;
        transformation = parameterIsGettable ? Transformation.NONE : Transformation.ONE;
      } else if (context.getClassUtils().isSame(element, PagingIterable.class)) {
        if (!parameterIsResultSet) {
          context
              .getMessager()
              .error(
                  methodElement,
                  "Invalid return type: %s methods must return %s if the argument is %s",
                  GetEntity.class.getSimpleName(),
                  PagingIterable.class.getSimpleName(),
                  ResultSet.class.getSimpleName());
          return Optional.empty();
        }
        entityElement = typeArgumentAsEntityElement(returnType);
        transformation = Transformation.MAP;
      } else if (context.getClassUtils().isSame(element, MappedAsyncPagingIterable.class)) {
        if (!parameterIsAsyncResultSet) {
          context
              .getMessager()
              .error(
                  methodElement,
                  "Invalid return type: %s methods must return %s if the argument is %s",
                  GetEntity.class.getSimpleName(),
                  MappedAsyncPagingIterable.class.getSimpleName(),
                  AsyncResultSet.class.getSimpleName());
          return Optional.empty();
        }
        entityElement = typeArgumentAsEntityElement(returnType);
        transformation = Transformation.MAP;
      }
    }
    if (entityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type: "
                  + "%s methods must return a %s-annotated class, or a %s or %s thereof",
              GetEntity.class.getSimpleName(),
              Entity.class.getSimpleName(),
              PagingIterable.class.getSimpleName(),
              MappedAsyncPagingIterable.class.getSimpleName());
      return Optional.empty();
    }

    // Generate the implementation:
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));

    MethodSpec.Builder overridingMethodBuilder = GeneratedCodePatterns.override(methodElement);
    switch (transformation) {
      case NONE:
        overridingMethodBuilder.addStatement("return $L.get($L)", helperFieldName, parameterName);
        break;
      case ONE:
        overridingMethodBuilder
            .addStatement("$T row = $L.one()", Row.class, parameterName)
            .addStatement("return (row == null) ? null : $L.get(row)", helperFieldName);
        break;
      case MAP:
        overridingMethodBuilder.addStatement(
            "return $L.map($L::get)", parameterName, helperFieldName);
        break;
    }
    return Optional.of(overridingMethodBuilder.build());
  }
}
