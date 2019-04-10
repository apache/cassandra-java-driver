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
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SkipGenerationException;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoGetEntityMethodGenerator implements MethodGenerator {

  /** The type of processing to do on the argument in addition to invoking the entity helper. */
  private enum Transformation {
    /** Single source element to single entity. */
    NONE,
    /** First row of iterable to single entity. */
    ONE,
    /** Iterable of rows to iterable of entity. */
    MAP,
  }

  private final ExecutableElement methodElement;
  private final DaoImplementationGenerator daoImplementationGenerator;
  private final String parameterName;
  /**
   * The entity type manipulated by this method (it is either the direct return type, or the type
   * parameter of the returned iterable).
   */
  private final TypeElement entityElement;

  private final Transformation transformation;

  public DaoGetEntityMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationGenerator daoImplementationGenerator,
      ProcessorContext context) {
    this.methodElement = methodElement;
    this.daoImplementationGenerator = daoImplementationGenerator;
    // Methods should only have one parameter
    if (methodElement.getParameters().size() != 1) {
      context
          .getMessager()
          .error(
              methodElement, "%s method must have one parameter", GetEntity.class.getSimpleName());
      throw new SkipGenerationException();
    }
    VariableElement parameterElement = methodElement.getParameters().get(0);
    parameterName = parameterElement.getSimpleName().toString();
    // Check the parameter type, make sure it matches an expected type
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
              "Invalid parameter type, expected a %s, %s or %s",
              GettableByName.class.getSimpleName(),
              ResultSet.class.getSimpleName(),
              AsyncResultSet.class.getSimpleName());
      throw new SkipGenerationException();
    }
    TypeElement tmpEntityElement = null;
    Transformation tmpTransformation = null;
    // Check return type. Make sure it matches the parameter type
    TypeMirror returnType = methodElement.getReturnType();
    if (returnType.getKind() == TypeKind.DECLARED) {
      Element element = ((DeclaredType) returnType).asElement();
      // Simple case return type is an entity type
      if (element.getKind() == ElementKind.CLASS && element.getAnnotation(Entity.class) != null) {
        tmpEntityElement = (TypeElement) element;
        tmpTransformation = parameterIsGettable ? Transformation.NONE : Transformation.ONE;
      } else if (context.getClassUtils().isSame(element, PagingIterable.class)) {
        if (!parameterIsResultSet) {
          context
              .getMessager()
              .error(
                  methodElement,
                  "%s method can only return %s if its argument is %s",
                  GetEntity.class.getSimpleName(),
                  PagingIterable.class.getSimpleName(),
                  ResultSet.class.getSimpleName());
          throw new SkipGenerationException();
        }
        tmpEntityElement = extractTypeParameter(returnType);
        tmpTransformation = Transformation.MAP;
      } else if (context.getClassUtils().isSame(element, MappedAsyncPagingIterable.class)) {
        if (!parameterIsAsyncResultSet) {
          context
              .getMessager()
              .error(
                  methodElement,
                  "%s method can only return %s if its argument is %s",
                  GetEntity.class.getSimpleName(),
                  MappedAsyncPagingIterable.class.getSimpleName(),
                  AsyncResultSet.class.getSimpleName());
          throw new SkipGenerationException();
        }
        tmpEntityElement = extractTypeParameter(returnType);
        tmpTransformation = Transformation.MAP;
      }
    }
    if (tmpEntityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type. Expected %s-annotated class, or a %s or %s thereof",
              Entity.class.getSimpleName(),
              PagingIterable.class.getSimpleName(),
              MappedAsyncPagingIterable.class.getSimpleName());
      throw new SkipGenerationException();
    }
    this.entityElement = tmpEntityElement;
    this.transformation = tmpTransformation;
  }

  /** @return the type argument if it's an annotated entity, otherwise null */
  private TypeElement extractTypeParameter(TypeMirror returnType) {
    // Only called when we've already checked the main type is PagingIterable or
    // AsyncPagingIterable, so we know it's a declared type and the number of type type arguments is
    // exactly 1.
    assert returnType.getKind() == TypeKind.DECLARED;
    TypeMirror typeArgumentMirror = ((DeclaredType) returnType).getTypeArguments().get(0);
    if (typeArgumentMirror.getKind() == TypeKind.DECLARED) {
      Element typeArgumentElement = ((DeclaredType) typeArgumentMirror).asElement();
      if (typeArgumentElement.getAnnotation(Entity.class) != null) {
        return (TypeElement) typeArgumentElement;
      }
    }
    return null;
  }

  @Override
  public MethodSpec.Builder generate() {
    String helperFieldName =
        daoImplementationGenerator.addEntityHelperField(ClassName.get(entityElement));

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
    return overridingMethodBuilder;
  }
}
