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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.datastax.oss.driver.internal.mapper.processor.entity.PropertyDefinition;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoSelectMethodGenerator implements MethodGenerator {

  private final ExecutableElement methodElement;
  private final DaoImplementationSharedCode enclosingClass;
  private final ProcessorContext context;

  public DaoSelectMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    this.methodElement = methodElement;
    this.enclosingClass = enclosingClass;
    this.context = context;
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the return type: entity, or future thereof, or PagingIterable, or future of
    // MappedAsyncPagingIterable.
    TypeElement entityElement = null;
    boolean isAsync = false, returnsIterable = false;
    TypeMirror returnTypeMirror = methodElement.getReturnType();
    if (returnTypeMirror.getKind() == TypeKind.DECLARED) {
      Element returnElement = ((DeclaredType) returnTypeMirror).asElement();
      if (returnElement.getKind() == ElementKind.CLASS
          && returnElement.getAnnotation(Entity.class) != null) {
        entityElement = (TypeElement) returnElement;
      } else if (context.getClassUtils().isSame(returnElement, PagingIterable.class)) {
        entityElement = extractTypeParameter(returnTypeMirror);
        returnsIterable = true;
      } else if (context.getClassUtils().isFuture(((DeclaredType) returnTypeMirror))) {
        TypeMirror typeArgumentMirror = ((DeclaredType) returnTypeMirror).getTypeArguments().get(0);
        if (typeArgumentMirror.getKind() == TypeKind.DECLARED
            && context
                .getClassUtils()
                .isSame(
                    ((DeclaredType) typeArgumentMirror).asElement(),
                    MappedAsyncPagingIterable.class)) {
          entityElement = extractTypeParameter(typeArgumentMirror);
          isAsync = true;
          returnsIterable = true;
        } else {
          entityElement = extractTypeParameter(returnTypeMirror);
          isAsync = true;
        }
      }
    }
    if (entityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type: "
                  + "%1$s methods must return an %2$s-annotated class, or a %3$s, %4$s, %5$s "
                  + "or %3$s/%4$s<%6$s> thereof",
              Select.class.getSimpleName(),
              Entity.class.getSimpleName(),
              CompletionStage.class.getSimpleName(),
              CompletableFuture.class.getSimpleName(),
              PagingIterable.class.getSimpleName(),
              MappedAsyncPagingIterable.class.getSimpleName());
      return Optional.empty();
    }
    EntityDefinition entityDefinition = context.getEntityFactory().getDefinition(entityElement);

    // Validate the parameters:
    // - if there is a custom clause, they are free-form  (they'll be used as bind variables)
    // - otherwise, they must be an exact match for the entity's primary key
    List<? extends VariableElement> parameters = methodElement.getParameters();
    Select selectAnnotation = methodElement.getAnnotation(Select.class);
    assert selectAnnotation != null; // otherwise we wouldn't have gotten into this class
    String customClause = selectAnnotation.customWhereClause();
    if (customClause.isEmpty()) {
      List<TypeName> primaryKeyTypes =
          entityDefinition
              .getPrimaryKey()
              .stream()
              .map(d -> d.getType().asTypeName())
              .collect(Collectors.toList());
      List<TypeName> parameterTypes =
          parameters.stream().map(p -> TypeName.get(p.asType())).collect(Collectors.toList());
      // Note that we only check the types: we allow the names to be different.
      if (!primaryKeyTypes.equals(parameterTypes)) {
        context
            .getMessager()
            .error(
                methodElement,
                "Invalid parameter list: %s methods that don't use a custom clause "
                    + "must take the partition key components in the exact order "
                    + "(expected PK of %s: %s)",
                Select.class.getSimpleName(),
                entityElement.getSimpleName(),
                primaryKeyTypes);
        return Optional.empty();
      }
    }

    // Generate the method:
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generateSelectRequest(methodBuilder, requestName, helperFieldName));
    MethodSpec.Builder selectBuilder = GeneratedCodePatterns.override(methodElement);

    if (isAsync) {
      selectBuilder.beginControlFlow("try");
    }
    selectBuilder.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);
    if (parameters.size() > 0) {
      if (customClause.isEmpty()) {
        // Parameters are the PK components, we allow them to be named differently
        List<String> primaryKeyNames =
            entityDefinition
                .getPrimaryKey()
                .stream()
                .map(PropertyDefinition::getCqlName)
                .collect(Collectors.toList());
        GeneratedCodePatterns.bindParameters(
            parameters, primaryKeyNames, selectBuilder, enclosingClass, context);
      } else {
        // We don't know the bind marker names in the custom clause, so use the same names as the
        // parameters, user is responsible to make them match.
        GeneratedCodePatterns.bindParameters(parameters, selectBuilder, enclosingClass, context);
      }
    }
    selectBuilder
        .addCode("\n")
        .addStatement("$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

    String baseMethodName =
        String.format(
            "%sAndMapTo%s",
            isAsync ? "executeAsync" : "execute",
            returnsIterable ? "EntityIterable" : "SingleEntity");

    selectBuilder.addStatement("return $L(boundStatement, $L)", baseMethodName, helperFieldName);

    if (isAsync) {
      selectBuilder
          .nextControlFlow("catch ($T t)", Throwable.class)
          .addStatement("return $T.failedFuture(t)", CompletableFutures.class)
          .endControlFlow();
    }
    return Optional.of(selectBuilder.build());
  }

  /** @return the type argument if it's an annotated entity, otherwise null */
  private TypeElement extractTypeParameter(TypeMirror returnType) {
    // Only called when we've already checked the main type is a parameterized class that takes
    // exactly 1 argument.
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

  private void generateSelectRequest(
      MethodSpec.Builder methodBuilder, String requestName, String helperFieldName) {
    String customClause = methodElement.getAnnotation(Select.class).customWhereClause();
    if (customClause.isEmpty()) {
      methodBuilder.addStatement(
          "$T $L = $L.selectByPrimaryKey().build()",
          SimpleStatement.class,
          requestName,
          helperFieldName);
    } else {
      methodBuilder.addStatement(
          "$1T $2L = $1T.newInstance($3L.selectStart().asCql() + $4S)",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          " " + customClause);
    }
  }
}
