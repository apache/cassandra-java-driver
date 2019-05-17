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

import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_ASYNC_PAGING_ITERABLE;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.PAGING_ITERABLE;

import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.datastax.oss.driver.internal.mapper.processor.entity.PropertyDefinition;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

public class DaoSelectMethodGenerator extends DaoMethodGenerator {

  private static final EnumSet<ReturnTypeKind> SUPPORTED_RETURN_TYPES =
      EnumSet.of(
          ENTITY,
          OPTIONAL_ENTITY,
          FUTURE_OF_ENTITY,
          FUTURE_OF_OPTIONAL_ENTITY,
          PAGING_ITERABLE,
          FUTURE_OF_ASYNC_PAGING_ITERABLE);

  public DaoSelectMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, enclosingClass, context);
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the return type:
    ReturnType returnType = parseReturnType(methodElement.getReturnType());
    if (!SUPPORTED_RETURN_TYPES.contains(returnType.kind)) {
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
    TypeElement entityElement = returnType.entityElement;
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

    if (returnType.kind.isAsync) {
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

    returnType.kind.addExecuteStatement(selectBuilder, helperFieldName);

    if (returnType.kind.isAsync) {
      selectBuilder
          .nextControlFlow("catch ($T t)", Throwable.class)
          .addStatement("return $T.failedFuture(t)", CompletableFutures.class)
          .endControlFlow();
    }
    return Optional.of(selectBuilder.build());
  }

  private void generateSelectRequest(
      MethodSpec.Builder methodBuilder, String requestName, String helperFieldName) {
    String customWhereClause = methodElement.getAnnotation(Select.class).customWhereClause();
    if (customWhereClause.isEmpty()) {
      methodBuilder.addStatement(
          "$T $L = $L.selectByPrimaryKey().build()",
          SimpleStatement.class,
          requestName,
          helperFieldName);
    } else {
      methodBuilder.addStatement(
          "$T $L = $L.selectStart().whereRaw($S).build()",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          customWhereClause);
    }
  }
}
