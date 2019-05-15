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

import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.BOOLEAN;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_ASYNC_PAGING_ITERABLE;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_ASYNC_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_BOOLEAN;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_LONG;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_ROW;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.FUTURE_OF_VOID;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.LONG;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.PAGING_ITERABLE;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.ROW;
import static com.datastax.oss.driver.internal.mapper.processor.dao.ReturnTypeKind.VOID;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Query;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.DaoBase;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import java.util.EnumSet;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

public class DaoQueryMethodGenerator extends DaoMethodGenerator {

  private static final EnumSet<ReturnTypeKind> SUPPORTED_RETURN_TYPES =
      EnumSet.of(
          VOID,
          FUTURE_OF_VOID,
          BOOLEAN,
          FUTURE_OF_BOOLEAN,
          LONG,
          FUTURE_OF_LONG,
          ROW,
          FUTURE_OF_ROW,
          ENTITY,
          OPTIONAL_ENTITY,
          FUTURE_OF_ENTITY,
          FUTURE_OF_OPTIONAL_ENTITY,
          RESULT_SET,
          FUTURE_OF_ASYNC_RESULT_SET,
          PAGING_ITERABLE,
          FUTURE_OF_ASYNC_PAGING_ITERABLE);

  private final String queryString;

  public DaoQueryMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, enclosingClass, context);
    this.queryString = methodElement.getAnnotation(Query.class).value();
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
              "Invalid return type: %s methods must return void, boolean, Integer, Row, "
                  + "an entity class, a result set, a mapped iterable, or a "
                  + "CompletionStage/CompletableFuture of any of the above",
              Query.class.getSimpleName());
      return Optional.empty();
    }

    // Generate the method:
    TypeElement entityElement = returnType.entityElement;
    String helperFieldName =
        (entityElement == null)
            ? null
            : enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generatePrepareRequest(methodBuilder, requestName, helperFieldName));

    MethodSpec.Builder queryBuilder = GeneratedCodePatterns.override(methodElement);

    if (returnType.kind.isAsync) {
      queryBuilder.beginControlFlow("try");
    }

    if (queryString.contains(DaoBase.KEYSPACE_ID_PLACEHOLDER)) {
      queryBuilder
          .addComment("The query string contains $L", DaoBase.KEYSPACE_ID_PLACEHOLDER)
          .beginControlFlow("if (context.getKeyspaceId() == null)")
          .addComment(
              "Make sure we have a keyspace (otherwise the substitutions in initAsync failed "
                  + "and the prepared statement is null)")
          .addStatement(
              "throw new $T($S)",
              IllegalStateException.class,
              String.format(
                  "Can't use %s in @%s method if the DAO wasn't built with a keyspace",
                  DaoBase.KEYSPACE_ID_PLACEHOLDER, Query.class.getSimpleName()))
          .endControlFlow();
    }
    if ((queryString.contains(DaoBase.TABLE_ID_PLACEHOLDER)
            || queryString.contains(DaoBase.QUALIFIED_TABLE_ID_PLACEHOLDER))
        && helperFieldName == null) {
      queryBuilder
          .addComment(
              "The query string contains $L or $L",
              DaoBase.TABLE_ID_PLACEHOLDER,
              DaoBase.QUALIFIED_TABLE_ID_PLACEHOLDER)
          .beginControlFlow("if (context.getTableId() == null)")
          .addComment(
              "Make sure we have a table (otherwise the substitutions in initAsync failed "
                  + "and the prepared statement is null)")
          .addStatement(
              "throw new $T($S)",
              IllegalStateException.class,
              String.format(
                  "Can't use %s or %s in @%s method if it doesn't return an entity class "
                      + "and the DAO wasn't built with a table",
                  DaoBase.TABLE_ID_PLACEHOLDER,
                  DaoBase.QUALIFIED_TABLE_ID_PLACEHOLDER,
                  Query.class.getSimpleName()))
          .endControlFlow();
    }

    queryBuilder.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);

    GeneratedCodePatterns.bindParameters(
        methodElement.getParameters(), queryBuilder, enclosingClass, context);

    queryBuilder
        .addCode("\n")
        .addStatement("$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

    returnType.kind.addExecuteStatement(queryBuilder, helperFieldName);

    if (returnType.kind.isAsync) {
      queryBuilder
          .nextControlFlow("catch ($T t)", Throwable.class)
          .addStatement("return $T.failedFuture(t)", CompletableFutures.class)
          .endControlFlow();
    }

    return Optional.of(queryBuilder.build());
  }

  private void generatePrepareRequest(
      MethodSpec.Builder methodBuilder, String requestName, String helperFieldName) {
    methodBuilder.addStatement(
        "$T $L = replaceKeyspaceAndTablePlaceholders($S, context, $L)",
        SimpleStatement.class,
        requestName,
        queryString,
        (helperFieldName == null) ? "null" : helperFieldName + ".defaultTableId");
  }
}
