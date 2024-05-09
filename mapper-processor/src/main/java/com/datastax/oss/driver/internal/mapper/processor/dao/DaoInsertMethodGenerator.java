/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.internal.mapper.processor.dao;

import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.BOOLEAN;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.BOUND_STATEMENT;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.CUSTOM;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ASYNC_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_BOOLEAN;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_VOID;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.REACTIVE_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.VOID;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

public class DaoInsertMethodGenerator extends DaoMethodGenerator {

  private final NullSavingStrategyValidation nullSavingStrategyValidation;

  public DaoInsertMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
    nullSavingStrategyValidation = new NullSavingStrategyValidation(context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    return ImmutableSet.of(
        VOID,
        FUTURE_OF_VOID,
        ENTITY,
        FUTURE_OF_ENTITY,
        OPTIONAL_ENTITY,
        FUTURE_OF_OPTIONAL_ENTITY,
        BOOLEAN,
        FUTURE_OF_BOOLEAN,
        RESULT_SET,
        BOUND_STATEMENT,
        FUTURE_OF_ASYNC_RESULT_SET,
        REACTIVE_RESULT_SET,
        CUSTOM);
  }

  @Override
  public boolean requiresReactive() {
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Insert.class.getSimpleName());
    if (returnType == null) {
      return false;
    }
    return returnType.requiresReactive();
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the parameters:
    // - the first one must be the entity.
    // - the others are completely free-form (they'll be used as additional bind variables)
    // A Function<BoundStatementBuilder, BoundStatementBuilder> can be added in last position.
    List<? extends VariableElement> parameters = methodElement.getParameters();
    VariableElement boundStatementFunction = findBoundStatementFunction(methodElement);
    if (boundStatementFunction != null) {
      parameters = parameters.subList(0, parameters.size() - 1);
    }
    TypeElement entityElement =
        parameters.isEmpty()
            ? null
            : EntityUtils.asEntityElement(parameters.get(0), typeParameters);
    if (entityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "%s methods must take the entity to insert as the first parameter",
              Insert.class.getSimpleName());
      return Optional.empty();
    }

    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Insert.class.getSimpleName());
    if (returnType == null) {
      return Optional.empty();
    }
    if (returnType.getEntityElement() != null
        && !returnType.getEntityElement().equals(entityElement)) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type: %s methods must return the same entity as their argument ",
              Insert.class.getSimpleName());
      return Optional.empty();
    }

    // Generate the method:
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generatePrepareRequest(methodBuilder, requestName, helperFieldName));

    CodeBlock.Builder createStatementBlock = CodeBlock.builder();

    createStatementBlock.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);

    populateBuilderWithStatementAttributes(createStatementBlock, methodElement);
    populateBuilderWithFunction(createStatementBlock, boundStatementFunction);

    warnIfCqlNamePresent(parameters.subList(0, 1));
    String entityParameterName = parameters.get(0).getSimpleName().toString();

    NullSavingStrategy nullSavingStrategy =
        nullSavingStrategyValidation.getNullSavingStrategy(
            Insert.class, Insert::nullSavingStrategy, methodElement, enclosingClass);

    createStatementBlock.addStatement(
        "$1L.set($2L, boundStatementBuilder, $3T.$4L, false)",
        helperFieldName,
        entityParameterName,
        NullSavingStrategy.class,
        nullSavingStrategy);

    // Handle all remaining parameters as additional bound values
    if (parameters.size() > 1) {
      List<? extends VariableElement> bindMarkers = parameters.subList(1, parameters.size());
      if (validateCqlNamesPresent(bindMarkers)) {
        GeneratedCodePatterns.bindParameters(
            bindMarkers, createStatementBlock, enclosingClass, context, false);
      } else {
        return Optional.empty();
      }
    }

    createStatementBlock.addStatement(
        "$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

    return crudMethod(createStatementBlock, returnType, helperFieldName);
  }

  private void generatePrepareRequest(
      MethodSpec.Builder methodBuilder, String requestName, String helperFieldName) {
    methodBuilder.addCode(
        "$[$T $L = $L.insert()", SimpleStatement.class, requestName, helperFieldName);
    Insert annotation = methodElement.getAnnotation(Insert.class);
    if (annotation.ifNotExists()) {
      methodBuilder.addCode(".ifNotExists()");
    }

    maybeAddTtl(annotation.ttl(), methodBuilder);
    maybeAddTimestamp(annotation.timestamp(), methodBuilder);

    methodBuilder.addCode(".build()$];\n");
  }
}
