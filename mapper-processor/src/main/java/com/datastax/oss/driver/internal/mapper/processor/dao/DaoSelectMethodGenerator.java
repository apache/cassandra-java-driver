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

import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ASYNC_PAGING_ITERABLE;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.PAGING_ITERABLE;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.datastax.oss.driver.internal.mapper.processor.entity.PropertyDefinition;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

public class DaoSelectMethodGenerator extends DaoMethodGenerator {

  public DaoSelectMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, enclosingClass, context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    return ImmutableSet.of(
        ENTITY,
        OPTIONAL_ENTITY,
        FUTURE_OF_ENTITY,
        FUTURE_OF_OPTIONAL_ENTITY,
        PAGING_ITERABLE,
        FUTURE_OF_ASYNC_PAGING_ITERABLE);
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Select.class.getSimpleName());
    if (returnType == null) {
      return Optional.empty();
    }
    TypeElement entityElement = returnType.getEntityElement();
    EntityDefinition entityDefinition = context.getEntityFactory().getDefinition(entityElement);

    // Validate the parameters:
    // - if there is a custom clause, they are free-form  (they'll be used as bind variables)
    // - otherwise, they must be an exact match for the entity's primary key
    // In either case, a Function<BoundStatementBuilder, BoundStatementBuilder> can be added in last
    // position.
    List<? extends VariableElement> parameters = methodElement.getParameters();

    VariableElement boundStatementFunction = findBoundStatementFunction(methodElement);
    if (boundStatementFunction != null) {
      parameters = parameters.subList(0, parameters.size() - 1);
    }
    Select selectAnnotation = methodElement.getAnnotation(Select.class);
    assert selectAnnotation != null; // otherwise we wouldn't have gotten into this class
    String customClause = selectAnnotation.customWhereClause();
    // select without where criteria is ok.
    // if parameters are provided, we must have at least enough to match partition key.
    if (customClause.isEmpty()
        && !parameters.isEmpty()
        && !EntityUtils.areParametersValid(
            context,
            methodElement,
            entityElement,
            entityDefinition,
            parameters,
            Select.class,
            "don't use a custom clause")) {
      return Optional.empty();
    }

    // Generate the method:
    final int parameterSize = parameters.size();
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generateSelectRequest(methodBuilder, requestName, helperFieldName, parameterSize));

    CodeBlock.Builder methodBodyBuilder = CodeBlock.builder();

    methodBodyBuilder.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);
    populateBuilderWithStatementAttributes(methodBodyBuilder, methodElement);
    populateBuilderWithFunction(methodBodyBuilder, boundStatementFunction);
    if (parameters.size() > 0) {
      if (customClause.isEmpty()) {
        // Parameters are the PK components, we allow them to be named differently
        List<CodeBlock> primaryKeyNames =
            entityDefinition.getPrimaryKey().stream()
                .map(PropertyDefinition::getCqlName)
                .collect(Collectors.toList());
        warnIfCqlNamePresent(parameters);
        GeneratedCodePatterns.bindParameters(
            parameters, primaryKeyNames, methodBodyBuilder, enclosingClass, context, false);
      } else {
        if (validateCqlNamesPresent(parameters)) {
          GeneratedCodePatterns.bindParameters(
              parameters, methodBodyBuilder, enclosingClass, context, false);
        } else {
          return Optional.empty();
        }
      }
    }
    methodBodyBuilder
        .add("\n")
        .addStatement("$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

    returnType.getKind().addExecuteStatement(methodBodyBuilder, helperFieldName);

    CodeBlock methodBody = returnType.getKind().wrapWithErrorHandling(methodBodyBuilder.build());

    return Optional.of(
        GeneratedCodePatterns.override(methodElement, typeParameters).addCode(methodBody).build());
  }

  private void generateSelectRequest(
      MethodSpec.Builder methodBuilder,
      String requestName,
      String helperFieldName,
      int parameterSize) {
    String customWhereClause = methodElement.getAnnotation(Select.class).customWhereClause();
    if (customWhereClause.isEmpty()) {
      methodBuilder.addStatement(
          "$T $L = $L.selectByPrimaryKeyParts($L).build()",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          parameterSize);
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
