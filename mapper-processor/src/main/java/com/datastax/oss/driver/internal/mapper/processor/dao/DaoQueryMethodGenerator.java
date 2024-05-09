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

import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.UNSUPPORTED;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.values;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Query;
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

public class DaoQueryMethodGenerator extends DaoMethodGenerator {

  private final String queryString;
  private final NullSavingStrategyValidation nullSavingStrategyValidation;

  public DaoQueryMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
    this.queryString = methodElement.getAnnotation(Query.class).value();
    nullSavingStrategyValidation = new NullSavingStrategyValidation(context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    ImmutableSet.Builder<DaoReturnTypeKind> builder = ImmutableSet.builder();
    for (DefaultDaoReturnTypeKind value : values()) {
      if (value != UNSUPPORTED) {
        builder.add(value);
      }
    }
    return builder.build();
  }

  @Override
  public boolean requiresReactive() {
    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Query.class.getSimpleName());
    if (returnType == null) {
      return false;
    }
    return returnType.requiresReactive();
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Query.class.getSimpleName());
    if (returnType == null) {
      return Optional.empty();
    }

    // Generate the method:
    TypeElement entityElement = returnType.getEntityElement();
    String helperFieldName =
        (entityElement == null)
            ? null
            : enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generatePrepareRequest(methodBuilder, requestName, helperFieldName));

    CodeBlock.Builder createStatementBlock = CodeBlock.builder();

    List<? extends VariableElement> parameters = methodElement.getParameters();

    VariableElement boundStatementFunction = findBoundStatementFunction(methodElement);
    if (boundStatementFunction != null) {
      parameters = parameters.subList(0, methodElement.getParameters().size() - 1);
    }

    createStatementBlock.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);

    NullSavingStrategy nullSavingStrategy =
        nullSavingStrategyValidation.getNullSavingStrategy(
            Query.class, Query::nullSavingStrategy, methodElement, enclosingClass);

    createStatementBlock.addStatement(
        "$1T nullSavingStrategy = $1T.$2L", NullSavingStrategy.class, nullSavingStrategy);

    populateBuilderWithStatementAttributes(createStatementBlock, methodElement);
    populateBuilderWithFunction(createStatementBlock, boundStatementFunction);

    if (validateCqlNamesPresent(parameters)) {
      GeneratedCodePatterns.bindParameters(
          parameters, createStatementBlock, enclosingClass, context, true);

      createStatementBlock.addStatement(
          "$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

      return crudMethod(createStatementBlock, returnType, helperFieldName);
    } else {
      return Optional.empty();
    }
  }

  private void generatePrepareRequest(
      MethodSpec.Builder methodBuilder, String requestName, String helperFieldName) {
    methodBuilder.addStatement(
        "$T $L = replaceKeyspaceAndTablePlaceholders($S, context, $L)",
        SimpleStatement.class,
        requestName,
        queryString,
        helperFieldName);
  }
}
