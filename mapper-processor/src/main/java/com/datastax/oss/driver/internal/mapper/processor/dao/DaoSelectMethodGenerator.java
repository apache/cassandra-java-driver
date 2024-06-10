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

import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.CUSTOM;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ASYNC_PAGING_ITERABLE;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_STREAM;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.MAPPED_REACTIVE_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.OPTIONAL_ENTITY;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.PAGING_ITERABLE;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.STREAM;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Select;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.datastax.oss.driver.internal.mapper.processor.entity.PropertyDefinition;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import java.util.Collections;
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
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    return ImmutableSet.of(
        ENTITY,
        OPTIONAL_ENTITY,
        FUTURE_OF_ENTITY,
        FUTURE_OF_OPTIONAL_ENTITY,
        PAGING_ITERABLE,
        STREAM,
        FUTURE_OF_ASYNC_PAGING_ITERABLE,
        FUTURE_OF_STREAM,
        MAPPED_REACTIVE_RESULT_SET,
        CUSTOM);
  }

  @Override
  public boolean requiresReactive() {
    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Select.class.getSimpleName());
    if (returnType == null) {
      return false;
    }
    return returnType.requiresReactive();
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
    // - if there is a custom clause, they are free-form (they'll be used as bind variables)
    // - otherwise, we accept the primary key components or a subset thereof (possibly empty to
    //   select all rows), followed by free-form parameters bound to the secondary clauses (such as
    //   LIMIT).
    // In either case, a Function<BoundStatementBuilder, BoundStatementBuilder> can be added in last
    // position.
    List<? extends VariableElement> parameters = methodElement.getParameters();

    VariableElement boundStatementFunction = findBoundStatementFunction(methodElement);
    if (boundStatementFunction != null) {
      parameters = parameters.subList(0, parameters.size() - 1);
    }

    final List<? extends VariableElement> primaryKeyParameters;
    final List<? extends VariableElement> freeFormParameters;
    Select selectAnnotation = methodElement.getAnnotation(Select.class);
    assert selectAnnotation != null; // otherwise we wouldn't have gotten into this class
    String customClause = selectAnnotation.customWhereClause();
    if (parameters.isEmpty()) {
      primaryKeyParameters = freeFormParameters = Collections.emptyList();
    } else if (customClause.isEmpty()) {
      // If we have a partial primary key *and* free-form parameters, things get ambiguous because
      // we don't know where the primary key ends. By convention, we require the first free-form
      // parameter to be annotated with @CqlName in those cases.
      // So the boundary is either when we have enough parameters for a full primary key, or when we
      // encounter the first annotated parameter.
      int firstNamedParameter = parameters.size();
      for (int i = 0; i < parameters.size(); i++) {
        if (parameters.get(i).getAnnotation(CqlName.class) != null) {
          firstNamedParameter = i;
          break;
        }
      }
      int primaryKeyEnd = Math.min(firstNamedParameter, entityDefinition.getPrimaryKey().size());
      if (primaryKeyEnd >= parameters.size()) {
        primaryKeyParameters = parameters;
        freeFormParameters = Collections.emptyList();
      } else {
        primaryKeyParameters = parameters.subList(0, primaryKeyEnd);
        freeFormParameters = parameters.subList(primaryKeyEnd, parameters.size());
      }
    } else {
      primaryKeyParameters = Collections.emptyList();
      freeFormParameters = parameters;
    }

    // If we have parameters for some primary key components, validate that the types match:
    if (!primaryKeyParameters.isEmpty()
        && !EntityUtils.areParametersValid(
            entityElement,
            entityDefinition,
            primaryKeyParameters,
            Select.class,
            context,
            methodElement,
            processedType,
            "don't use a custom clause")) {
      return Optional.empty();
    }

    // Generate the method:
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generateSelectRequest(
                    methodBuilder, requestName, helperFieldName, primaryKeyParameters.size()));

    CodeBlock.Builder createStatementBlock = CodeBlock.builder();

    createStatementBlock.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);
    populateBuilderWithStatementAttributes(createStatementBlock, methodElement);
    populateBuilderWithFunction(createStatementBlock, boundStatementFunction);

    if (!primaryKeyParameters.isEmpty()) {
      List<CodeBlock> primaryKeyNames =
          entityDefinition.getPrimaryKey().stream()
              .map(PropertyDefinition::getCqlName)
              .collect(Collectors.toList())
              .subList(0, primaryKeyParameters.size());
      GeneratedCodePatterns.bindParameters(
          primaryKeyParameters,
          primaryKeyNames,
          createStatementBlock,
          enclosingClass,
          context,
          false);
    }

    if (!freeFormParameters.isEmpty()) {
      if (validateCqlNamesPresent(freeFormParameters)) {
        GeneratedCodePatterns.bindParameters(
            freeFormParameters, createStatementBlock, enclosingClass, context, false);
      } else {
        return Optional.empty();
      }
    }

    createStatementBlock.addStatement(
        "$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

    return crudMethod(createStatementBlock, returnType, helperFieldName);
  }

  private void generateSelectRequest(
      MethodSpec.Builder methodBuilder,
      String requestName,
      String helperFieldName,
      int numberOfPrimaryKeyPartsInWhereClause) {
    Select annotation = methodElement.getAnnotation(Select.class);
    String customWhereClause = annotation.customWhereClause();
    if (customWhereClause.isEmpty()) {
      methodBuilder.addCode(
          "$[$T $L = $L.selectByPrimaryKeyParts($L)",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          numberOfPrimaryKeyPartsInWhereClause);
    } else {
      methodBuilder.addCode(
          "$[$T $L = $L.selectStart().whereRaw($S)",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          customWhereClause);
    }
    maybeAddSimpleClause(annotation.limit(), Integer::parseInt, "limit", "limit", methodBuilder);
    maybeAddSimpleClause(
        annotation.perPartitionLimit(),
        Integer::parseInt,
        "perPartitionLimit",
        "perPartitionLimit",
        methodBuilder);
    maybeAddTimeout(annotation.usingTimeout(), methodBuilder);
    for (String orderingSpec : annotation.orderBy()) {
      addOrdering(orderingSpec, methodBuilder);
    }
    for (String groupByColumn : annotation.groupBy()) {
      methodBuilder.addCode(".groupBy($S)", groupByColumn);
    }
    if (annotation.allowFiltering()) {
      methodBuilder.addCode(".allowFiltering()");
    }
    if (annotation.bypassCache()) {
      methodBuilder.addCode(".bypassCache()");
    }
    methodBuilder.addCode(".build();$]\n");
  }

  private void addOrdering(String orderingSpec, MethodSpec.Builder methodBuilder) {
    List<String> tokens = ON_SPACES.splitToList(orderingSpec);
    ClusteringOrder clusteringOrder;
    if (tokens.size() != 2 || (clusteringOrder = parseClusteringOrder(tokens.get(1))) == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Can't parse ordering '%s', expected a column name followed by ASC or DESC",
              orderingSpec);
      return;
    }
    methodBuilder.addCode(
        ".orderBy($S, $T.$L)", tokens.get(0), ClusteringOrder.class, clusteringOrder);
  }

  private ClusteringOrder parseClusteringOrder(String spec) {
    try {
      return ClusteringOrder.valueOf(spec.toUpperCase());
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static final Splitter ON_SPACES = Splitter.on(' ').omitEmptyStrings();
}
