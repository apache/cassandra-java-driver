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
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ASYNC_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_BOOLEAN;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_VOID;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.REACTIVE_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.VOID;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Update;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.datastax.oss.driver.internal.mapper.processor.entity.PropertyDefinition;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.internal.querybuilder.update.DefaultUpdate;
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

public class DaoUpdateMethodGenerator extends DaoMethodGenerator {

  private final NullSavingStrategyValidation nullSavingStrategyValidation;

  public DaoUpdateMethodGenerator(
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
        RESULT_SET,
        BOUND_STATEMENT,
        FUTURE_OF_ASYNC_RESULT_SET,
        BOOLEAN,
        FUTURE_OF_BOOLEAN,
        REACTIVE_RESULT_SET,
        CUSTOM);
  }

  @Override
  public boolean requiresReactive() {
    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Update.class.getSimpleName());
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
              "%s methods must take the entity to update as the first parameter",
              Update.class.getSimpleName());
      return Optional.empty();
    }
    warnIfCqlNamePresent(parameters.subList(0, 1));
    EntityDefinition entityDefinition = context.getEntityFactory().getDefinition(entityElement);

    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Update.class.getSimpleName());
    if (returnType == null) {
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

    String entityParameterName = parameters.get(0).getSimpleName().toString();

    Update annotation = methodElement.getAnnotation(Update.class);
    String customWhereClause = annotation.customWhereClause();

    NullSavingStrategy nullSavingStrategy =
        nullSavingStrategyValidation.getNullSavingStrategy(
            Update.class, Update::nullSavingStrategy, methodElement, enclosingClass);

    if (customWhereClause.isEmpty()) {
      // We generated an update by primary key (see maybeAddWhereClause), all entity properties are
      // present as placeholders.
      createStatementBlock.addStatement(
          "$1L.set($2L, boundStatementBuilder, $3T.$4L, false)",
          helperFieldName,
          entityParameterName,
          NullSavingStrategy.class,
          nullSavingStrategy);
    } else {
      createStatementBlock.addStatement(
          "$1T nullSavingStrategy = $1T.$2L", NullSavingStrategy.class, nullSavingStrategy);

      // Only non-PK properties are present in SET ... clauses.
      // (if the custom clause has custom placeholders, this will be addressed below)
      for (PropertyDefinition property : entityDefinition.getRegularColumns()) {
        GeneratedCodePatterns.setValue(
            property.getCqlName(),
            property.getType(),
            CodeBlock.of("$L.$L()", entityParameterName, property.getGetterName()),
            "boundStatementBuilder",
            createStatementBlock,
            enclosingClass,
            true,
            false);
      }
    }

    // Handle all remaining parameters as additional bound values in customWhereClause or
    // customIfClause
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
    Update annotation = methodElement.getAnnotation(Update.class);

    maybeAddWhereClause(
        methodBuilder, requestName, helperFieldName, annotation.customWhereClause());
    maybeAddTtl(annotation.ttl(), methodBuilder);
    maybeAddTimestamp(annotation.timestamp(), methodBuilder);
    maybeAddTimeout(annotation.usingTimeout(), methodBuilder);
    methodBuilder.addCode(")");
    maybeAddIfClause(methodBuilder, annotation);

    methodBuilder.addCode(".asCql()");
    methodBuilder.addCode(")$];\n");
  }

  private void maybeAddWhereClause(
      MethodSpec.Builder methodBuilder,
      String requestName,
      String helperFieldName,
      String customWhereClause) {

    if (customWhereClause.isEmpty()) {
      methodBuilder.addCode(
          "$[$1T $2L = $1T.newInstance((($4T)$3L.updateByPrimaryKey()",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          DefaultUpdate.class);
    } else {
      methodBuilder.addCode(
          "$[$1T $2L = $1T.newInstance((($5T)$3L.updateStart().whereRaw($4S)",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          customWhereClause,
          DefaultUpdate.class);
    }
  }

  private void maybeAddIfClause(MethodSpec.Builder methodBuilder, Update annotation) {
    if (annotation.ifExists() && !annotation.customIfClause().isEmpty()) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid annotation parameters: %s cannot have both ifExists and customIfClause",
              Update.class.getSimpleName());
    }

    if (annotation.ifExists()) {
      methodBuilder.addCode(".ifExists()");
    }

    if (!annotation.customIfClause().isEmpty()) {
      methodBuilder.addCode(".ifRaw($S)", annotation.customIfClause());
    }
  }
}
