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
import com.datastax.oss.driver.api.mapper.annotations.Delete;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.datastax.oss.driver.internal.mapper.processor.entity.PropertyDefinition;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
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

public class DaoDeleteMethodGenerator extends DaoMethodGenerator {

  public DaoDeleteMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    return ImmutableSet.of(
        VOID,
        FUTURE_OF_VOID,
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
    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Delete.class.getSimpleName());
    if (returnType == null) {
      return false;
    }
    return returnType.requiresReactive();
  }

  @Override
  public Optional<MethodSpec> generate() {

    Delete annotation = methodElement.getAnnotation(Delete.class);
    assert annotation != null;
    if (annotation.ifExists() && !annotation.customIfClause().isEmpty()) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid annotation parameters: %s cannot have both ifExists and customIfClause",
              Delete.class.getSimpleName());
      return Optional.empty();
    }

    // Validate the arguments: either an entity instance, or the PK components (in the latter case,
    // the entity class has to be provided via the annotation).
    // In either case, a Function<BoundStatementBuilder, BoundStatementBuilder> can be added in last
    // position.
    List<? extends VariableElement> parameters = methodElement.getParameters();
    VariableElement boundStatementFunction = findBoundStatementFunction(methodElement);
    if (boundStatementFunction != null) {
      parameters = parameters.subList(0, parameters.size() - 1);
    }

    TypeElement entityElement;
    EntityDefinition entityDefinition;
    boolean hasEntityParameter;
    if (parameters.isEmpty()) {
      context
          .getMessager()
          .error(
              methodElement,
              "Wrong number of parameters: %s methods with no custom clause "
                  + "must take either an entity instance, or the primary key components",
              Delete.class.getSimpleName());
      return Optional.empty();
    }
    String customWhereClause = annotation.customWhereClause();
    String customIfClause = annotation.customIfClause();
    VariableElement firstParameter = parameters.get(0);
    entityElement = EntityUtils.asEntityElement(firstParameter, typeParameters);
    hasEntityParameter = (entityElement != null);

    // the number of primary key parameters provided, if -1 this implies a custom
    // where clause where number of parameters that are primary key are irrelevant.
    final int primaryKeyParameterCount;
    if (hasEntityParameter) {
      if (!customWhereClause.isEmpty()) {
        context
            .getMessager()
            .error(
                methodElement,
                "Invalid parameter list: %s methods that have a custom where clause "
                    + "must not take an Entity (%s) as a parameter",
                Delete.class.getSimpleName(),
                entityElement.getSimpleName());
      }
      entityDefinition = context.getEntityFactory().getDefinition(entityElement);
      primaryKeyParameterCount = entityDefinition.getPrimaryKey().size();
    } else {
      entityElement = getEntityClassFromAnnotation(Delete.class);
      if (entityElement == null) {
        context
            .getMessager()
            .error(
                methodElement,
                "Missing entity class: %s methods that do not operate on an entity "
                    + "instance must have an 'entityClass' argument",
                Delete.class.getSimpleName());
        return Optional.empty();
      } else {
        entityDefinition = context.getEntityFactory().getDefinition(entityElement);
      }

      if (customWhereClause.isEmpty()) {
        /* if a custom if clause is provided, the whole primary key must also be provided.
         * we only do this check if there is no custom where clause as the order of
         * keys may differ in that case.*/
        List<? extends VariableElement> primaryKeyParameters = parameters;
        if (!customIfClause.isEmpty()) {
          if (primaryKeyParameters.size() < entityDefinition.getPrimaryKey().size()) {
            List<TypeName> primaryKeyTypes =
                entityDefinition.getPrimaryKey().stream()
                    .map(d -> d.getType().asTypeName())
                    .collect(Collectors.toList());
            context
                .getMessager()
                .error(
                    methodElement,
                    "Invalid parameter list: %s methods that have a custom if clause"
                        + "must specify the entire primary key (expected primary keys of %s: %s)",
                    Delete.class.getSimpleName(),
                    entityElement.getSimpleName(),
                    primaryKeyTypes);
            return Optional.empty();
          } else {
            // restrict parameters to primary key length.
            primaryKeyParameters =
                primaryKeyParameters.subList(0, entityDefinition.getPrimaryKey().size());
          }
        }

        primaryKeyParameterCount = primaryKeyParameters.size();
        if (!EntityUtils.areParametersValid(
            entityElement,
            entityDefinition,
            primaryKeyParameters,
            Delete.class,
            context,
            methodElement,
            processedType,
            "do not operate on an entity instance and lack a custom where clause")) {
          return Optional.empty();
        }
      } else {
        primaryKeyParameterCount = -1;
      }
    }

    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Delete.class.getSimpleName());
    if (returnType == null) {
      return Optional.empty();
    }

    // Generate the method
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generatePrepareRequest(
                    methodBuilder, requestName, helperFieldName, primaryKeyParameterCount));

    CodeBlock.Builder createStatementBlock = CodeBlock.builder();

    createStatementBlock.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);
    populateBuilderWithStatementAttributes(createStatementBlock, methodElement);
    populateBuilderWithFunction(createStatementBlock, boundStatementFunction);

    int nextParameterIndex = 0;
    if (hasEntityParameter) {
      warnIfCqlNamePresent(Collections.singletonList(firstParameter));
      // Bind entity's PK properties
      for (PropertyDefinition property : entityDefinition.getPrimaryKey()) {
        GeneratedCodePatterns.setValue(
            property.getCqlName(),
            property.getType(),
            CodeBlock.of("$L.$L()", firstParameter.getSimpleName(), property.getGetterName()),
            "boundStatementBuilder",
            createStatementBlock,
            enclosingClass);
      }
      nextParameterIndex = 1;
    } else if (customWhereClause.isEmpty()) {
      // The PK components are passed as arguments to the method (we've already checked that the
      // types match).
      List<CodeBlock> primaryKeyNames =
          entityDefinition.getPrimaryKey().stream()
              .map(PropertyDefinition::getCqlName)
              .collect(Collectors.toList())
              .subList(0, primaryKeyParameterCount);
      List<? extends VariableElement> bindMarkers = parameters.subList(0, primaryKeyParameterCount);
      warnIfCqlNamePresent(bindMarkers);
      GeneratedCodePatterns.bindParameters(
          bindMarkers, primaryKeyNames, createStatementBlock, enclosingClass, context, false);
      nextParameterIndex = primaryKeyNames.size();
    }

    // Bind any remaining parameters, assuming they are values for a custom WHERE or IF clause
    if (nextParameterIndex < parameters.size()) {
      if (customIfClause.isEmpty() && customWhereClause.isEmpty()) {
        context
            .getMessager()
            .error(
                methodElement,
                "Wrong number of parameters: %s methods can only have additional "
                    + "parameters if they specify a custom WHERE or IF clause",
                Delete.class.getSimpleName());
      }
      List<? extends VariableElement> bindMarkers =
          parameters.subList(nextParameterIndex, parameters.size());
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
      MethodSpec.Builder methodBuilder,
      String requestName,
      String helperFieldName,
      int parameterSize) {

    Delete delete = methodElement.getAnnotation(Delete.class);
    boolean ifExists = delete.ifExists();
    String customWhereClause = delete.customWhereClause();
    String customIfClause = delete.customIfClause();

    methodBuilder.addCode("$[$T $L = $L", SimpleStatement.class, requestName, helperFieldName);

    if (!customWhereClause.isEmpty()) {
      methodBuilder.addCode(".deleteStart().whereRaw($S)", customWhereClause);
    } else {
      methodBuilder.addCode(".deleteByPrimaryKeyParts($L)", parameterSize);
    }

    if (ifExists) {
      methodBuilder.addCode(".ifExists()");
    } else if (!customIfClause.isEmpty()) {
      methodBuilder.addCode(".ifRaw($S)", customIfClause);
    }

    methodBuilder.addCode(".build();$]\n");
  }
}
