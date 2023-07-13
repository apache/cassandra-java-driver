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
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_ASYNC_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_BOOLEAN;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_VOID;
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
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;

public class DaoDeleteMethodGenerator extends DaoMethodGenerator {

  public DaoDeleteMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, enclosingClass, context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    return ImmutableSet.of(
        VOID, FUTURE_OF_VOID, BOOLEAN, FUTURE_OF_BOOLEAN, RESULT_SET, FUTURE_OF_ASYNC_RESULT_SET);
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
                  + "must take either an entity instance, or the partition key components",
              Delete.class.getSimpleName());
      return Optional.empty();
    }
    VariableElement firstParameter = parameters.get(0);
    entityElement = EntityUtils.asEntityElement(firstParameter, typeParameters);
    hasEntityParameter = (entityElement != null);
    if (hasEntityParameter) {
      entityDefinition = context.getEntityFactory().getDefinition(entityElement);
    } else {
      entityElement = getEntityFromAnnotation();
      if (entityElement == null) {
        context
            .getMessager()
            .error(
                methodElement,
                "Missing entity class: %s methods that do not operate on an entity "
                    + "instance must have an 'entityClass' argument",
                Delete.class.getSimpleName());
        return Optional.empty();
      }
      entityDefinition = context.getEntityFactory().getDefinition(entityElement);
      List<TypeName> primaryKeyTypes =
          entityDefinition
              .getPrimaryKey()
              .stream()
              .map(d -> d.getType().asTypeName())
              .collect(Collectors.toList());
      List<TypeName> parameterTypes =
          parameters.stream().map(p -> TypeName.get(p.asType())).collect(Collectors.toList());
      // Note that we only check the types: we allow the names to be different.
      if (parameterTypes.size() < primaryKeyTypes.size()
          || !primaryKeyTypes.equals(parameterTypes.subList(0, primaryKeyTypes.size()))) {
        context
            .getMessager()
            .error(
                methodElement,
                "Invalid parameter list: %s methods that do not operate on an entity instance "
                    + "must take the partition key components in the exact order "
                    + "(expected PK of %s: %s)",
                Delete.class.getSimpleName(),
                entityElement.getSimpleName(),
                primaryKeyTypes);
        return Optional.empty();
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
                generatePrepareRequest(methodBuilder, requestName, helperFieldName));

    CodeBlock.Builder methodBodyBuilder = CodeBlock.builder();

    methodBodyBuilder.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);
    populateBuilderWithStatementAttributes(methodBodyBuilder, methodElement);
    populateBuilderWithFunction(methodBodyBuilder, boundStatementFunction);
    int nextParameterIndex;
    if (hasEntityParameter) {
      warnIfCqlNamePresent(Collections.singletonList(firstParameter));
      // Bind entity's PK properties
      for (PropertyDefinition property : entityDefinition.getPrimaryKey()) {
        GeneratedCodePatterns.setValue(
            property.getCqlName(),
            property.getType(),
            CodeBlock.of("$L.$L()", firstParameter.getSimpleName(), property.getGetterName()),
            "boundStatementBuilder",
            methodBodyBuilder,
            enclosingClass);
      }
      nextParameterIndex = 1;
    } else {
      // The PK components are passed as arguments to the method (we've already checked that the
      // types match).
      List<CodeBlock> primaryKeyNames =
          entityDefinition
              .getPrimaryKey()
              .stream()
              .map(PropertyDefinition::getCqlName)
              .collect(Collectors.toList());
      List<? extends VariableElement> bindMarkers = parameters.subList(0, primaryKeyNames.size());
      warnIfCqlNamePresent(bindMarkers);
      GeneratedCodePatterns.bindParameters(
          bindMarkers, primaryKeyNames, methodBodyBuilder, enclosingClass, context, false);
      nextParameterIndex = primaryKeyNames.size();
    }

    // Bind any remaining parameters, assuming they are values for a custom IF clause
    if (nextParameterIndex < parameters.size()) {
      if (annotation.customIfClause().isEmpty()) {
        context
            .getMessager()
            .error(
                methodElement,
                "Wrong number of parameters: %s methods can only have additional "
                    + "parameters if they specify a custom IF clause",
                Delete.class.getSimpleName());
      }
      List<? extends VariableElement> bindMarkers =
          parameters.subList(nextParameterIndex, parameters.size());
      if (validateCqlNamesPresent(bindMarkers)) {
        GeneratedCodePatterns.bindParameters(
            bindMarkers, methodBodyBuilder, enclosingClass, context, false);
      } else {
        return Optional.empty();
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

  private TypeElement getEntityFromAnnotation() {
    // Note: because Delete.entityClass references a class, we can't read it directly through
    // methodElement.getAnnotation(Delete.class).

    AnnotationMirror annotationMirror = null;
    for (AnnotationMirror candidate : methodElement.getAnnotationMirrors()) {
      if (context.getClassUtils().isSame(candidate.getAnnotationType(), Delete.class)) {
        annotationMirror = candidate;
        break;
      }
    }
    assert annotationMirror != null;

    for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
        annotationMirror.getElementValues().entrySet()) {
      if (entry.getKey().getSimpleName().contentEquals("entityClass")) {
        @SuppressWarnings("unchecked")
        List<? extends AnnotationValue> values = (List) entry.getValue().getValue();
        if (values.isEmpty()) {
          return null;
        }
        TypeMirror mirror = (TypeMirror) values.get(0).getValue();
        TypeElement element = EntityUtils.asEntityElement(mirror, typeParameters);
        if (values.size() > 1) {
          context
              .getMessager()
              .warn(
                  methodElement,
                  "Too many entity classes: %s must have at most one 'entityClass' argument "
                      + "(will use the first one: %s)",
                  Delete.class.getSimpleName(),
                  element.getSimpleName());
        }
        return element;
      }
    }
    return null;
  }

  private void generatePrepareRequest(
      MethodSpec.Builder methodBuilder, String requestName, String helperFieldName) {

    boolean ifExists = methodElement.getAnnotation(Delete.class).ifExists();
    String customIfClause = methodElement.getAnnotation(Delete.class).customIfClause();

    if (ifExists) {
      methodBuilder.addStatement(
          "$T $L = $L.deleteByPrimaryKey().ifExists().build()",
          SimpleStatement.class,
          requestName,
          helperFieldName);
    } else if (!customIfClause.isEmpty()) {
      methodBuilder.addStatement(
          "$T $L = $L.deleteByPrimaryKey().ifRaw($S).build()",
          SimpleStatement.class,
          requestName,
          helperFieldName,
          customIfClause);
    } else {
      methodBuilder.addStatement(
          "$T $L = $L.deleteByPrimaryKey().build()",
          SimpleStatement.class,
          requestName,
          helperFieldName);
    }
  }
}
