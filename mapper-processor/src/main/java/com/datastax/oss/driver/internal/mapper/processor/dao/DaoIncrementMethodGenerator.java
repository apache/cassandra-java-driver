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

import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.FUTURE_OF_VOID;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.REACTIVE_RESULT_SET;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.VOID;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Increment;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.datastax.oss.driver.internal.mapper.processor.entity.PropertyDefinition;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoIncrementMethodGenerator extends DaoMethodGenerator {

  public DaoIncrementMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    return ImmutableSet.of(VOID, FUTURE_OF_VOID, REACTIVE_RESULT_SET);
  }

  @Override
  public boolean requiresReactive() {
    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Increment.class.getSimpleName());
    if (returnType == null) {
      return false;
    }
    return returnType.requiresReactive();
  }

  @Override
  public Optional<MethodSpec> generate() {

    TypeElement entityElement = getEntityClassFromAnnotation(Increment.class);
    EntityDefinition entityDefinition;
    if (entityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Missing entity class: %s methods must always have an 'entityClass' argument",
              Increment.class.getSimpleName());
      return Optional.empty();
    } else {
      entityDefinition = context.getEntityFactory().getDefinition(entityElement);
    }

    // Validate the parameters:
    // - all the PK components of the entity, in order.
    // - one or more increment parameters that must match non-PK columns.
    // - a Function<BoundStatementBuilder, BoundStatementBuilder> can be added in last position.
    List<? extends VariableElement> parameters = methodElement.getParameters();
    VariableElement boundStatementFunction = findBoundStatementFunction(methodElement);
    if (boundStatementFunction != null) {
      parameters = parameters.subList(0, parameters.size() - 1);
    }

    List<? extends VariableElement> primaryKeyParameters = parameters;
    // Must have at least enough parameters for the full PK
    if (primaryKeyParameters.size() < entityDefinition.getPrimaryKey().size()) {
      List<TypeName> primaryKeyTypes =
          entityDefinition.getPrimaryKey().stream()
              .map(d -> d.getType().asTypeName())
              .collect(Collectors.toList());
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid parameter list: %s methods must specify the entire primary key "
                  + "(expected primary keys of %s: %s)",
              Increment.class.getSimpleName(),
              entityElement.getSimpleName(),
              primaryKeyTypes);
      return Optional.empty();
    } else {
      primaryKeyParameters =
          primaryKeyParameters.subList(0, entityDefinition.getPrimaryKey().size());
      warnIfCqlNamePresent(primaryKeyParameters);
    }
    // PK parameter types must match
    if (!EntityUtils.areParametersValid(
        entityElement,
        entityDefinition,
        primaryKeyParameters,
        Increment.class,
        context,
        methodElement,
        processedType,
        "" /* no condition, @Increment must always have the full PK */)) {
      return Optional.empty();
    }

    // The remaining parameters are the increments to the counter columns
    List<? extends VariableElement> incrementParameters =
        parameters.subList(primaryKeyParameters.size(), parameters.size());
    if (!validateCqlNamesPresent(incrementParameters)) {
      return Optional.empty();
    }
    for (VariableElement parameter : incrementParameters) {
      TypeMirror type = parameter.asType();
      if (type.getKind() != TypeKind.LONG && !context.getClassUtils().isSame(type, Long.class)) {
        context
            .getMessager()
            .error(
                methodElement,
                "Invalid argument type: increment parameters of %s methods can only be "
                    + "primitive longs or java.lang.Long. Offending parameter: '%s' (%s)",
                Increment.class.getSimpleName(),
                parameter.getSimpleName(),
                type);
        return Optional.empty();
      }
    }

    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), Increment.class.getSimpleName());
    if (returnType == null) {
      return Optional.empty();
    }

    // Generate the method:
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generatePrepareRequest(
                    methodBuilder,
                    requestName,
                    entityDefinition,
                    helperFieldName,
                    incrementParameters));

    CodeBlock.Builder updateStatementBlock = CodeBlock.builder();

    updateStatementBlock.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);

    populateBuilderWithStatementAttributes(updateStatementBlock, methodElement);
    populateBuilderWithFunction(updateStatementBlock, boundStatementFunction);

    // Bind the counter increments. The bind parameter names are always the raw parameter names, see
    // generatePrepareRequest.
    List<CodeBlock> bindMarkerNames =
        incrementParameters.stream()
            .map(p -> CodeBlock.of("$S", p.getSimpleName()))
            .collect(Collectors.toList());
    // Force the null saving strategy. This will fail if the user targets Cassandra 2.2, but
    // SET_TO_NULL would not work with counters anyway.
    updateStatementBlock.addStatement(
        "final $1T nullSavingStrategy = $1T.$2L",
        NullSavingStrategy.class,
        NullSavingStrategy.DO_NOT_SET);
    GeneratedCodePatterns.bindParameters(
        incrementParameters, bindMarkerNames, updateStatementBlock, enclosingClass, context, true);

    // Bind the PK columns
    List<CodeBlock> primaryKeyNames =
        entityDefinition.getPrimaryKey().stream()
            .map(PropertyDefinition::getCqlName)
            .collect(Collectors.toList());
    GeneratedCodePatterns.bindParameters(
        primaryKeyParameters,
        primaryKeyNames,
        updateStatementBlock,
        enclosingClass,
        context,
        false);

    updateStatementBlock.addStatement(
        "$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

    return crudMethod(updateStatementBlock, returnType, helperFieldName);
  }

  private void generatePrepareRequest(
      MethodSpec.Builder methodBuilder,
      String requestName,
      EntityDefinition entityDefinition,
      String helperFieldName,
      List<? extends VariableElement> incrementParameters) {

    if (incrementParameters.isEmpty()) {
      context
          .getMessager()
          .error(
              methodElement,
              "%s method must take at least one parameter representing an increment to a "
                  + "counter column",
              Increment.class.getSimpleName());
      return;
    }

    methodBuilder
        .addStatement("$L.throwIfKeyspaceMissing()", helperFieldName)
        .addCode(
            "$[$1T $2L = (($3L.getKeyspaceId() == null)\n"
                + "? $4T.update($3L.getTableId())\n"
                + ": $4T.update($3L.getKeyspaceId(), $3L.getTableId()))",
            SimpleStatement.class,
            requestName,
            helperFieldName,
            QueryBuilder.class);

    // Add an increment clause for every non-PK parameter. Example: for a parameter `long oneStar`
    // =>  `.append("one_star", QueryBuilder.bindMarker("oneStar"))`
    for (VariableElement parameter : incrementParameters) {
      CodeBlock cqlName = null;
      CqlName annotation = parameter.getAnnotation(CqlName.class);
      if (annotation != null) {
        // If a CQL name is provided, use that
        cqlName = CodeBlock.of("$S", annotation.value());
      } else {
        // Otherwise, try to match the parameter to an entity property based on the names, for
        // example parameter `oneStar` matches `ProductRating.getOneStar()`.
        for (PropertyDefinition property : entityDefinition.getRegularColumns()) {
          if (property.getJavaName().equals(parameter.getSimpleName().toString())) {
            cqlName = property.getCqlName();
            break;
          }
        }
        if (cqlName == null) {
          List<String> javaNames =
              StreamSupport.stream(entityDefinition.getRegularColumns().spliterator(), false)
                  .map(PropertyDefinition::getJavaName)
                  .collect(Collectors.toList());
          context
              .getMessager()
              .error(
                  parameter,
                  "Could not match '%s' with any counter column in %s (expected one of: %s). "
                      + "You can also specify a CQL name directly with @%s.",
                  parameter.getSimpleName(),
                  entityDefinition.getClassName().simpleName(),
                  javaNames,
                  CqlName.class.getSimpleName());
          // Don't return abruptly, execute the rest of the method to finish the Java statement
          // cleanly (otherwise JavaPoet throws an error). The generated statement will be
          // incorrect, but that doesn't matter since we've already thrown a compile error.
          break;
        }
      }

      // Always use the parameter name. This is what the binding code will expect (see
      // GeneratedCodePatterns.bindParameters call in generate())
      String bindMarkerName = parameter.getSimpleName().toString();

      // We use `append` to generate "c=c+?". QueryBuilder also has `increment` that produces
      // "c+=?", but that doesn't work with Cassandra 2.1.
      methodBuilder.addCode(
          "\n.append($1L, $2T.bindMarker($3S))", cqlName, QueryBuilder.class, bindMarkerName);
    }

    for (PropertyDefinition property : entityDefinition.getPrimaryKey()) {
      methodBuilder.addCode(
          "\n.where($1T.column($2L).isEqualTo($3T.bindMarker($2L)))",
          Relation.class,
          property.getCqlName(),
          QueryBuilder.class);
    }

    methodBuilder.addCode("\n.build()$];\n");
  }
}
