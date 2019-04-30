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

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import java.util.List;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoInsertMethodGenerator extends DaoMethodGenerator {

  public DaoInsertMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, enclosingClass, context);
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the parameters:
    // - the first one must be the entity.
    // - the others are completely free-form (they'll be used as additional bind variables)
    if (methodElement.getParameters().isEmpty()) {
      context
          .getMessager()
          .error(
              methodElement,
              "Wrong number of parameters: %s methods must have at least one",
              Insert.class.getSimpleName());
      return Optional.empty();
    }
    VariableElement firstParameter = methodElement.getParameters().get(0);
    TypeElement entityElement = asEntityElement(firstParameter);
    if (entityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid parameter type: "
                  + "%s methods must take the entity to insert as the first parameter",
              Insert.class.getSimpleName());
      return Optional.empty();
    }

    // Validate the return type:
    // - void, CompletionStage/CompletableFuture<Void>
    // - the same entity, or a future thereof (for INSERT... IF NOT EXISTS)
    Boolean isVoid = null;
    Boolean isAsync = null;
    TypeMirror returnTypeMirror = methodElement.getReturnType();
    if (returnTypeMirror.getKind() == TypeKind.VOID) {
      isVoid = true;
      isAsync = false;
    } else if (returnTypeMirror.getKind() == TypeKind.DECLARED) {
      DeclaredType declaredReturnType = (DeclaredType) returnTypeMirror;
      if (context.getClassUtils().isFuture(declaredReturnType)) {
        TypeMirror typeArgumentMirror = declaredReturnType.getTypeArguments().get(0);
        if ((typeArgumentMirror.getKind() == TypeKind.DECLARED)) {
          if (context.getClassUtils().isSame(typeArgumentMirror, Void.class)) {
            isVoid = true;
            isAsync = true;
          } else if (entityElement.equals(((DeclaredType) typeArgumentMirror).asElement())) {
            isVoid = false;
            isAsync = true;
          }
        }
      } else if (entityElement.equals(declaredReturnType.asElement())) {
        isVoid = false;
        isAsync = false;
      }
    }
    if (isVoid == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type: %s methods must return either void or the entity class "
                  + "(possibly wrapped in a CompletionStage/CompletableFuture)",
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

    MethodSpec.Builder insertBuilder = GeneratedCodePatterns.override(methodElement);

    if (isAsync) {
      insertBuilder.beginControlFlow("try");
    }
    insertBuilder.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);

    List<? extends VariableElement> parameters = methodElement.getParameters();
    String entityParameterName = parameters.get(0).getSimpleName().toString();
    insertBuilder.addStatement(
        "$L.set($L, boundStatementBuilder)", helperFieldName, entityParameterName);

    // Handle all remaining parameters as additional bound values
    if (parameters.size() > 1) {
      GeneratedCodePatterns.bindParameters(
          parameters.subList(1, parameters.size()), insertBuilder, enclosingClass, context);
    }

    insertBuilder
        .addCode("\n")
        .addStatement("$T boundStatement = boundStatementBuilder.build()", BoundStatement.class);

    if (isAsync) {
      if (isVoid) {
        insertBuilder.addStatement("return executeAsyncAndMapToVoid(boundStatement)");
      } else {
        insertBuilder.addStatement(
            "return executeAsyncAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
      }
      insertBuilder
          .nextControlFlow("catch ($T t)", Throwable.class)
          .addStatement("return $T.failedFuture(t)", CompletableFutures.class)
          .endControlFlow();
    } else {
      if (isVoid) {
        insertBuilder.addStatement("execute(boundStatement)");
      } else {
        insertBuilder.addStatement(
            "return executeAndMapToSingleEntity(boundStatement, $L)", helperFieldName);
      }
    }
    return Optional.of(insertBuilder.build());
  }

  private void generatePrepareRequest(
      MethodSpec.Builder methodBuilder, String requestName, String helperFieldName) {
    methodBuilder.addCode(
        "$[$1T $2L = $1T.newInstance($3L.insert().asCql()",
        SimpleStatement.class,
        requestName,
        helperFieldName);
    String customClause = methodElement.getAnnotation(Insert.class).customClause();
    if (!customClause.isEmpty()) {
      methodBuilder.addCode(" + $S", " " + customClause);
    }
    methodBuilder.addCode(")$];\n");
  }
}
