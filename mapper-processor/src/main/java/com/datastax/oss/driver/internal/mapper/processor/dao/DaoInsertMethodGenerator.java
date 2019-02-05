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
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Insert;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.SkipGenerationException;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoInsertMethodGenerator implements MethodGenerator {

  private final ExecutableElement methodElement;
  private final DaoImplementationGenerator enclosingClass;
  private final ProcessorContext context;
  private final TypeElement entityElement;
  private final boolean isVoid;
  private final boolean isAsync;

  public DaoInsertMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationGenerator enclosingClass,
      ProcessorContext context) {
    this.methodElement = methodElement;
    this.enclosingClass = enclosingClass;
    this.context = context;

    // We're accepting:
    // Parameters:
    // - the first one must be the entity.
    // - the others are completely free-form (they'll be used as additional bind variables)
    // Return type:
    // - void, CompletionStage/CompletableFuture<Void>
    // - the same entity, or a future thereof (for INSERT... IF NOT EXISTS)
    if (methodElement.getParameters().isEmpty()) {
      context
          .getMessager()
          .error(
              methodElement,
              "%s methods must have at least one parameter",
              Insert.class.getSimpleName());
      throw new SkipGenerationException();
    }
    VariableElement firstParameter = methodElement.getParameters().get(0);
    entityElement = extractEntityElement(firstParameter);
    if (entityElement == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "%s methods must take the entity to insert as the first parameter",
              Insert.class.getSimpleName());
      throw new SkipGenerationException();
    }

    TypeMirror returnTypeMirror = methodElement.getReturnType();
    Boolean tmpVoid = null;
    Boolean tmpAsync = null;
    if (returnTypeMirror.getKind() == TypeKind.VOID) {
      tmpVoid = true;
      tmpAsync = false;
    } else if (returnTypeMirror.getKind() == TypeKind.DECLARED) {
      DeclaredType declaredReturnType = (DeclaredType) returnTypeMirror;
      if (isFuture(declaredReturnType.asElement())) {
        TypeMirror typeArgumentMirror = declaredReturnType.getTypeArguments().get(0);
        if ((typeArgumentMirror.getKind() == TypeKind.DECLARED)) {
          if (context.getClassUtils().isSame(typeArgumentMirror, Void.class)) {
            tmpVoid = true;
            tmpAsync = true;
          } else if (entityElement.equals(((DeclaredType) typeArgumentMirror).asElement())) {
            tmpVoid = false;
            tmpAsync = true;
          }
        }
      } else if (entityElement.equals(declaredReturnType.asElement())) {
        tmpVoid = false;
        tmpAsync = false;
      }
    }
    if (tmpVoid == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "%s methods must return either void or the entity class "
                  + "(possibly wrapped in a CompletionStage/CompletableFuture)",
              Insert.class.getSimpleName());
      throw new SkipGenerationException();
    }
    this.isVoid = tmpVoid;
    this.isAsync = tmpAsync;
  }

  private TypeElement extractEntityElement(VariableElement parameter) {
    TypeMirror mirror = parameter.asType();
    if (mirror.getKind() != TypeKind.DECLARED) {
      return null;
    }
    Element element = ((DeclaredType) mirror).asElement();
    if (element.getKind() != ElementKind.CLASS) {
      return null;
    }
    TypeElement typeElement = (TypeElement) element;
    if (typeElement.getAnnotation(Entity.class) == null) {
      return null;
    }
    return typeElement;
  }

  // TODO factor this (in Classes?)
  private boolean isFuture(Element element) {
    return context.getClassUtils().isSame(element, CompletionStage.class)
        || context.getClassUtils().isSame(element, CompletableFuture.class);
  }

  @Override
  public MethodSpec.Builder generate() {
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));
    String statementName =
        enclosingClass.addPreparedStatement(
            methodElement,
            (methodBuilder, requestName) ->
                generatePrepareRequest(methodBuilder, requestName, helperFieldName));

    MethodSpec.Builder insertBuilder =
        MethodSpec.methodBuilder(methodElement.getSimpleName().toString())
            .addAnnotation(Override.class)
            .addModifiers(Modifier.PUBLIC)
            .returns(ClassName.get(methodElement.getReturnType()));
    List<? extends VariableElement> parameters = methodElement.getParameters();
    for (VariableElement parameterElement : parameters) {
      insertBuilder.addParameter(
          ClassName.get(parameterElement.asType()), parameterElement.getSimpleName().toString());
    }

    if (isAsync) {
      insertBuilder.beginControlFlow("try");
    }
    insertBuilder.addStatement(
        "$T boundStatementBuilder = $L.boundStatementBuilder()",
        BoundStatementBuilder.class,
        statementName);

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
    return insertBuilder;
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
