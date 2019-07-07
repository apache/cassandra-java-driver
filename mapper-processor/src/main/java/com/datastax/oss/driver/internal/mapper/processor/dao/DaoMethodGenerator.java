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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.StatementAttributes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.CodeBlock;
import com.squareup.javapoet.MethodSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public abstract class DaoMethodGenerator implements MethodGenerator {

  protected final ExecutableElement methodElement;
  protected final DaoImplementationSharedCode enclosingClass;
  protected final ProcessorContext context;
  protected final Map<Name, TypeElement> typeParameters;

  public DaoMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    this.methodElement = methodElement;
    this.typeParameters = typeParameters;
    this.enclosingClass = enclosingClass;
    this.context = context;
  }

  @Nullable
  protected DaoReturnType parseAndValidateReturnType(
      @NonNull Set<DaoReturnTypeKind> validKinds, @NonNull String annotationName) {
    DaoReturnType returnType =
        context
            .getCodeGeneratorFactory()
            .getDaoReturnTypeParser()
            .parse(methodElement.getReturnType(), typeParameters);
    if (!validKinds.contains(returnType.getKind())) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type: %s methods must return one of %s",
              annotationName,
              validKinds);
      return null;
    }
    return returnType;
  }

  protected void maybeAddTtl(String ttl, MethodSpec.Builder methodBuilder) {
    maybeAddSimpleClause(ttl, Integer::parseInt, "usingTtl", "ttl", methodBuilder);
  }

  protected void maybeAddTimestamp(String timestamp, MethodSpec.Builder methodBuilder) {
    maybeAddSimpleClause(timestamp, Long::parseLong, "usingTimestamp", "timestamp", methodBuilder);
  }

  protected void maybeAddSimpleClause(
      String annotationValue,
      Function<String, ? extends Number> numberParser,
      String dslMethodName,
      String valueDescription,
      MethodSpec.Builder methodBuilder) {
    if (!annotationValue.isEmpty()) {
      if (annotationValue.startsWith(":")) {
        String bindMarkerName = annotationValue.substring(1);
        try {
          CqlIdentifier.fromCql(bindMarkerName);
        } catch (IllegalArgumentException ignored) {
          context
              .getMessager()
              .warn(
                  methodElement,
                  "Invalid "
                      + valueDescription
                      + " value: "
                      + "'%s' is not a valid placeholder, the generated query will probably fail",
                  annotationValue);
        }
        methodBuilder.addCode(
            ".$L($T.bindMarker($S))", dslMethodName, QueryBuilder.class, bindMarkerName);
      } else {
        try {
          Number ignored = numberParser.apply(annotationValue);
        } catch (NumberFormatException ignored) {
          context
              .getMessager()
              .warn(
                  methodElement,
                  "Invalid "
                      + valueDescription
                      + " value: "
                      + "'%s' is not a bind marker name and can't be parsed as a number literal "
                      + "either, the generated query will probably fail",
                  annotationValue);
        }
        methodBuilder.addCode(".$L($L)", dslMethodName, annotationValue);
      }
    }
  }

  protected void populateBuilderWithFunction(
      CodeBlock.Builder builder, VariableElement functionParam) {
    if (functionParam != null) {
      builder.addStatement(
          "boundStatementBuilder = $L.apply(boundStatementBuilder)",
          functionParam.getSimpleName().toString());
    }
  }

  protected void populateBuilderWithStatementAttributes(
      CodeBlock.Builder builder, ExecutableElement methodElement) {
    StatementAttributes statementAttributes =
        methodElement.getAnnotation(StatementAttributes.class);
    if (statementAttributes != null) {
      builder.addStatement(
          "boundStatementBuilder = populateBoundStatementWithStatementAttributes("
              + "boundStatementBuilder, $1S, $2S, $3S, $4L, $5L, $6S, $7S)",
          statementAttributes.executionProfileName(),
          statementAttributes.consistencyLevel(),
          statementAttributes.serialConsistencyLevel(),
          (statementAttributes.idempotence().length == 0)
              ? null
              : statementAttributes.idempotence()[0],
          statementAttributes.pageSize(),
          statementAttributes.timeout(),
          statementAttributes.routingKeyspace());
    }
  }

  protected VariableElement findBoundStatementFunction(ExecutableElement methodElement) {
    if (methodElement.getParameters().size() > 0) {
      int lastParamIndex = methodElement.getParameters().size() - 1;
      VariableElement lastParam = methodElement.getParameters().get(lastParamIndex);
      TypeMirror mirror = lastParam.asType();
      if (mirror.getKind() == TypeKind.DECLARED) {
        DeclaredType declaredType = (DeclaredType) mirror;
        if ((context.getClassUtils().isSame(declaredType.asElement(), Function.class)
                && context
                    .getClassUtils()
                    .isSame(declaredType.getTypeArguments().get(0), BoundStatementBuilder.class)
                && context
                    .getClassUtils()
                    .isSame(declaredType.getTypeArguments().get(1), BoundStatementBuilder.class))
            || (context.getClassUtils().isSame(declaredType.asElement(), UnaryOperator.class)
                && context
                    .getClassUtils()
                    .isSame(declaredType.getTypeArguments().get(0), BoundStatementBuilder.class))) {
          return lastParam;
        }
      }
    }
    return null;
  }

  protected boolean validateCqlNamesPresent(List<? extends VariableElement> parameters) {
    boolean valid = true;
    if (isFromClassFile()) {
      for (VariableElement parameter : parameters) {
        CqlName cqlName = parameter.getAnnotation(CqlName.class);
        if (cqlName == null) {
          context
              .getMessager()
              .error(
                  parameter,
                  "Method %s: parameter %s is declared in a compiled method "
                      + "and refers to a bind marker "
                      + "and thus must be annotated with @%s",
                  methodElement,
                  parameter.getSimpleName(),
                  CqlName.class.getSimpleName());
          valid = false;
        }
      }
    }
    return valid;
  }

  protected void warnIfCqlNamePresent(List<? extends VariableElement> parameters) {
    for (VariableElement parameter : parameters) {
      CqlName cqlName = parameter.getAnnotation(CqlName.class);
      if (cqlName != null) {
        context
            .getMessager()
            .warn(
                parameter,
                "Method %s: parameter %s does not refer to a bind marker, "
                    + "@%s annotation will be ignored",
                methodElement,
                parameter.getSimpleName(),
                CqlName.class.getSimpleName());
      }
    }
  }

  protected boolean isFromClassFile() {
    TypeElement enclosingElement = (TypeElement) methodElement.getEnclosingElement();
    try {
      Class.forName(enclosingElement.getQualifiedName().toString());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
