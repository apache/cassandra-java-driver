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
import com.datastax.oss.driver.api.mapper.StatementAttributes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.MethodSpec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.Set;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

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
    if (!ttl.isEmpty()) {
      if (ttl.startsWith(":")) {
        String bindMarkerName = ttl.substring(1);
        try {
          CqlIdentifier.fromCql(bindMarkerName);
        } catch (IllegalArgumentException ignored) {
          context
              .getMessager()
              .warn(
                  methodElement,
                  "Invalid ttl value: "
                      + "'%s' is not a valid placeholder, the generated query will probably fail",
                  ttl);
        }
        methodBuilder.addCode(".usingTtl($T.bindMarker($S))", QueryBuilder.class, bindMarkerName);
      } else {
        try {
          Integer.parseInt(ttl);
        } catch (NumberFormatException ignored) {
          context
              .getMessager()
              .warn(
                  methodElement,
                  "Invalid ttl value: "
                      + "'%s' is not a bind marker name and can't be parsed as a literal integer "
                      + "either, the generated query will probably fail",
                  ttl);
        }
        methodBuilder.addCode(".usingTtl($L)", ttl);
      }
    }
  }

  protected void maybeAddTimestamp(String timestamp, MethodSpec.Builder methodBuilder) {
    if (!timestamp.isEmpty()) {
      if (timestamp.startsWith(":")) {
        String bindMarkerName = timestamp.substring(1);
        try {
          CqlIdentifier.fromCql(bindMarkerName);
        } catch (IllegalArgumentException ignored) {
          context
              .getMessager()
              .warn(
                  methodElement,
                  "Invalid timestamp value: "
                      + "'%s' is not a valid placeholder, the generated query will probably fail",
                  timestamp);
        }
        methodBuilder.addCode(
            ".usingTimestamp($T.bindMarker($S))", QueryBuilder.class, bindMarkerName);
      } else {
        try {
          Long.parseLong(timestamp);
        } catch (NumberFormatException ignored) {
          context
              .getMessager()
              .warn(
                  methodElement,
                  "Invalid timestamp value: "
                      + "'%s' is not a bind marker name and can't be parsed as a literal long "
                      + "either, the generated query will probably fail",
                  timestamp);
        }
        methodBuilder.addCode(".usingTimestamp($L)", timestamp);
      }
    }
  }

  protected VariableElement findStatementAttributesParam(ExecutableElement methodElement) {
    if (methodElement.getParameters().size() > 0) {
      int lastParamIndex = methodElement.getParameters().size() - 1;
      VariableElement lastParam = methodElement.getParameters().get(lastParamIndex);
      if (context.getClassUtils().isSame(lastParam.asType(), StatementAttributes.class)) {
        return lastParam;
      }
    }
    return null;
  }
}
