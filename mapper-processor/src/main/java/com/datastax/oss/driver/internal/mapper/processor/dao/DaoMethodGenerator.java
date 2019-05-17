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
import com.datastax.oss.driver.api.core.MappedAsyncPagingIterable;
import com.datastax.oss.driver.api.core.PagingIterable;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.squareup.javapoet.MethodSpec;
import java.util.Optional;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public abstract class DaoMethodGenerator implements MethodGenerator {

  protected final ExecutableElement methodElement;
  protected final DaoImplementationSharedCode enclosingClass;
  protected final ProcessorContext context;

  public DaoMethodGenerator(
      ExecutableElement methodElement,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    this.methodElement = methodElement;
    this.enclosingClass = enclosingClass;
    this.context = context;
  }

  /**
   * If the type of this parameter is an {@link Entity}-annotated class, return that class's
   * element, otherwise {@code null}.
   */
  protected TypeElement asEntityElement(VariableElement parameter) {
    return asEntityElement(parameter.asType());
  }

  /**
   * If this mirror's first type argument is an {@link Entity}-annotated class, return that class's
   * element, otherwise {@code null}.
   *
   * <p>This method will fail if the mirror does not reference a generic type, the caller is
   * responsible to perform that check beforehand.
   */
  protected TypeElement typeArgumentAsEntityElement(TypeMirror mirror) {
    DeclaredType declaredType = (DeclaredType) mirror;
    assert !declaredType.getTypeArguments().isEmpty();
    return asEntityElement(declaredType.getTypeArguments().get(0));
  }

  protected TypeElement asEntityElement(TypeMirror mirror) {
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

  protected ReturnType parseReturnType(TypeMirror returnTypeMirror) {
    TypeElement entityElement;
    if (returnTypeMirror.getKind() == TypeKind.VOID) {
      return new ReturnType(ReturnTypeKind.VOID);
    } else if (returnTypeMirror.getKind() == TypeKind.BOOLEAN) {
      return new ReturnType(ReturnTypeKind.BOOLEAN);
    } else if (returnTypeMirror.getKind() == TypeKind.LONG) {
      return new ReturnType(ReturnTypeKind.LONG);
    } else if ((entityElement = asEntityElement(returnTypeMirror)) != null) {
      return new ReturnType(ReturnTypeKind.ENTITY, entityElement);
    } else if (returnTypeMirror.getKind() == TypeKind.DECLARED) {
      DeclaredType declaredReturnType = (DeclaredType) returnTypeMirror;
      Element returnElement = declaredReturnType.asElement();
      if (context.getClassUtils().isSame(declaredReturnType, Boolean.class)) {
        return new ReturnType(ReturnTypeKind.BOOLEAN);
      } else if (context.getClassUtils().isSame(declaredReturnType, Long.class)) {
        return new ReturnType(ReturnTypeKind.LONG);
      } else if (context.getClassUtils().isSame(declaredReturnType, Row.class)) {
        return new ReturnType(ReturnTypeKind.ROW);
      } else if (context.getClassUtils().isSame(declaredReturnType, ResultSet.class)) {
        return new ReturnType(ReturnTypeKind.RESULT_SET);
      } else if (context.getClassUtils().isSame(returnElement, PagingIterable.class)
          && (entityElement = typeArgumentAsEntityElement(returnTypeMirror)) != null) {
        return new ReturnType(ReturnTypeKind.PAGING_ITERABLE, entityElement);
      } else if (context.getClassUtils().isSame(returnElement, Optional.class)
          && (entityElement = typeArgumentAsEntityElement(returnTypeMirror)) != null) {
        return new ReturnType(ReturnTypeKind.OPTIONAL_ENTITY, entityElement);
      } else if (context.getClassUtils().isFuture(declaredReturnType)) {
        TypeMirror typeArgumentMirror = declaredReturnType.getTypeArguments().get(0);
        if (context.getClassUtils().isSame(typeArgumentMirror, Void.class)) {
          return new ReturnType(ReturnTypeKind.FUTURE_OF_VOID);
        } else if (context.getClassUtils().isSame(typeArgumentMirror, Boolean.class)) {
          return new ReturnType(ReturnTypeKind.FUTURE_OF_BOOLEAN);
        } else if (context.getClassUtils().isSame(typeArgumentMirror, Long.class)) {
          return new ReturnType(ReturnTypeKind.FUTURE_OF_LONG);
        } else if (context.getClassUtils().isSame(typeArgumentMirror, Row.class)) {
          return new ReturnType(ReturnTypeKind.FUTURE_OF_ROW);
        } else if (context.getClassUtils().isSame(typeArgumentMirror, AsyncResultSet.class)) {
          return new ReturnType(ReturnTypeKind.FUTURE_OF_ASYNC_RESULT_SET);
        } else if ((entityElement = asEntityElement(typeArgumentMirror)) != null) {
          return new ReturnType(ReturnTypeKind.FUTURE_OF_ENTITY, entityElement);
        } else if (typeArgumentMirror.getKind() == TypeKind.DECLARED) {
          Element typeArgumentElement = ((DeclaredType) typeArgumentMirror).asElement();
          if (context.getClassUtils().isSame(typeArgumentElement, Optional.class)
              && (entityElement = typeArgumentAsEntityElement(typeArgumentMirror)) != null) {
            return new ReturnType(ReturnTypeKind.FUTURE_OF_OPTIONAL_ENTITY, entityElement);
          } else if (context
                  .getClassUtils()
                  .isSame(typeArgumentElement, MappedAsyncPagingIterable.class)
              && (entityElement = typeArgumentAsEntityElement(typeArgumentMirror)) != null) {
            return new ReturnType(ReturnTypeKind.FUTURE_OF_ASYNC_PAGING_ITERABLE, entityElement);
          }
        }
      }
    }
    return new ReturnType(ReturnTypeKind.UNSUPPORTED);
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

  protected static class ReturnType {
    final ReturnTypeKind kind;
    final TypeElement entityElement;

    ReturnType(ReturnTypeKind kind, TypeElement entityElement) {
      this.kind = kind;
      this.entityElement = entityElement;
    }

    ReturnType(ReturnTypeKind kind) {
      this(kind, null);
    }
  }
}
