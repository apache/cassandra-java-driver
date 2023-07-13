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
package com.datastax.oss.driver.internal.mapper.processor.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.DaoKeyspace;
import com.datastax.oss.driver.api.mapper.annotations.DaoTable;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.internal.mapper.DaoCacheKey;
import com.datastax.oss.driver.internal.mapper.processor.GeneratedNames;
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeName;
import java.util.Optional;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

/**
 * Generates the implementation of a DAO-producing method in a {@link Mapper}-annotated interface.
 */
public class MapperDaoFactoryMethodGenerator implements MethodGenerator {

  private final ExecutableElement methodElement;
  private final TypeElement processedType;
  private final MapperImplementationSharedCode enclosingClass;
  private final ProcessorContext context;

  public MapperDaoFactoryMethodGenerator(
      ExecutableElement methodElement,
      TypeElement processedType,
      MapperImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    this.methodElement = methodElement;
    this.processedType = processedType;
    this.enclosingClass = enclosingClass;
    this.context = context;
  }

  @Override
  public Optional<MethodSpec> generate() {

    // Validate the return type, which tells us what DAO to build, and whether the method should be
    // async.
    ClassName daoImplementationName = null;
    boolean isAsync = false;
    TypeMirror returnTypeMirror = methodElement.getReturnType();
    if (returnTypeMirror.getKind() == TypeKind.DECLARED) {
      DeclaredType declaredReturnType = (DeclaredType) returnTypeMirror;
      if (declaredReturnType.getTypeArguments().isEmpty()) {
        Element returnTypeElement = declaredReturnType.asElement();
        if (returnTypeElement.getAnnotation(Dao.class) != null) {
          daoImplementationName =
              GeneratedNames.daoImplementation(((TypeElement) returnTypeElement));
        }
      } else if (context.getClassUtils().isFuture(declaredReturnType)) {
        TypeMirror typeArgument = declaredReturnType.getTypeArguments().get(0);
        if (typeArgument.getKind() == TypeKind.DECLARED) {
          Element typeArgumentElement = ((DeclaredType) typeArgument).asElement();
          if (typeArgumentElement.getAnnotation(Dao.class) != null) {
            daoImplementationName =
                GeneratedNames.daoImplementation(((TypeElement) typeArgumentElement));
            isAsync = true;
          }
        }
      }
    }
    if (daoImplementationName == null) {
      context
          .getMessager()
          .error(
              methodElement,
              processedType,
              "Invalid return type: %s methods must return a %s-annotated interface, "
                  + "or future thereof",
              DaoFactory.class.getSimpleName(),
              Dao.class.getSimpleName());
      return Optional.empty();
    }

    // Validate the arguments
    String keyspaceArgumentName = null;
    String tableArgumentName = null;
    for (VariableElement parameterElement : methodElement.getParameters()) {
      if (parameterElement.getAnnotation(DaoKeyspace.class) != null) {
        keyspaceArgumentName =
            validateKeyspaceOrTableParameter(
                parameterElement, keyspaceArgumentName, DaoKeyspace.class, context);
        if (keyspaceArgumentName == null) {
          return Optional.empty();
        }
      } else if (parameterElement.getAnnotation(DaoTable.class) != null) {
        tableArgumentName =
            validateKeyspaceOrTableParameter(
                parameterElement, tableArgumentName, DaoTable.class, context);
        if (tableArgumentName == null) {
          return Optional.empty();
        }
      } else {
        context
            .getMessager()
            .error(
                methodElement,
                processedType,
                "Invalid parameter annotations: "
                    + "%s method parameters must be annotated with @%s or @%s",
                DaoFactory.class.getSimpleName(),
                DaoKeyspace.class.getSimpleName(),
                DaoTable.class.getSimpleName());
        return Optional.empty();
      }
    }
    boolean isCachedByKeyspaceAndTable =
        (keyspaceArgumentName != null || tableArgumentName != null);

    TypeName returnTypeName = ClassName.get(methodElement.getReturnType());
    String suggestedFieldName = methodElement.getSimpleName() + "Cache";
    String fieldName =
        isCachedByKeyspaceAndTable
            ? enclosingClass.addDaoMapField(suggestedFieldName, returnTypeName)
            : enclosingClass.addDaoSimpleField(
                suggestedFieldName, returnTypeName, daoImplementationName, isAsync);

    MethodSpec.Builder overridingMethodBuilder = GeneratedCodePatterns.override(methodElement);

    if (isCachedByKeyspaceAndTable) {
      // DaoCacheKey key = new DaoCacheKey(x, y)
      // where x, y is either the name of the parameter or "(CqlIdentifier)null"
      overridingMethodBuilder.addCode("$1T key = new $1T(", DaoCacheKey.class);
      if (keyspaceArgumentName == null) {
        overridingMethodBuilder.addCode("($T)null", CqlIdentifier.class);
      } else {
        overridingMethodBuilder.addCode("$L", keyspaceArgumentName);
      }
      overridingMethodBuilder.addCode(", ");
      if (tableArgumentName == null) {
        overridingMethodBuilder.addCode("($T)null", CqlIdentifier.class);
      } else {
        overridingMethodBuilder.addCode("$L", tableArgumentName);
      }
      overridingMethodBuilder
          .addCode(");\n")
          .addStatement(
              "return $L.computeIfAbsent(key, "
                  + "k -> $T.$L(context.withKeyspaceAndTable(k.getKeyspaceId(), k.getTableId())))",
              fieldName,
              daoImplementationName,
              isAsync ? "initAsync" : "init");
    } else {
      overridingMethodBuilder.addStatement("return $L.get()", fieldName);
    }
    return Optional.of(overridingMethodBuilder.build());
  }

  private String validateKeyspaceOrTableParameter(
      VariableElement candidate, String previous, Class<?> annotation, ProcessorContext context) {
    if (previous != null) {
      context
          .getMessager()
          .error(
              candidate,
              processedType,
              "Invalid parameter annotations: "
                  + "only one %s method parameter can be annotated with @%s",
              DaoFactory.class.getSimpleName(),
              annotation.getSimpleName());
      return null;
    }
    TypeMirror type = candidate.asType();
    if (!context.getClassUtils().isSame(type, String.class)
        && !context.getClassUtils().isSame(type, CqlIdentifier.class)) {
      context
          .getMessager()
          .error(
              candidate,
              processedType,
              "Invalid parameter type: @%s-annotated parameter of %s methods must be of type %s or %s",
              annotation.getSimpleName(),
              DaoFactory.class.getSimpleName(),
              String.class.getSimpleName(),
              CqlIdentifier.class.getSimpleName());
      return null;
    }
    return candidate.getSimpleName().toString();
  }
}
