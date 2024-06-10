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

import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.UNSUPPORTED;
import static com.datastax.oss.driver.internal.mapper.processor.dao.DefaultDaoReturnTypeKind.values;

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.AnnotationValue;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoQueryProviderMethodGenerator extends DaoMethodGenerator {

  public DaoQueryProviderMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
  }

  protected Set<DaoReturnTypeKind> getSupportedReturnTypes() {
    ImmutableSet.Builder<DaoReturnTypeKind> builder = ImmutableSet.builder();
    for (DefaultDaoReturnTypeKind value : values()) {
      if (value != UNSUPPORTED) {
        builder.add(value);
      }
    }
    return builder.build();
  }

  @Override
  public boolean requiresReactive() {
    // Validate the return type:
    DaoReturnType returnType =
        parseAndValidateReturnType(getSupportedReturnTypes(), QueryProvider.class.getSimpleName());
    if (returnType == null) {
      return false;
    }
    return returnType.requiresReactive();
  }

  @Override
  public Optional<MethodSpec> generate() {

    // No parameter or return type validation, they're completely free-form as long as the provider
    // method matches.

    MethodSpec.Builder methodBuilder =
        GeneratedCodePatterns.override(methodElement, typeParameters);

    // Request the instantiation of the provider during DAO initialization
    List<ClassName> entityHelperTypes = getEntityHelperTypes();
    String providerName =
        enclosingClass.addQueryProvider(methodElement, getProviderClass(), entityHelperTypes);

    // Delegate to the provider's method
    String providerMethod = methodElement.getAnnotation(QueryProvider.class).providerMethod();
    if (providerMethod.isEmpty()) {
      providerMethod = methodElement.getSimpleName().toString();
    }

    methodBuilder.addCode("$[");
    if (methodElement.getReturnType().getKind() != TypeKind.VOID) {
      methodBuilder.addCode("return ");
    }
    methodBuilder.addCode("$L.$L(", providerName, providerMethod);
    boolean first = true;
    for (VariableElement parameter : methodElement.getParameters()) {
      if (first) {
        first = false;
      } else {
        methodBuilder.addCode(", ");
      }
      methodBuilder.addCode("$L", parameter.getSimpleName().toString());
    }
    methodBuilder.addCode(");$]\n");

    return Optional.of(methodBuilder.build());
  }

  private TypeMirror getProviderClass() {
    AnnotationMirror annotationMirror = getQueryProviderAnnotationMirror();
    for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
        annotationMirror.getElementValues().entrySet()) {
      if (entry.getKey().getSimpleName().contentEquals("providerClass")) {
        return ((TypeMirror) entry.getValue().getValue());
      }
    }
    // providerClass is mandatory on the annotation, so if we get here the user will already have to
    // deal with a compile error.
    // But return something so that the processor doesn't crash downstream.
    return context.getTypeUtils().getPrimitiveType(TypeKind.INT);
  }

  @SuppressWarnings("MixedMutabilityReturnType")
  private List<ClassName> getEntityHelperTypes() {
    AnnotationMirror annotationMirror = getQueryProviderAnnotationMirror();
    for (Map.Entry<? extends ExecutableElement, ? extends AnnotationValue> entry :
        annotationMirror.getElementValues().entrySet()) {
      if (entry.getKey().getSimpleName().contentEquals("entityHelpers")) {
        @SuppressWarnings("unchecked")
        List<? extends AnnotationValue> values = (List) entry.getValue().getValue();
        List<ClassName> result = new ArrayList<>(values.size());
        for (AnnotationValue value : values) {
          TypeMirror entityMirror = (TypeMirror) value.getValue();
          TypeElement entityElement = EntityUtils.asEntityElement(entityMirror, typeParameters);
          if (entityElement == null) {
            context
                .getMessager()
                .error(
                    methodElement,
                    "Invalid annotation configuration: the elements in %s.entityHelpers "
                        + "must be %s-annotated classes (offending element: %s)",
                    QueryProvider.class.getSimpleName(),
                    Entity.class.getSimpleName(),
                    entityMirror);
            // No need to keep going, compilation will fail anyway
            return Collections.emptyList();
          } else {
            result.add(ClassName.get(entityElement));
          }
        }
        return result;
      }
    }
    return Collections.emptyList();
  }

  private AnnotationMirror getQueryProviderAnnotationMirror() {
    for (AnnotationMirror candidate : methodElement.getAnnotationMirrors()) {
      if (context.getClassUtils().isSame(candidate.getAnnotationType(), QueryProvider.class)) {
        return candidate;
      }
    }
    // We'll never get here because we wouldn't be in this class if the method didn't have the
    // annotation
    throw new AssertionError("Expected to find QueryProvider annotation");
  }
}
