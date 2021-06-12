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
import com.datastax.oss.driver.api.core.data.SettableByName;
import com.datastax.oss.driver.api.mapper.annotations.SetEntity;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.GeneratedCodePatterns;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import java.util.Map;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

public class DaoSetEntityMethodGenerator extends DaoMethodGenerator {

  private final NullSavingStrategyValidation nullSavingStrategyValidation;
  private final boolean lenient;

  public DaoSetEntityMethodGenerator(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass,
      ProcessorContext context) {
    super(methodElement, typeParameters, processedType, enclosingClass, context);
    nullSavingStrategyValidation = new NullSavingStrategyValidation(context);
    lenient = methodElement.getAnnotation(SetEntity.class).lenient();
  }

  @Override
  public Optional<MethodSpec> generate() {

    String entityParameterName = null;
    TypeElement entityElement = null;
    String targetParameterName = null;

    // Validate the parameters: one is an annotated entity, and the other a subtype of
    // SettableByName.
    if (methodElement.getParameters().size() != 2) {
      context
          .getMessager()
          .error(
              methodElement,
              "Wrong number of parameters: %s methods must have two",
              SetEntity.class.getSimpleName());
      return Optional.empty();
    }
    TypeMirror targetParameterType = null;
    for (VariableElement parameterElement : methodElement.getParameters()) {
      TypeMirror parameterType = parameterElement.asType();
      if (context.getClassUtils().implementsSettableByName(parameterType)) {
        targetParameterName = parameterElement.getSimpleName().toString();
        targetParameterType = parameterElement.asType();
      } else if (parameterType.getKind() == TypeKind.DECLARED
          || parameterType.getKind() == TypeKind.TYPEVAR) {
        TypeElement parameterTypeElement =
            EntityUtils.asEntityElement(parameterType, typeParameters);
        if (parameterTypeElement != null) {
          entityParameterName = parameterElement.getSimpleName().toString();
          entityElement = parameterTypeElement;
        }
      }
    }
    if (entityParameterName == null || targetParameterName == null) {
      context
          .getMessager()
          .error(
              methodElement,
              "Wrong parameter types: %s methods must take a %s "
                  + "and an annotated entity (in any order)",
              SetEntity.class.getSimpleName(),
              SettableByName.class.getSimpleName());
      return Optional.empty();
    }

    // Validate the return type: either void or the same SettableByName as the parameter
    TypeMirror returnType = methodElement.getReturnType();
    boolean isVoid = returnType.getKind() == TypeKind.VOID;
    if (isVoid) {
      if (context.getClassUtils().isSame(targetParameterType, BoundStatement.class)) {
        context
            .getMessager()
            .warn(
                methodElement,
                "BoundStatement is immutable, "
                    + "this method will not modify '%s' in place. "
                    + "It should probably return BoundStatement rather than void",
                targetParameterName);
      }
    } else if (!context.getTypeUtils().isSameType(returnType, targetParameterType)) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid return type: %s methods must either be void, or return the same "
                  + "type as their settable parameter (in this case, %s to match '%s')",
              SetEntity.class.getSimpleName(),
              targetParameterType,
              targetParameterName);
      return Optional.empty();
    }

    // Generate the method:
    String helperFieldName = enclosingClass.addEntityHelperField(ClassName.get(entityElement));

    NullSavingStrategy nullSavingStrategy =
        nullSavingStrategyValidation.getNullSavingStrategy(
            SetEntity.class, SetEntity::nullSavingStrategy, methodElement, enclosingClass);

    // Forward to the base injector in the helper:
    return Optional.of(
        GeneratedCodePatterns.override(methodElement, typeParameters)
            .addStatement(
                "$1L$2L.set($3L, $4L, $5T.$6L, $7L)",
                isVoid ? "" : "return ",
                helperFieldName,
                entityParameterName,
                targetParameterName,
                NullSavingStrategy.class,
                nullSavingStrategy,
                lenient)
            .build());
  }
}
