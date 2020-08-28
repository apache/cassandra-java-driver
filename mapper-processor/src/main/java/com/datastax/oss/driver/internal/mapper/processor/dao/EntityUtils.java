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

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
import com.datastax.oss.driver.internal.mapper.processor.entity.EntityDefinition;
import com.squareup.javapoet.TypeName;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.type.TypeVariable;

public class EntityUtils {

  /**
   * If the type of this parameter is an {@link Entity}-annotated class, return that class's
   * element, otherwise {@code null}.
   */
  public static TypeElement asEntityElement(
      VariableElement parameter, Map<Name, TypeElement> typeParameters) {
    return asEntityElement(parameter.asType(), typeParameters);
  }

  /**
   * If this mirror's first type argument is an {@link Entity}-annotated class, return that class's
   * element, otherwise {@code null}.
   *
   * <p>This method will fail if the mirror does not reference a generic type, the caller is
   * responsible to perform that check beforehand.
   */
  public static TypeElement typeArgumentAsEntityElement(
      TypeMirror mirror, Map<Name, TypeElement> typeParameters) {
    DeclaredType declaredType = (DeclaredType) mirror;
    assert !declaredType.getTypeArguments().isEmpty();
    return asEntityElement(declaredType.getTypeArguments().get(0), typeParameters);
  }

  /**
   * If this mirror is an {@link Entity}-annotated class, return that class's element, otherwise
   * {@code null}.
   */
  public static TypeElement asEntityElement(
      TypeMirror mirror, Map<Name, TypeElement> typeParameters) {
    Element element;
    if (mirror.getKind() == TypeKind.TYPEVAR) {
      // extract concrete implementation for type variable.
      TypeVariable typeVariable = ((TypeVariable) mirror);
      Name name = typeVariable.asElement().getSimpleName();
      element = typeParameters.get(name);
      if (element == null) {
        return null;
      }
    } else if (mirror.getKind() == TypeKind.DECLARED) {
      element = ((DeclaredType) mirror).asElement();
    } else {
      return null;
    }
    if (element.getKind() != ElementKind.CLASS
        // Hack to support Java 14 records without having to compile against JDK 14
        && !element.getKind().name().equals("RECORD")) {
      return null;
    }
    TypeElement typeElement = (TypeElement) element;
    if (typeElement.getAnnotation(Entity.class) == null) {
      return null;
    }
    return typeElement;
  }

  /**
   * Validates that the given parameters are valid for an {@link EntityDefinition}, meaning that
   * there are at least enough parameters provided to match the number of partition key columns and
   * that parameter types match the primary key types.
   *
   * <p>If it is determined that the parameters are not valid, false is returned and an error
   * message is emitted on the given method element.
   */
  public static boolean areParametersValid(
      TypeElement entityElement,
      EntityDefinition entityDefinition,
      List<? extends VariableElement> parameters,
      Class<? extends Annotation> annotationClass,
      ProcessorContext context,
      ExecutableElement methodElement,
      TypeElement processedType,
      String exceptionCondition) {

    if (exceptionCondition == null || exceptionCondition.isEmpty()) {
      exceptionCondition = "";
    } else {
      exceptionCondition = " that " + exceptionCondition;
    }

    List<TypeName> primaryKeyTypes =
        entityDefinition.getPrimaryKey().stream()
            .map(d -> d.getType().asTypeName())
            .collect(Collectors.toList());
    List<TypeName> partitionKeyTypes =
        entityDefinition.getPartitionKey().stream()
            .map(d -> d.getType().asTypeName())
            .collect(Collectors.toList());
    List<TypeName> parameterTypes =
        parameters.stream().map(p -> TypeName.get(p.asType())).collect(Collectors.toList());
    // if parameters are provided, we must have at least enough to match partition key.
    if (parameterTypes.size() < partitionKeyTypes.size()) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid parameter list: %s methods%s "
                  + "must at least specify partition key components "
                  + "(expected partition key of %s: %s)",
              annotationClass.getSimpleName(),
              exceptionCondition,
              entityElement.getSimpleName(),
              partitionKeyTypes);
      return false;
    }

    if (parameterTypes.size() > primaryKeyTypes.size()) {
      context
          .getMessager()
          .error(
              methodElement,
              "Invalid parameter list: %s methods%s "
                  + "must match the primary key components in the exact order "
                  + "(expected primary key of %s: %s). Too many parameters provided",
              annotationClass.getSimpleName(),
              exceptionCondition,
              entityElement.getSimpleName(),
              primaryKeyTypes);
      return false;
    }

    // validate that each parameter type matches the primary key type
    for (int parameterIndex = 0; parameterIndex < parameterTypes.size(); parameterIndex++) {
      TypeName parameterType = parameterTypes.get(parameterIndex);
      TypeName primaryKeyParameterType = primaryKeyTypes.get(parameterIndex);
      if (!parameterType.equals(primaryKeyParameterType)) {
        context
            .getMessager()
            .error(
                methodElement,
                "Invalid parameter list: %s methods%s "
                    + "must match the primary key components in the exact order "
                    + "(expected primary key of %s: %s). Mismatch at index %d: %s should be %s",
                annotationClass.getSimpleName(),
                exceptionCondition,
                entityElement.getSimpleName(),
                primaryKeyTypes,
                parameterIndex,
                parameterType,
                primaryKeyParameterType);
        return false;
      }
    }
    return true;
  }
}
