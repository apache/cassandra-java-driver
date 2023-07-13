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

import com.datastax.oss.driver.api.mapper.annotations.Entity;
import java.util.Map;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
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
    if (element.getKind() != ElementKind.CLASS) {
      return null;
    }
    TypeElement typeElement = (TypeElement) element;
    if (typeElement.getAnnotation(Entity.class) == null) {
      return null;
    }
    return typeElement;
  }
}
