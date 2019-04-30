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
import com.datastax.oss.driver.internal.mapper.processor.MethodGenerator;
import com.datastax.oss.driver.internal.mapper.processor.ProcessorContext;
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
}
