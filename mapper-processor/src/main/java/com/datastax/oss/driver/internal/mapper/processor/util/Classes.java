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
package com.datastax.oss.driver.internal.mapper.processor.util;

import javax.lang.model.element.Element;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.DeclaredType;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

/**
 * Utility methods to work with existing classes in the context of an annotation processing round.
 *
 * <p>Similar to {@link Types} and {@link Elements}, but for classes that are known at compile time,
 * e.g. they belong to the JDK or one of the processor's dependencies.
 */
public class Classes {

  private final Types typeUtils;
  private final Elements elementUtils;

  public Classes(Types typeUtils, Elements elementUtils) {
    this.typeUtils = typeUtils;
    this.elementUtils = elementUtils;
  }

  /** Whether an element is the {@link TypeElement} for the given class. */
  public boolean isSame(Element element, Class<?> javaClass) {
    return element.equals(elementUtils.getTypeElement(javaClass.getName()));
  }

  /**
   * Whether a type mirror is the {@link DeclaredType} for the given class.
   *
   * <p>Note that this only intended for non-parameterized classes, e.g. {@code String}. If the type
   * mirror is {@code List<String>}, it won't match {@code List.class}.
   */
  public boolean isSame(TypeMirror mirror, Class<?> javaClass) {
    return typeUtils.isSameType(mirror, elementUtils.getTypeElement(javaClass.getName()).asType());
  }
}
