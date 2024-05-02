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
package com.datastax.oss.driver.internal.mapper.processor.util;

import com.datastax.oss.driver.api.core.data.GettableByName;
import com.datastax.oss.driver.api.core.data.SettableByName;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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

  private final TypeMirror settableByNameType;
  private final TypeMirror gettableByNameType;
  private final TypeElement listElement;
  private final TypeElement setElement;
  private final TypeElement mapElement;
  private final TypeElement completionStageElement;
  private final TypeElement completableFutureElement;

  public Classes(Types typeUtils, Elements elementUtils) {
    this.typeUtils = typeUtils;
    this.elementUtils = elementUtils;

    this.settableByNameType =
        typeUtils.erasure(elementUtils.getTypeElement(SettableByName.class.getName()).asType());
    this.gettableByNameType =
        typeUtils.erasure(elementUtils.getTypeElement(GettableByName.class.getName()).asType());
    this.listElement = elementUtils.getTypeElement(List.class.getCanonicalName());
    this.setElement = elementUtils.getTypeElement(Set.class.getCanonicalName());
    this.mapElement = elementUtils.getTypeElement(Map.class.getCanonicalName());
    this.completionStageElement =
        elementUtils.getTypeElement(CompletionStage.class.getCanonicalName());
    this.completableFutureElement =
        elementUtils.getTypeElement(CompletableFuture.class.getCanonicalName());
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

  public boolean implementsSettableByName(TypeMirror mirror) {
    return typeUtils.isAssignable(mirror, settableByNameType);
  }

  public boolean implementsGettableByName(TypeMirror mirror) {
    return typeUtils.isAssignable(mirror, gettableByNameType);
  }

  /** Whether a type mirror is a parameterized {@code java.util.List}. */
  public boolean isList(DeclaredType declaredType) {
    return declaredType.asElement().equals(listElement);
  }

  /** Whether a type mirror is a parameterized {@code java.util.Set}. */
  public boolean isSet(DeclaredType declaredType) {
    return declaredType.asElement().equals(setElement);
  }

  /** Whether a type mirror is a parameterized {@code java.util.Map}. */
  public boolean isMap(DeclaredType declaredType) {
    return declaredType.asElement().equals(mapElement);
  }

  /**
   * Whether a type mirror is a parameterized Java 8 future ({@code CompletionStage or
   * CompletableFuture}.
   */
  public boolean isFuture(DeclaredType declaredType) {
    return declaredType.asElement().equals(completionStageElement)
        || declaredType.asElement().equals(completableFutureElement);
  }
}
