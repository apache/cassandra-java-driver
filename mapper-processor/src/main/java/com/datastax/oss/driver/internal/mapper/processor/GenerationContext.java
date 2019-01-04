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
package com.datastax.oss.driver.internal.mapper.processor;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.squareup.javapoet.ClassName;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.processing.Filer;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.lang.model.util.Types;

/**
 * A custom context to share processor-level information with code generators. Basically just a way
 * to avoid passing a gazillion parameters everywhere.
 */
public class GenerationContext {

  private final Map<TypeMirror, EntityDefinition> entityDefinitions = new HashMap<>();
  private final Map<TypeMirror, ClassName> generatedDaos = new HashMap<>();
  private final DecoratedMessager messager;
  private final Types typeUtils;
  private final Elements elementUtils;
  private final Filer filer;
  private final String indent;

  public GenerationContext(
      DecoratedMessager messager,
      Types typeUtils,
      Elements elementUtils,
      Filer filer,
      String indent) {
    this.messager = messager;
    this.typeUtils = typeUtils;
    this.elementUtils = elementUtils;
    this.filer = filer;
    this.indent = indent;
  }

  public Map<TypeMirror, EntityDefinition> getEntityDefinitions() {
    return entityDefinitions;
  }

  /**
   * The {@link Dao}-annotated interfaces processed so far (interface name => implementation name).
   */
  public Map<TypeMirror, ClassName> getGeneratedDaos() {
    return generatedDaos;
  }

  public DecoratedMessager getMessager() {
    return messager;
  }

  public Types getTypeUtils() {
    return typeUtils;
  }

  public Elements getElementUtils() {
    return elementUtils;
  }

  public Filer getFiler() {
    return filer;
  }

  public String getIndent() {
    return indent;
  }
}
