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

import com.squareup.javapoet.CodeBlock;
import java.util.Map;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;

/**
 * A "kind" of return type of a DAO method.
 *
 * <p>This represents a category of types that will be produced with the same pattern in the
 * generated code. For example, "future of entity" is a kind that encompasses {@code
 * CompletableFuture<Product>}, {@code CompletionStage<User>}, etc.
 */
public interface DaoReturnTypeKind {

  /**
   * Generates the code to execute a given statement (accessible through a local variable named
   * {@code boundStatement}), and convert the result set into this kind.
   *
   * @param methodBuilder the method to add the code to.
   * @param helperFieldName the name of the helper for entity conversions (might not get used for
   *     certain kinds, in that case it's ok to pass null).
   * @param methodElement the return type of the method (in case the result must be cast).
   * @param typeParameters
   */
  void addExecuteStatement(
      CodeBlock.Builder methodBuilder,
      String helperFieldName,
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters);

  /**
   * Generates a try-catch around the given code block, to translate unchecked exceptions into a
   * result consistent with this kind.
   *
   * <p>For example, for futures, we want to generate this:
   *
   * <pre>
   * CompletionStage&lt;Product&gt; findByIdAsync() {
   *   try {
   *     ... // innerBlock
   *   } catch (Throwable t) {
   *     return CompletableFutures.failedFuture(t);
   *   }
   * }
   * </pre>
   *
   * <p>For some kinds, it's fine to let unchecked exceptions bubble up and no try-catch is
   * necessary; in this case, this method can return {@code innerBlock} unchanged.
   */
  CodeBlock wrapWithErrorHandling(
      CodeBlock innerBlock, ExecutableElement methodElement, Map<Name, TypeElement> typeParameters);

  /** A short description suitable for error messages. */
  String getDescription();

  /**
   * Whether this return type kind requires the Reactive Streams API.
   *
   * <p>If true, the generated DAO class will inherit from {@link
   * com.datastax.dse.driver.internal.mapper.reactive.ReactiveDaoBase}, otherwise it will inherit
   * from {@link com.datastax.oss.driver.internal.mapper.DaoBase}.
   */
  boolean requiresReactive();
}
