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

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.DefaultNullSavingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import com.datastax.oss.driver.internal.mapper.processor.util.generation.BindableHandlingSharedCode;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.MethodSpec;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.type.TypeMirror;

/**
 * Exposes callbacks that allow individual method generators for a {@link Dao}-annotated class to
 * request the generation of class-level fields that they will use.
 */
public interface DaoImplementationSharedCode extends BindableHandlingSharedCode {

  /**
   * Requests the generation of a prepared statement in this DAO. It will be initialized in {@code
   * initAsync}, and then passed to the constructor which will store it in a private field.
   *
   * @param methodElement the method that will be using this statement.
   * @param simpleStatementGenerator a callback that generates code to create a {@link
   *     SimpleStatement} local variable that will be used to create the statement. The first
   *     parameter is the method to add to, and the second the name of the local variable.
   * @return the name of the generated field that will hold the statement.
   */
  String addPreparedStatement(
      ExecutableElement methodElement,
      BiConsumer<MethodSpec.Builder, String> simpleStatementGenerator);

  /**
   * Requests the instantiation of a user-provided query provider class in this DAO. It will be
   * initialized in {@code initAsync}, and then passed to the constructor which will store it in a
   * private field.
   *
   * @param methodElement the method that will be using this provider.
   * @param providerClass the provider class.
   * @param entityHelperTypes the types of the entity helpers that should be injected through the
   *     class's constructor (in addition to the {@link MapperContext}).
   * @return the name of the generated field that will hold the provider.
   * @see QueryProvider
   */
  String addQueryProvider(
      ExecutableElement methodElement, TypeMirror providerClass, List<ClassName> entityHelperTypes);

  /**
   * @return {@link DefaultNullSavingStrategy#value()} ()} if annotation is present on a given
   *     {@link Dao}. If this annotation is not present on a Dao level it Returns {@code
   *     Optional.Empty}
   */
  Optional<NullSavingStrategy> getNullSavingStrategy();
}
