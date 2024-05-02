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
package com.datastax.oss.driver.internal.mapper.processor;

import com.datastax.oss.driver.api.mapper.annotations.Dao;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoImplementationSharedCode;
import com.datastax.oss.driver.internal.mapper.processor.dao.DaoReturnTypeParser;
import com.datastax.oss.driver.internal.mapper.processor.mapper.MapperImplementationSharedCode;
import java.util.Map;
import java.util.Optional;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Name;
import javax.lang.model.element.TypeElement;

public interface CodeGeneratorFactory {

  /** The "helper" class associated to an {@link Entity}-annotated class. */
  CodeGenerator newEntity(TypeElement classElement);

  /**
   * All the types derived from a {@link Mapper}-annotated interface.
   *
   * <p>By default, this calls {@link #newMapperImplementation(TypeElement)} and {@link
   * #newMapperBuilder(TypeElement)}.
   */
  CodeGenerator newMapper(TypeElement interfaceElement);

  /**
   * The implementation of a {@link Mapper}-annotated interface.
   *
   * <p>The default code factory calls {@link #newMapperImplementationMethod} for each non-static,
   * non-default method, but this is not a hard requirement.
   */
  CodeGenerator newMapperImplementation(TypeElement interfaceElement);

  /**
   * A method in the implementation of a {@link Mapper}-annotated interface.
   *
   * @return empty if the processor can't determine what to generate. This will translate as a
   *     compile-time error.
   * @see #newMapperImplementation(TypeElement)
   */
  Optional<MethodGenerator> newMapperImplementationMethod(
      ExecutableElement methodElement,
      TypeElement processedType,
      MapperImplementationSharedCode enclosingClass);

  /** The builder associated to a {@link Mapper}-annotated interface. */
  CodeGenerator newMapperBuilder(TypeElement interfaceElement);

  /**
   * The implementation of a {@link Dao}-annotated interface.
   *
   * <p>The default code factory calls {@link #newDaoImplementationMethod} for each non-static,
   * non-default method, but this is not a hard requirement.
   */
  CodeGenerator newDaoImplementation(TypeElement interfaceElement);

  /**
   * A method in the implementation of a {@link Dao}-annotated interface.
   *
   * @return empty if the processor can't determine what to generate. This will translate as a
   *     compile-time error.
   * @see #newDaoImplementation(TypeElement)
   */
  Optional<MethodGenerator> newDaoImplementationMethod(
      ExecutableElement methodElement,
      Map<Name, TypeElement> typeParameters,
      TypeElement processedType,
      DaoImplementationSharedCode enclosingClass);

  DaoReturnTypeParser getDaoReturnTypeParser();
}
