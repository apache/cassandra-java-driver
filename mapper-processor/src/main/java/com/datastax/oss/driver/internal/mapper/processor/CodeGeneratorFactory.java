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
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.internal.mapper.processor.util.NameIndex;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;

public interface CodeGeneratorFactory {

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
   * <p>By default, this calls {@link #newDaoMethodFactory(ExecutableElement, NameIndex)} for each
   * DAO factory method.
   */
  CodeGenerator newMapperImplementation(TypeElement interfaceElement);

  /** A method of a {@link Mapper}-annotated interface that produces DAO instances. */
  PartialClassGenerator newDaoMethodFactory(ExecutableElement methodElement, NameIndex nameIndex);

  /** The builder associated to a {@link Mapper}-annotated interface. */
  CodeGenerator newMapperBuilder(TypeElement interfaceElement);

  /** The implementation of a {@link Dao}-annotated interface. */
  CodeGenerator newDao(TypeElement interfaceElement);
}
