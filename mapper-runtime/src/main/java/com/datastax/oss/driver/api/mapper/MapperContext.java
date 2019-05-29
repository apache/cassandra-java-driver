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
package com.datastax.oss.driver.api.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A runtime context that gets passed from the mapper to DAO components to share global resources
 * and configuration.
 */
public interface MapperContext {

  @NonNull
  CqlSession getSession();

  /**
   * If this context belongs to a DAO that was built with a keyspace-parameterized mapper method,
   * the value of that parameter. Otherwise null.
   */
  @Nullable
  CqlIdentifier getKeyspaceId();

  /**
   * If this context belongs to a DAO that was built with a table-parameterized mapper method, the
   * value of that parameter. Otherwise null.
   */
  @Nullable
  CqlIdentifier getTableId();

  /**
   * Returns an instance of the given converter class.
   *
   * <p>The results of this method are cached at the mapper level. If no instance of this class
   * exists yet for this mapper, a new instance is built by looking for a public no-arg constructor.
   */
  @NonNull
  NameConverter getNameConverter(Class<? extends NameConverter> converterClass);
}
