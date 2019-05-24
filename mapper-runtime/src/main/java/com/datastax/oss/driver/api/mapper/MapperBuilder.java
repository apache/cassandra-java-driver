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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;

/**
 * Builds an instance of a {@link Mapper}-annotated interface wrapping a {@link CqlSession}.
 *
 * <p>The mapper generates an implementation of this class for every such interface. It is either
 * named {@code <InterfaceName>Builder}, or what you provided in {@link Mapper#builderName()}.
 */
public abstract class MapperBuilder<MapperT> {

  protected final CqlSession session;

  protected MapperBuilder(CqlSession session) {
    this.session = session;
  }

  public abstract MapperT build();
}
