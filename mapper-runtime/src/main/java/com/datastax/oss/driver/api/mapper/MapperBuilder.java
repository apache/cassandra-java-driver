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
package com.datastax.oss.driver.api.mapper;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds an instance of a {@link Mapper}-annotated interface wrapping a {@link CqlSession}.
 *
 * <p>The mapper generates an implementation of this class for every such interface. It is either
 * named {@code <InterfaceName>Builder}, or what you provided in {@link Mapper#builderName()}.
 */
public abstract class MapperBuilder<MapperT> {

  protected final CqlSession session;
  protected Map<Object, Object> customState;

  protected MapperBuilder(CqlSession session) {
    this.session = session;
    this.customState = new HashMap<>();
  }

  /**
   * Stores custom state that will be propagated to {@link MapperContext#getCustomState()}.
   *
   * <p>This is intended mainly for {@link QueryProvider} methods: since provider classes are
   * instantiated directly by the generated mapper code, they have no way to access non-static state
   * from the rest of your application. This method allows you to pass that state while building the
   * mapper, and access it later at runtime.
   *
   * <p>Note that this state will be accessed concurrently, it should be thread-safe.
   */
  @NonNull
  public MapperBuilder<MapperT> withCustomState(@Nullable Object key, @Nullable Object value) {
    customState.put(key, value);
    return this;
  }

  public abstract MapperT build();
}
