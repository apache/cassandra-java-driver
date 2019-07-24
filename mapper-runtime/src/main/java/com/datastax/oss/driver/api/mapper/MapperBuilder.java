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
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
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
  protected CqlIdentifier defaultKeyspaceId;
  protected Map<Object, Object> customState;

  protected MapperBuilder(CqlSession session) {
    this.session = session;
    this.customState = new HashMap<>();
  }

  /**
   * Specifies a default keyspace that will be used for all DAOs built with this mapper (unless they
   * specify their own keyspace).
   *
   * <p>In other words, given the following definitions:
   *
   * <pre>
   * &#64;Mapper
   * public interface InventoryMapper {
   *   &#64;DaoFactory
   *   ProductDao productDao();
   *
   *   &#64;DaoFactory
   *   ProductDao productDao(@DaoKeyspace CqlIdentifier keyspace);
   * }
   *
   * InventoryMapper mapper1 = new InventoryMapperBuilder(session)
   *     .withDefaultKeyspace(CqlIdentifier.fromCql("ks1"))
   *     .build();
   * InventoryMapper mapper2 = new InventoryMapperBuilder(session)
   *     .withDefaultKeyspace(CqlIdentifier.fromCql("ks2"))
   *     .build();
   * </pre>
   *
   * Then:
   *
   * <ul>
   *   <li>{@code mapper1.productDao()} will use keyspace {@code ks1};
   *   <li>{@code mapper2.productDao()} will use keyspace {@code ks2};
   *   <li>{@code mapper1.productDao(CqlIdentifier.fromCql("ks3"))} will use keyspace {@code ks3}.
   * </ul>
   *
   * @see DaoFactory
   */
  @NonNull
  public MapperBuilder<MapperT> withDefaultKeyspace(@Nullable CqlIdentifier keyspaceId) {
    this.defaultKeyspaceId = keyspaceId;
    return this;
  }

  /**
   * Shortcut for {@link #withDefaultKeyspace(CqlIdentifier)
   * withDefaultKeyspace(CqlIdentifier.fromCql(keyspaceName))}.
   */
  @NonNull
  public MapperBuilder<MapperT> withDefaultKeyspace(@Nullable String keyspaceName) {
    return withDefaultKeyspace(keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName));
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
