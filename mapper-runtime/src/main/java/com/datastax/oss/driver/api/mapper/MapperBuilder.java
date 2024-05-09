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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.mapper.annotations.DaoFactory;
import com.datastax.oss.driver.api.mapper.annotations.Mapper;
import com.datastax.oss.driver.api.mapper.annotations.NamingStrategy;
import com.datastax.oss.driver.api.mapper.annotations.QueryProvider;
import com.datastax.oss.driver.api.mapper.annotations.SchemaHint;
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

  public static final String SCHEMA_VALIDATION_ENABLED_SETTING =
      "datastax.mapper.schemaValidationEnabled";
  protected final CqlSession session;
  protected CqlIdentifier defaultKeyspaceId;
  protected Map<Object, Object> customState;
  protected String defaultExecutionProfileName;
  protected DriverExecutionProfile defaultExecutionProfile;

  protected MapperBuilder(CqlSession session) {
    this.session = session;
    this.customState = new HashMap<>();
    // schema validation is enabled by default
    customState.put(SCHEMA_VALIDATION_ENABLED_SETTING, true);
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
   * Specifies a default execution profile name that will be used for all DAOs built with this
   * mapper (unless they specify their own execution profile).
   *
   * <p>This works the same way as the {@linkplain #withDefaultKeyspace(CqlIdentifier) default
   * keyspace}.
   *
   * <p>Note that if you had already set a profile with #withDefaultExecutionProfile, this method
   * erases it.
   *
   * @see DaoFactory
   */
  @NonNull
  public MapperBuilder<MapperT> withDefaultExecutionProfileName(
      @Nullable String executionProfileName) {
    this.defaultExecutionProfileName = executionProfileName;
    if (executionProfileName != null) {
      this.defaultExecutionProfile = null;
    }
    return this;
  }

  /**
   * Specifies a default execution profile name that will be used for all DAOs built with this
   * mapper (unless they specify their own execution profile).
   *
   * <p>This works the same way as the {@linkplain #withDefaultKeyspace(CqlIdentifier) default
   * keyspace}.
   *
   * <p>Note that if you had already set a profile name with #withDefaultExecutionProfileName, this
   * method erases it.
   *
   * @see DaoFactory
   */
  @NonNull
  public MapperBuilder<MapperT> withDefaultExecutionProfile(
      @Nullable DriverExecutionProfile executionProfile) {
    this.defaultExecutionProfile = executionProfile;
    if (executionProfile != null) {
      this.defaultExecutionProfileName = null;
    }
    return this;
  }

  /**
   * Whether to validate mapped entities against the database schema.
   *
   * <p>If this is enabled, then every time a new DAO gets created, for each entity referenced in
   * the DAO, the mapper will check that there is a corresponding table or UDT.
   *
   * <ul>
   *   <li>for each entity field, the database table or UDT must contain a column with the
   *       corresponding name (according to the {@link NamingStrategy}).
   *   <li>the types must be compatible, according to the {@link CodecRegistry} used by the session.
   *   <li>additionally, if the target element is a table, the primary key must be properly
   *       annotated in the entity.
   * </ul>
   *
   * If any of those steps fails, an {@link IllegalArgumentException} is thrown.
   *
   * <p>Schema validation is enabled by default; it adds a small startup overhead, so once your
   * application is stable you may want to disable it.
   *
   * @see SchemaHint
   */
  public MapperBuilder<MapperT> withSchemaValidationEnabled(boolean enableSchemaValidation) {
    customState.put(SCHEMA_VALIDATION_ENABLED_SETTING, enableSchemaValidation);
    return this;
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
