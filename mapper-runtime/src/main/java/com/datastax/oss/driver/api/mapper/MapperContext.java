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
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.driver.api.mapper.result.MapperResultProducer;
import com.datastax.oss.driver.api.mapper.result.MapperResultProducerService;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;

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
   * If this context belongs to a DAO that was built with a method that takes an execution profile
   * name as parameter, the value of that parameter. Otherwise null.
   *
   * <p>Note that this is mutually exclusive with {@link #getExecutionProfile()}: at most one of the
   * two methods returns a non-null value (or both return null if no profile was provided).
   */
  @Nullable
  String getExecutionProfileName();

  /**
   * If this context belongs to a DAO that was built with a method that takes an execution profile
   * as parameter, the value of that parameter. Otherwise null.
   *
   * <p>Note that this is mutually exclusive with {@link #getExecutionProfileName()}: at most one of
   * the two methods returns a non-null value (or both return null if no profile was provided).
   */
  @Nullable
  DriverExecutionProfile getExecutionProfile();

  /**
   * Returns an instance of the given converter class.
   *
   * <p>The results of this method are cached at the mapper level. If no instance of this class
   * exists yet for this mapper, a new instance is built by looking for a public no-arg constructor.
   */
  @NonNull
  NameConverter getNameConverter(Class<? extends NameConverter> converterClass);

  /**
   * Retrieves any custom state that was set while building the mapper with {@link
   * MapperBuilder#withCustomState(Object, Object)}.
   *
   * <p>The returned map is immutable. If no state was set on the builder, it will be empty.
   */
  @NonNull
  Map<Object, Object> getCustomState();

  /**
   * Returns a component that will execute a statement and convert it into a custom result of the
   * given type.
   *
   * <p>These components must be registered through the Java Service Provider Interface mechanism,
   * see {@link MapperResultProducerService}.
   *
   * <p>The results of this method are cached at the JVM level.
   *
   * @throws IllegalArgumentException if no producer was registered for this type.
   */
  @NonNull
  MapperResultProducer getResultProducer(@NonNull GenericType<?> resultToProduce);
}
