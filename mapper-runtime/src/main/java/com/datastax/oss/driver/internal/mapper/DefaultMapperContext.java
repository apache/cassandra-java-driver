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
package com.datastax.oss.driver.internal.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.MapperResultProducer;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultMapperContext implements MapperContext {

  private final CqlSession session;
  private final CqlIdentifier keyspaceId;
  private final CqlIdentifier tableId;
  private final String executionProfileName;
  private final DriverExecutionProfile executionProfile;
  private final ConcurrentMap<Class<? extends NameConverter>, NameConverter> nameConverterCache;
  private final Map<Object, Object> customState;
  private final List<MapperResultProducer> resultProducers;

  public DefaultMapperContext(
      @NonNull CqlSession session,
      @Nullable CqlIdentifier keyspaceId,
      @Nullable String executionProfileName,
      @Nullable DriverExecutionProfile executionProfile,
      @NonNull Map<Object, Object> customState,
      @NonNull List<MapperResultProducer> resultProducers) {
    this(
        session,
        keyspaceId,
        null,
        executionProfileName,
        executionProfile,
        new ConcurrentHashMap<>(),
        NullAllowingImmutableMap.copyOf(customState),
        ImmutableList.copyOf(resultProducers));
  }

  private DefaultMapperContext(
      CqlSession session,
      CqlIdentifier keyspaceId,
      CqlIdentifier tableId,
      String executionProfileName,
      DriverExecutionProfile executionProfile,
      ConcurrentMap<Class<? extends NameConverter>, NameConverter> nameConverterCache,
      Map<Object, Object> customState,
      List<MapperResultProducer> resultProducers) {
    if (executionProfile != null && executionProfileName != null) {
      // the mapper code prevents this, so we should never get here
      throw new IllegalArgumentException("Can't provide both a profile and a name");
    }
    this.session = session;
    this.keyspaceId = keyspaceId;
    this.tableId = tableId;
    this.nameConverterCache = nameConverterCache;
    this.customState = customState;
    this.executionProfileName = executionProfileName;
    this.executionProfile = executionProfile;
    this.resultProducers = resultProducers;
  }

  public DefaultMapperContext withDaoParameters(
      @Nullable CqlIdentifier newKeyspaceId,
      @Nullable CqlIdentifier newTableId,
      @Nullable String newExecutionProfileName,
      @Nullable DriverExecutionProfile newExecutionProfile) {
    return (Objects.equals(newKeyspaceId, this.keyspaceId)
            && Objects.equals(newTableId, this.tableId)
            && Objects.equals(newExecutionProfileName, this.executionProfileName)
            && Objects.equals(newExecutionProfile, this.executionProfile))
        ? this
        : new DefaultMapperContext(
            session,
            newKeyspaceId,
            newTableId,
            newExecutionProfileName,
            newExecutionProfile,
            nameConverterCache,
            customState,
            resultProducers);
  }

  @NonNull
  @Override
  public CqlSession getSession() {
    return session;
  }

  @Nullable
  @Override
  public CqlIdentifier getKeyspaceId() {
    return keyspaceId;
  }

  @Nullable
  @Override
  public CqlIdentifier getTableId() {
    return tableId;
  }

  @Nullable
  @Override
  public String getExecutionProfileName() {
    return executionProfileName;
  }

  @Nullable
  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return executionProfile;
  }

  @NonNull
  @Override
  public NameConverter getNameConverter(Class<? extends NameConverter> converterClass) {
    return nameConverterCache.computeIfAbsent(
        converterClass, DefaultMapperContext::buildNameConverter);
  }

  @NonNull
  @Override
  public Map<Object, Object> getCustomState() {
    return customState;
  }

  @NonNull
  @Override
  public MapperResultProducer getResultProducer(@NonNull GenericType<?> resultToProduce) {
    for (MapperResultProducer resultProducer : resultProducers) {
      if (resultProducer.canProduce(resultToProduce)) {
        return resultProducer;
      }
    }
    throw new IllegalArgumentException(
        String.format(
            "Found no registered %s that can produce %s",
            MapperResultProducer.class.getSimpleName(), resultToProduce));
  }

  private static NameConverter buildNameConverter(Class<? extends NameConverter> converterClass) {
    try {
      return converterClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new MapperException(
          String.format(
              "Error while building an instance of %s. "
                  + "%s implementations must have a public no-arg constructor",
              converterClass, NameConverter.class.getSimpleName()),
          e);
    }
  }
}
