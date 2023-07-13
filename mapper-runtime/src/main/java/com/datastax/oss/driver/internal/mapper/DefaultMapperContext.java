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
package com.datastax.oss.driver.internal.mapper;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.MapperException;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class DefaultMapperContext implements MapperContext {

  private final CqlSession session;
  private final CqlIdentifier keyspaceId;
  private final CqlIdentifier tableId;
  private final ConcurrentMap<Class<? extends NameConverter>, NameConverter> nameConverterCache;
  private final Map<Object, Object> customState;

  public DefaultMapperContext(
      @NonNull CqlSession session,
      @Nullable CqlIdentifier keyspaceId,
      @NonNull Map<Object, Object> customState) {
    this(
        session,
        keyspaceId,
        null,
        new ConcurrentHashMap<>(),
        NullAllowingImmutableMap.copyOf(customState));
  }

  public DefaultMapperContext(
      @NonNull CqlSession session, @NonNull Map<Object, Object> customState) {
    this(session, null, customState);
  }

  private DefaultMapperContext(
      CqlSession session,
      CqlIdentifier keyspaceId,
      CqlIdentifier tableId,
      ConcurrentMap<Class<? extends NameConverter>, NameConverter> nameConverterCache,
      Map<Object, Object> customState) {
    this.session = session;
    this.keyspaceId = keyspaceId;
    this.tableId = tableId;
    this.nameConverterCache = nameConverterCache;
    this.customState = customState;
  }

  public DefaultMapperContext withKeyspaceAndTable(
      @Nullable CqlIdentifier newKeyspaceId, @Nullable CqlIdentifier newTableId) {
    return (Objects.equals(newKeyspaceId, this.keyspaceId)
            && Objects.equals(newTableId, this.tableId))
        ? this
        : new DefaultMapperContext(
            session, newKeyspaceId, newTableId, nameConverterCache, customState);
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
