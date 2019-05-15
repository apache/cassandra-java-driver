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
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.mapper.entity.naming.NameConverter;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A runtime context that gets passed from the mapper to DAO components to share global resources
 * and configuration.
 */
public class MapperContext {

  private final Session session;
  private final CqlIdentifier keyspaceId;
  private final CqlIdentifier tableId;
  private final ConcurrentMap<Class<? extends NameConverter>, NameConverter> nameConverterCache;

  public MapperContext(Session session) {
    this(session, null, null, new ConcurrentHashMap<>());
  }

  private MapperContext(
      Session session,
      CqlIdentifier keyspaceId,
      CqlIdentifier tableId,
      ConcurrentMap<Class<? extends NameConverter>, NameConverter> nameConverterCache) {
    this.session = session;
    this.keyspaceId = keyspaceId;
    this.tableId = tableId;
    this.nameConverterCache = nameConverterCache;
  }

  public MapperContext withKeyspaceAndTable(CqlIdentifier newKeyspaceId, CqlIdentifier newTableId) {
    return (Objects.equals(newKeyspaceId, this.keyspaceId)
            && Objects.equals(newTableId, this.tableId))
        ? this
        : new MapperContext(session, newKeyspaceId, newTableId, nameConverterCache);
  }

  public Session getSession() {
    return session;
  }

  /**
   * If this context belongs to a DAO that was built with a keyspace-parameterized mapper method,
   * the value of that parameter. Otherwise null.
   */
  public CqlIdentifier getKeyspaceId() {
    return keyspaceId;
  }

  /**
   * If this context belongs to a DAO that was built with a table-parameterized mapper method, the
   * value of that parameter. Otherwise null.
   */
  public CqlIdentifier getTableId() {
    return tableId;
  }

  /**
   * Returns an instance of the given converter class.
   *
   * <p>The results of this method are cached at the mapper level. If no instance of this class
   * exists yet for this mapper, a new instance is built by looking for a public no-arg constructor.
   */
  public NameConverter getNameConverter(Class<? extends NameConverter> converterClass) {
    return nameConverterCache.computeIfAbsent(converterClass, MapperContext::buildNameConverter);
  }

  private static NameConverter buildNameConverter(Class<? extends NameConverter> converterClass) {
    try {
      return converterClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | InvocationTargetException e) {
      throw new IllegalStateException(
          String.format(
              "Error while building an instance of %s. "
                  + "%s implementations must have a public no-arg constructor",
              converterClass, NameConverter.class.getSimpleName()),
          e);
    }
  }
}
