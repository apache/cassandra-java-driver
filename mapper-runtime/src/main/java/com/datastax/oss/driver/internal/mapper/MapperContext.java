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
import java.util.Objects;

/**
 * A runtime context that gets passed from the mapper to DAO components to share global resources
 * and configuration.
 */
public class MapperContext {

  private final Session session;
  private final CqlIdentifier keyspaceId;
  private final CqlIdentifier tableId;

  public MapperContext(Session session) {
    this(session, null, null);
  }

  private MapperContext(Session session, CqlIdentifier keyspaceId, CqlIdentifier tableId) {
    this.session = session;
    this.keyspaceId = keyspaceId;
    this.tableId = tableId;
  }

  public MapperContext withKeyspaceAndTable(CqlIdentifier newKeyspaceId, CqlIdentifier newTableId) {
    return (Objects.equals(newKeyspaceId, this.keyspaceId)
            && Objects.equals(newTableId, this.tableId))
        ? this
        : new MapperContext(session, newKeyspaceId, newTableId);
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
}
