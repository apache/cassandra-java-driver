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
import java.util.Objects;

public class DaoCacheKey {

  private final CqlIdentifier keyspaceId;
  private final CqlIdentifier tableId;

  public DaoCacheKey(CqlIdentifier keyspaceId, CqlIdentifier tableId) {
    this.keyspaceId = keyspaceId;
    this.tableId = tableId;
  }

  public DaoCacheKey(CqlIdentifier keyspaceId, String tableName) {
    this(keyspaceId, toId(tableName));
  }

  public DaoCacheKey(String keyspaceName, CqlIdentifier tableId) {
    this(toId(keyspaceName), tableId);
  }

  public DaoCacheKey(String keyspaceName, String tableName) {
    this(toId(keyspaceName), toId(tableName));
  }

  private static CqlIdentifier toId(String name) {
    return name == null ? null : CqlIdentifier.fromCql(name);
  }

  public CqlIdentifier getKeyspaceId() {
    return keyspaceId;
  }

  public CqlIdentifier getTableId() {
    return tableId;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DaoCacheKey) {
      DaoCacheKey that = (DaoCacheKey) other;
      return Objects.equals(this.keyspaceId, that.keyspaceId)
          && Objects.equals(this.tableId, that.tableId);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspaceId, tableId);
  }
}
