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
package com.datastax.oss.driver.api.querybuilder.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public interface CreateIndexOnTable {

  /** Specifies the column to create the index on. */
  @NonNull
  default CreateIndex andColumn(@NonNull CqlIdentifier columnName) {
    return andColumn(columnName, null);
  }

  /** Shortcut for {@link #andColumn(CqlIdentifier) andColumn(CqlIdentifier.fromCql(columnName)}. */
  @NonNull
  default CreateIndex andColumn(@NonNull String columnName) {
    return andColumn(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Specifies to create the index on the given columns' keys, this must be done against a <code>map
   * </code> column.
   */
  @NonNull
  default CreateIndex andColumnKeys(@NonNull CqlIdentifier columnName) {
    return andColumn(columnName, "KEYS");
  }

  /**
   * Shortcut for {@link #andColumnKeys(CqlIdentifier)
   * andColumnKeys(CqlIdentifier.fromCql(columnName)}.
   */
  @NonNull
  default CreateIndex andColumnKeys(@NonNull String columnName) {
    return andColumnKeys(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Specifies to create the index on the given columns' values, this must be done against a <code>
   * map</code> column.
   */
  @NonNull
  default CreateIndex andColumnValues(@NonNull CqlIdentifier columnName) {
    return andColumn(columnName, "VALUES");
  }

  /**
   * Shortcut for {@link #andColumnValues(CqlIdentifier)
   * andColumnValues(CqlIdentifier.fromCql(columnName)}.
   */
  @NonNull
  default CreateIndex andColumnValues(@NonNull String columnName) {
    return andColumnValues(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Specifies to create the index on the given columns' entries (key-value pairs), this must be
   * done against a <code>map</code> column.
   */
  @NonNull
  default CreateIndex andColumnEntries(@NonNull CqlIdentifier columnName) {
    return andColumn(columnName, "ENTRIES");
  }

  /**
   * Shortcut for {@link #andColumnEntries(CqlIdentifier)
   * andColumnEntries(CqlIdentifier.fromCql(columnName)}.
   */
  @NonNull
  default CreateIndex andColumnEntries(@NonNull String columnName) {
    return andColumnEntries(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Specifies to create the index on the given columns' entire value, this must be done against a
   * <code>frozen</code> collection column.
   */
  @NonNull
  default CreateIndex andColumnFull(@NonNull CqlIdentifier columnName) {
    return andColumn(columnName, "FULL");
  }

  /**
   * Shortcut for {@link #andColumnFull(CqlIdentifier)
   * andColumnFull(CqlIdentifier.fromCql(columnName)}.
   */
  @NonNull
  default CreateIndex andColumnFull(@NonNull String columnName) {
    return andColumnFull(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Specifies to create the index on a given column with the given index type. This method should
   * not be used in the general case, unless there is an additional index type to use beyond KEYS,
   * VALUES, ENTRIES, or FULL.
   */
  @NonNull
  CreateIndex andColumn(@NonNull CqlIdentifier columnName, @Nullable String indexType);

  /**
   * Shortcut for {@link #andColumn(CqlIdentifier,String)
   * andColumn(CqlIdentifier.fromCql(columnName),indexType}.
   */
  @NonNull
  default CreateIndex andColumn(@NonNull String columnName, @NonNull String indexType) {
    return andColumn(CqlIdentifier.fromCql(columnName), indexType);
  }
}
