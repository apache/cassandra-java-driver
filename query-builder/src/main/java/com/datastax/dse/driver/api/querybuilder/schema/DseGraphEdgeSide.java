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
package com.datastax.dse.driver.api.querybuilder.schema;

import com.datastax.dse.driver.internal.querybuilder.schema.DefaultDseGraphEdgeSide;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;

public interface DseGraphEdgeSide {

  /** Starts the definition of a graph edge side by designating the from/to table. */
  @NonNull
  static DseGraphEdgeSide table(@NonNull CqlIdentifier tableId) {
    return new DefaultDseGraphEdgeSide(tableId);
  }

  /** Shortcut for {@link #table(CqlIdentifier) table(CqlIdentifier.fromCql(tableName))}. */
  @NonNull
  static DseGraphEdgeSide table(@NonNull String tableName) {
    return table(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Adds a partition key column to the primary key definition of this edge side.
   *
   * <p>Call this method multiple times if the partition key is composite.
   */
  @NonNull
  DseGraphEdgeSide withPartitionKey(@NonNull CqlIdentifier columnId);

  /**
   * Shortcut for {@link #withPartitionKey(CqlIdentifier)
   * withPartitionKey(CqlIdentifier.fromCql(columnName))}.
   */
  @NonNull
  default DseGraphEdgeSide withPartitionKey(@NonNull String columnName) {
    return withPartitionKey(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Adds a clustering column to the primary key definition of this edge side.
   *
   * <p>Call this method multiple times to add more than one clustering column.
   */
  @NonNull
  DseGraphEdgeSide withClusteringColumn(@NonNull CqlIdentifier columnId);

  /**
   * Shortcut for {@link #withClusteringColumn(CqlIdentifier)
   * withClusteringColumn(CqlIdentifier.fromCql(columnName))}.
   */
  @NonNull
  default DseGraphEdgeSide withClusteringColumn(@NonNull String columnName) {
    return withClusteringColumn(CqlIdentifier.fromCql(columnName));
  }

  @NonNull
  CqlIdentifier getTableId();

  @NonNull
  List<CqlIdentifier> getPartitionKeyColumns();

  @NonNull
  List<CqlIdentifier> getClusteringColumns();
}
