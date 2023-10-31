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
import java.util.List;
import javax.annotation.Nonnull;

public interface DseGraphEdgeSide {

  /** Starts the definition of a graph edge side by designating the from/to table. */
  @Nonnull
  static DseGraphEdgeSide table(@Nonnull CqlIdentifier tableId) {
    return new DefaultDseGraphEdgeSide(tableId);
  }

  /** Shortcut for {@link #table(CqlIdentifier) table(CqlIdentifier.fromCql(tableName))}. */
  @Nonnull
  static DseGraphEdgeSide table(@Nonnull String tableName) {
    return table(CqlIdentifier.fromCql(tableName));
  }

  /**
   * Adds a partition key column to the primary key definition of this edge side.
   *
   * <p>Call this method multiple times if the partition key is composite.
   */
  @Nonnull
  DseGraphEdgeSide withPartitionKey(@Nonnull CqlIdentifier columnId);

  /**
   * Shortcut for {@link #withPartitionKey(CqlIdentifier)
   * withPartitionKey(CqlIdentifier.fromCql(columnName))}.
   */
  @Nonnull
  default DseGraphEdgeSide withPartitionKey(@Nonnull String columnName) {
    return withPartitionKey(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Adds a clustering column to the primary key definition of this edge side.
   *
   * <p>Call this method multiple times to add more than one clustering column.
   */
  @Nonnull
  DseGraphEdgeSide withClusteringColumn(@Nonnull CqlIdentifier columnId);

  /**
   * Shortcut for {@link #withClusteringColumn(CqlIdentifier)
   * withClusteringColumn(CqlIdentifier.fromCql(columnName))}.
   */
  @Nonnull
  default DseGraphEdgeSide withClusteringColumn(@Nonnull String columnName) {
    return withClusteringColumn(CqlIdentifier.fromCql(columnName));
  }

  @Nonnull
  CqlIdentifier getTableId();

  @Nonnull
  List<CqlIdentifier> getPartitionKeyColumns();

  @Nonnull
  List<CqlIdentifier> getClusteringColumns();
}
