/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class DefaultViewMetadata implements ViewMetadata {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier name;
  private final CqlIdentifier baseTable;
  private final boolean includesAllColumns;
  private final String whereClause;
  private final UUID id;
  private final ImmutableList<ColumnMetadata> partitionKey;
  private final ImmutableMap<ColumnMetadata, ClusteringOrder> clusteringColumns;
  private final ImmutableMap<CqlIdentifier, ColumnMetadata> columns;
  private final Map<CqlIdentifier, Object> options;

  public DefaultViewMetadata(
      CqlIdentifier keyspace,
      CqlIdentifier name,
      CqlIdentifier baseTable,
      boolean includesAllColumns,
      String whereClause,
      UUID id,
      ImmutableList<ColumnMetadata> partitionKey,
      ImmutableMap<ColumnMetadata, ClusteringOrder> clusteringColumns,
      ImmutableMap<CqlIdentifier, ColumnMetadata> columns,
      Map<CqlIdentifier, Object> options) {
    this.keyspace = keyspace;
    this.name = name;
    this.baseTable = baseTable;
    this.includesAllColumns = includesAllColumns;
    this.whereClause = whereClause;
    this.id = id;
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.columns = columns;
    this.options = options;
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public CqlIdentifier getBaseTable() {
    return baseTable;
  }

  @Override
  public boolean includesAllColumns() {
    return includesAllColumns;
  }

  @Override
  public String getWhereClause() {
    return whereClause;
  }

  @Override
  public List<ColumnMetadata> getPartitionKey() {
    return partitionKey;
  }

  @Override
  public Map<ColumnMetadata, ClusteringOrder> getClusteringColumns() {
    return clusteringColumns;
  }

  @Override
  public Map<CqlIdentifier, ColumnMetadata> getColumns() {
    return columns;
  }

  @Override
  public Map<CqlIdentifier, Object> getOptions() {
    return options;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof ViewMetadata) {
      ViewMetadata that = (ViewMetadata) other;
      return Objects.equals(this.keyspace, that.getKeyspace())
          && Objects.equals(this.name, that.getName())
          && Objects.equals(this.baseTable, that.getBaseTable())
          && this.includesAllColumns == that.includesAllColumns()
          && Objects.equals(this.whereClause, that.getWhereClause())
          && Objects.equals(this.id, that.getId())
          && Objects.equals(this.partitionKey, that.getPartitionKey())
          && Objects.equals(this.clusteringColumns, that.getClusteringColumns())
          && Objects.equals(this.columns, that.getColumns())
          && Objects.equals(this.options, that.getOptions());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keyspace,
        name,
        baseTable,
        includesAllColumns,
        whereClause,
        id,
        partitionKey,
        clusteringColumns,
        columns,
        options);
  }
}
