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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

public class DefaultTableMetadata implements TableMetadata {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier name;
  private final UUID id;
  private final boolean compactStorage;
  private final List<ColumnMetadata> partitionKey;
  private final Map<ColumnMetadata, ClusteringOrder> clusteringColumns;
  private final Map<CqlIdentifier, ColumnMetadata> columns;
  private final Map<CqlIdentifier, Object> options;
  private final Map<CqlIdentifier, IndexMetadata> indexes;

  public DefaultTableMetadata(
      CqlIdentifier keyspace,
      CqlIdentifier name,
      UUID id,
      boolean compactStorage,
      List<ColumnMetadata> partitionKey,
      Map<ColumnMetadata, ClusteringOrder> clusteringColumns,
      Map<CqlIdentifier, ColumnMetadata> columns,
      Map<CqlIdentifier, Object> options,
      Map<CqlIdentifier, IndexMetadata> indexes) {
    this.keyspace = keyspace;
    this.name = name;
    this.id = id;
    this.compactStorage = compactStorage;
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.columns = columns;
    this.options = options;
    this.indexes = indexes;
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
  public boolean isCompactStorage() {
    return compactStorage;
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
  public Map<CqlIdentifier, IndexMetadata> getIndexes() {
    return indexes;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof TableMetadata) {
      TableMetadata that = (TableMetadata) other;
      return Objects.equals(this.keyspace, that.getKeyspace())
          && Objects.equals(this.name, that.getName())
          && Objects.equals(this.id, that.getId())
          && this.compactStorage == that.isCompactStorage()
          && Objects.equals(this.partitionKey, that.getPartitionKey())
          && Objects.equals(this.clusteringColumns, that.getClusteringColumns())
          && Objects.equals(this.columns, that.getColumns())
          && Objects.equals(this.indexes, that.getIndexes());
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keyspace, name, id, compactStorage, partitionKey, clusteringColumns, columns, indexes);
  }
}
