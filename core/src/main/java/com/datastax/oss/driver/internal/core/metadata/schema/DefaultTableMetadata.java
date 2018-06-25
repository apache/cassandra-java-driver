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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultTableMetadata implements TableMetadata {

  @NonNull private final CqlIdentifier keyspace;
  @NonNull private final CqlIdentifier name;
  @NonNull private final UUID id;
  private final boolean compactStorage;
  @NonNull private final List<ColumnMetadata> partitionKey;
  @NonNull private final Map<ColumnMetadata, ClusteringOrder> clusteringColumns;
  @NonNull private final Map<CqlIdentifier, ColumnMetadata> columns;
  @NonNull private final Map<CqlIdentifier, Object> options;
  @NonNull private final Map<CqlIdentifier, IndexMetadata> indexes;

  public DefaultTableMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier name,
      @NonNull UUID id,
      boolean compactStorage,
      @NonNull List<ColumnMetadata> partitionKey,
      @NonNull Map<ColumnMetadata, ClusteringOrder> clusteringColumns,
      @NonNull Map<CqlIdentifier, ColumnMetadata> columns,
      @NonNull Map<CqlIdentifier, Object> options,
      @NonNull Map<CqlIdentifier, IndexMetadata> indexes) {
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

  @NonNull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @NonNull
  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public boolean isCompactStorage() {
    return compactStorage;
  }

  @NonNull
  @Override
  public List<ColumnMetadata> getPartitionKey() {
    return partitionKey;
  }

  @NonNull
  @Override
  public Map<ColumnMetadata, ClusteringOrder> getClusteringColumns() {
    return clusteringColumns;
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, ColumnMetadata> getColumns() {
    return columns;
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, Object> getOptions() {
    return options;
  }

  @NonNull
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
