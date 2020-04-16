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
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseEdgeMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseGraphTableMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseTableMetadata implements DseGraphTableMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @NonNull private final CqlIdentifier keyspace;
  @NonNull private final CqlIdentifier name;
  // null for virtual tables
  @Nullable private final UUID id;
  private final boolean compactStorage;
  private final boolean virtual;
  @NonNull private final List<ColumnMetadata> partitionKey;
  @NonNull private final Map<ColumnMetadata, ClusteringOrder> clusteringColumns;
  @NonNull private final Map<CqlIdentifier, ColumnMetadata> columns;
  @NonNull private final Map<CqlIdentifier, Object> options;
  @NonNull private final Map<CqlIdentifier, IndexMetadata> indexes;
  @Nullable private final DseVertexMetadata vertex;
  @Nullable private final DseEdgeMetadata edge;

  public DefaultDseTableMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier name,
      @Nullable UUID id,
      boolean compactStorage,
      boolean virtual,
      @NonNull List<ColumnMetadata> partitionKey,
      @NonNull Map<ColumnMetadata, ClusteringOrder> clusteringColumns,
      @NonNull Map<CqlIdentifier, ColumnMetadata> columns,
      @NonNull Map<CqlIdentifier, Object> options,
      @NonNull Map<CqlIdentifier, IndexMetadata> indexes,
      @Nullable DseVertexMetadata vertex,
      @Nullable DseEdgeMetadata edge) {
    this.keyspace = keyspace;
    this.name = name;
    this.id = id;
    this.compactStorage = compactStorage;
    this.virtual = virtual;
    this.partitionKey = partitionKey;
    this.clusteringColumns = clusteringColumns;
    this.columns = columns;
    this.options = options;
    this.indexes = indexes;
    this.vertex = vertex;
    this.edge = edge;
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
  public Optional<UUID> getId() {
    return Optional.ofNullable(id);
  }

  @Override
  public boolean isCompactStorage() {
    return compactStorage;
  }

  @Override
  public boolean isVirtual() {
    return virtual;
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

  @NonNull
  @Override
  public Optional<DseVertexMetadata> getVertex() {
    return Optional.ofNullable(vertex);
  }

  @NonNull
  @Override
  public Optional<DseEdgeMetadata> getEdge() {
    return Optional.ofNullable(edge);
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof DseGraphTableMetadata) {
      DseGraphTableMetadata that = (DseGraphTableMetadata) other;
      return Objects.equals(this.keyspace, that.getKeyspace())
          && Objects.equals(this.name, that.getName())
          && Objects.equals(this.id, that.getId().orElse(null))
          && this.compactStorage == that.isCompactStorage()
          && this.virtual == that.isVirtual()
          && Objects.equals(this.partitionKey, that.getPartitionKey())
          && Objects.equals(this.clusteringColumns, that.getClusteringColumns())
          && Objects.equals(this.columns, that.getColumns())
          && Objects.equals(this.indexes, that.getIndexes())
          && Objects.equals(this.vertex, that.getVertex().orElse(null))
          && Objects.equals(this.edge, that.getEdge().orElse(null));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        keyspace,
        name,
        id,
        compactStorage,
        virtual,
        partitionKey,
        clusteringColumns,
        columns,
        indexes,
        vertex,
        edge);
  }
}
