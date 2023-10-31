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
package com.datastax.dse.driver.internal.core.metadata.schema;

import com.datastax.dse.driver.api.core.metadata.schema.DseEdgeMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseGraphTableMetadata;
import com.datastax.dse.driver.api.core.metadata.schema.DseVertexMetadata;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.IndexMetadata;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultDseTableMetadata implements DseGraphTableMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @Nonnull private final CqlIdentifier keyspace;
  @Nonnull private final CqlIdentifier name;
  // null for virtual tables
  @Nullable private final UUID id;
  private final boolean compactStorage;
  private final boolean virtual;
  @Nonnull private final List<ColumnMetadata> partitionKey;
  @Nonnull private final Map<ColumnMetadata, ClusteringOrder> clusteringColumns;
  @Nonnull private final Map<CqlIdentifier, ColumnMetadata> columns;
  @Nonnull private final Map<CqlIdentifier, Object> options;
  @Nonnull private final Map<CqlIdentifier, IndexMetadata> indexes;
  @Nullable private final DseVertexMetadata vertex;
  @Nullable private final DseEdgeMetadata edge;

  public DefaultDseTableMetadata(
      @Nonnull CqlIdentifier keyspace,
      @Nonnull CqlIdentifier name,
      @Nullable UUID id,
      boolean compactStorage,
      boolean virtual,
      @Nonnull List<ColumnMetadata> partitionKey,
      @Nonnull Map<ColumnMetadata, ClusteringOrder> clusteringColumns,
      @Nonnull Map<CqlIdentifier, ColumnMetadata> columns,
      @Nonnull Map<CqlIdentifier, Object> options,
      @Nonnull Map<CqlIdentifier, IndexMetadata> indexes,
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

  @Nonnull
  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Nonnull
  @Override
  public CqlIdentifier getName() {
    return name;
  }

  @Nonnull
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

  @Nonnull
  @Override
  public List<ColumnMetadata> getPartitionKey() {
    return partitionKey;
  }

  @Nonnull
  @Override
  public Map<ColumnMetadata, ClusteringOrder> getClusteringColumns() {
    return clusteringColumns;
  }

  @Nonnull
  @Override
  public Map<CqlIdentifier, ColumnMetadata> getColumns() {
    return columns;
  }

  @Nonnull
  @Override
  public Map<CqlIdentifier, Object> getOptions() {
    return options;
  }

  @Nonnull
  @Override
  public Map<CqlIdentifier, IndexMetadata> getIndexes() {
    return indexes;
  }

  @Nonnull
  @Override
  public Optional<DseVertexMetadata> getVertex() {
    return Optional.ofNullable(vertex);
  }

  @Nonnull
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
