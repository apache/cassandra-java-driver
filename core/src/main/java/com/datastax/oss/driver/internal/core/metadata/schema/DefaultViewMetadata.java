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
package com.datastax.oss.driver.internal.core.metadata.schema;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.ViewMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
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
public class DefaultViewMetadata implements ViewMetadata, Serializable {

  private static final long serialVersionUID = 1;

  @NonNull private final CqlIdentifier keyspace;
  @NonNull private final CqlIdentifier name;
  @NonNull private final CqlIdentifier baseTable;
  private final boolean includesAllColumns;
  @Nullable private final String whereClause;
  @NonNull private final UUID id;
  @NonNull private final ImmutableList<ColumnMetadata> partitionKey;
  @NonNull private final ImmutableMap<ColumnMetadata, ClusteringOrder> clusteringColumns;
  @NonNull private final ImmutableMap<CqlIdentifier, ColumnMetadata> columns;
  @NonNull private final Map<CqlIdentifier, Object> options;

  public DefaultViewMetadata(
      @NonNull CqlIdentifier keyspace,
      @NonNull CqlIdentifier name,
      @NonNull CqlIdentifier baseTable,
      boolean includesAllColumns,
      @Nullable String whereClause,
      @NonNull UUID id,
      @NonNull ImmutableList<ColumnMetadata> partitionKey,
      @NonNull ImmutableMap<ColumnMetadata, ClusteringOrder> clusteringColumns,
      @NonNull ImmutableMap<CqlIdentifier, ColumnMetadata> columns,
      @NonNull Map<CqlIdentifier, Object> options) {
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
    return Optional.of(id);
  }

  @NonNull
  @Override
  public CqlIdentifier getBaseTable() {
    return baseTable;
  }

  @Override
  public boolean includesAllColumns() {
    return includesAllColumns;
  }

  @NonNull
  @Override
  public Optional<String> getWhereClause() {
    return Optional.ofNullable(whereClause);
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
          && Objects.equals(this.whereClause, that.getWhereClause().orElse(null))
          && Objects.equals(Optional.of(this.id), that.getId())
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

  @Override
  public String toString() {
    return "DefaultViewMetadata@"
        + Integer.toHexString(hashCode())
        + "("
        + keyspace.asInternal()
        + "."
        + name.asInternal()
        + ")";
  }
}
