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
package com.datastax.dse.driver.internal.querybuilder.schema;

import static com.datastax.oss.driver.internal.querybuilder.schema.Utils.appendSet;

import com.datastax.dse.driver.api.querybuilder.schema.CreateDseTable;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseTableStart;
import com.datastax.dse.driver.api.querybuilder.schema.CreateDseTableWithOptions;
import com.datastax.dse.driver.api.querybuilder.schema.DseGraphEdgeSide;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.internal.querybuilder.schema.OptionsUtils;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateDseTable
    implements CreateDseTableStart, CreateDseTable, CreateDseTableWithOptions {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier tableName;

  private final boolean ifNotExists;
  private final boolean compactStorage;

  private final ImmutableMap<String, Object> options;

  private final ImmutableMap<CqlIdentifier, DataType> columnsInOrder;

  private final ImmutableSet<CqlIdentifier> partitionKeyColumns;
  private final ImmutableSet<CqlIdentifier> clusteringKeyColumns;
  private final ImmutableSet<CqlIdentifier> staticColumns;
  private final ImmutableSet<CqlIdentifier> regularColumns;

  private final ImmutableMap<CqlIdentifier, ClusteringOrder> orderings;

  private final DseTableVertexOperation vertexOperation;
  private final DseTableEdgeOperation edgeOperation;

  public DefaultCreateDseTable(@Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier tableName) {
    this(
        keyspace,
        tableName,
        false,
        false,
        ImmutableMap.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableMap.of(),
        null,
        null,
        ImmutableMap.of());
  }

  public DefaultCreateDseTable(
      @Nullable CqlIdentifier keyspace,
      @Nonnull CqlIdentifier tableName,
      boolean ifNotExists,
      boolean compactStorage,
      @Nonnull ImmutableMap<CqlIdentifier, DataType> columnsInOrder,
      @Nonnull ImmutableSet<CqlIdentifier> partitionKeyColumns,
      @Nonnull ImmutableSet<CqlIdentifier> clusteringKeyColumns,
      @Nonnull ImmutableSet<CqlIdentifier> staticColumns,
      @Nonnull ImmutableSet<CqlIdentifier> regularColumns,
      @Nonnull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings,
      @Nullable DseTableVertexOperation vertexOperation,
      @Nullable DseTableEdgeOperation edgeOperation,
      @Nonnull ImmutableMap<String, Object> options) {
    this.keyspace = keyspace;
    this.tableName = tableName;
    this.ifNotExists = ifNotExists;
    this.compactStorage = compactStorage;
    this.columnsInOrder = columnsInOrder;
    this.partitionKeyColumns = partitionKeyColumns;
    this.clusteringKeyColumns = clusteringKeyColumns;
    this.staticColumns = staticColumns;
    this.regularColumns = regularColumns;
    this.orderings = orderings;
    this.options = options;
    this.vertexOperation = vertexOperation;
    this.edgeOperation = edgeOperation;
  }

  @Nonnull
  @Override
  public CreateDseTableStart ifNotExists() {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        true,
        compactStorage,
        columnsInOrder,
        partitionKeyColumns,
        clusteringKeyColumns,
        staticColumns,
        regularColumns,
        orderings,
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTable withPartitionKey(
      @Nonnull CqlIdentifier columnName, @Nonnull DataType dataType) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        ImmutableCollections.append(columnsInOrder, columnName, dataType),
        appendSet(partitionKeyColumns, columnName),
        clusteringKeyColumns,
        staticColumns,
        regularColumns,
        orderings,
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTable withClusteringColumn(
      @Nonnull CqlIdentifier columnName, @Nonnull DataType dataType) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        ImmutableCollections.append(columnsInOrder, columnName, dataType),
        partitionKeyColumns,
        appendSet(clusteringKeyColumns, columnName),
        staticColumns,
        regularColumns,
        orderings,
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTable withColumn(@Nonnull CqlIdentifier columnName, @Nonnull DataType dataType) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        ImmutableCollections.append(columnsInOrder, columnName, dataType),
        partitionKeyColumns,
        clusteringKeyColumns,
        staticColumns,
        appendSet(regularColumns, columnName),
        orderings,
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTable withStaticColumn(
      @Nonnull CqlIdentifier columnName, @Nonnull DataType dataType) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        ImmutableCollections.append(columnsInOrder, columnName, dataType),
        partitionKeyColumns,
        clusteringKeyColumns,
        appendSet(staticColumns, columnName),
        regularColumns,
        orderings,
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTableWithOptions withCompactStorage() {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        true,
        columnsInOrder,
        partitionKeyColumns,
        clusteringKeyColumns,
        staticColumns,
        regularColumns,
        orderings,
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTableWithOptions withClusteringOrderByIds(
      @Nonnull Map<CqlIdentifier, ClusteringOrder> orderings) {
    return withClusteringOrders(ImmutableCollections.concat(this.orderings, orderings));
  }

  @Nonnull
  @Override
  public CreateDseTableWithOptions withClusteringOrder(
      @Nonnull CqlIdentifier columnName, @Nonnull ClusteringOrder order) {
    return withClusteringOrders(ImmutableCollections.append(orderings, columnName, order));
  }

  @Nonnull
  public CreateDseTableWithOptions withClusteringOrders(
      @Nonnull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        columnsInOrder,
        partitionKeyColumns,
        clusteringKeyColumns,
        staticColumns,
        regularColumns,
        orderings,
        vertexOperation,
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTableWithOptions withVertexLabel(@Nullable CqlIdentifier vertexLabelId) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        columnsInOrder,
        partitionKeyColumns,
        clusteringKeyColumns,
        staticColumns,
        regularColumns,
        orderings,
        new DseTableVertexOperation(DseTableGraphOperationType.WITH, vertexLabelId),
        edgeOperation,
        options);
  }

  @Nonnull
  @Override
  public CreateDseTableWithOptions withEdgeLabel(
      @Nullable CqlIdentifier edgeLabelId,
      @Nonnull DseGraphEdgeSide from,
      @Nonnull DseGraphEdgeSide to) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        columnsInOrder,
        partitionKeyColumns,
        clusteringKeyColumns,
        staticColumns,
        regularColumns,
        orderings,
        vertexOperation,
        new DseTableEdgeOperation(DseTableGraphOperationType.WITH, edgeLabelId, from, to),
        options);
  }

  @Nonnull
  @Override
  public CreateDseTable withOption(@Nonnull String name, @Nonnull Object value) {
    return new DefaultCreateDseTable(
        keyspace,
        tableName,
        ifNotExists,
        compactStorage,
        columnsInOrder,
        partitionKeyColumns,
        clusteringKeyColumns,
        staticColumns,
        regularColumns,
        orderings,
        vertexOperation,
        edgeOperation,
        ImmutableCollections.append(options, name, value));
  }

  @Nonnull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    builder.append("CREATE TABLE ");
    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }

    CqlHelper.qualify(keyspace, tableName, builder);

    if (columnsInOrder.isEmpty()) {
      // no columns provided yet.
      return builder.toString();
    }

    boolean singlePrimaryKey = partitionKeyColumns.size() == 1 && clusteringKeyColumns.size() == 0;

    builder.append(" (");

    boolean first = true;
    for (Map.Entry<CqlIdentifier, DataType> column : columnsInOrder.entrySet()) {
      if (first) {
        first = false;
      } else {
        builder.append(',');
      }
      builder
          .append(column.getKey().asCql(true))
          .append(' ')
          .append(column.getValue().asCql(true, true));

      if (singlePrimaryKey && partitionKeyColumns.contains(column.getKey())) {
        builder.append(" PRIMARY KEY");
      } else if (staticColumns.contains(column.getKey())) {
        builder.append(" STATIC");
      }
    }

    if (!singlePrimaryKey) {
      builder.append(",");
      CqlHelper.buildPrimaryKey(partitionKeyColumns, clusteringKeyColumns, builder);
    }

    builder.append(')');

    boolean firstOption = true;

    if (compactStorage) {
      firstOption = false;
      builder.append(" WITH COMPACT STORAGE");
    }

    if (!orderings.isEmpty()) {
      if (firstOption) {
        builder.append(" WITH ");
        firstOption = false;
      } else {
        builder.append(" AND ");
      }
      builder.append("CLUSTERING ORDER BY (");
      boolean firstClustering = true;

      for (Map.Entry<CqlIdentifier, ClusteringOrder> ordering : orderings.entrySet()) {
        if (firstClustering) {
          firstClustering = false;
        } else {
          builder.append(',');
        }
        builder
            .append(ordering.getKey().asCql(true))
            .append(' ')
            .append(ordering.getValue().toString());
      }

      builder.append(')');
    }

    if (vertexOperation != null) {
      if (firstOption) {
        builder.append(" WITH ");
        firstOption = false;
      } else {
        builder.append(" AND ");
      }
      vertexOperation.append(builder);
    } else if (edgeOperation != null) {
      if (firstOption) {
        builder.append(" WITH ");
        firstOption = false;
      } else {
        builder.append(" AND ");
      }
      edgeOperation.append(builder);
    }

    builder.append(OptionsUtils.buildOptions(options, firstOption));

    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @Nonnull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Nonnull
  public CqlIdentifier getTable() {
    return tableName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public boolean isCompactStorage() {
    return compactStorage;
  }

  @Nonnull
  public ImmutableMap<CqlIdentifier, DataType> getColumnsInOrder() {
    return columnsInOrder;
  }

  @Nonnull
  public ImmutableSet<CqlIdentifier> getPartitionKeyColumns() {
    return partitionKeyColumns;
  }

  @Nonnull
  public ImmutableSet<CqlIdentifier> getClusteringKeyColumns() {
    return clusteringKeyColumns;
  }

  @Nonnull
  public ImmutableSet<CqlIdentifier> getStaticColumns() {
    return staticColumns;
  }

  @Nonnull
  public ImmutableSet<CqlIdentifier> getRegularColumns() {
    return regularColumns;
  }

  @Nonnull
  public ImmutableMap<CqlIdentifier, ClusteringOrder> getOrderings() {
    return orderings;
  }
}
