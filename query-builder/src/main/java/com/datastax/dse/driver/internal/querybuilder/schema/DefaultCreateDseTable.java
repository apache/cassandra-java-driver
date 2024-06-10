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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
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

  public DefaultCreateDseTable(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
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
      @NonNull CqlIdentifier tableName,
      boolean ifNotExists,
      boolean compactStorage,
      @NonNull ImmutableMap<CqlIdentifier, DataType> columnsInOrder,
      @NonNull ImmutableSet<CqlIdentifier> partitionKeyColumns,
      @NonNull ImmutableSet<CqlIdentifier> clusteringKeyColumns,
      @NonNull ImmutableSet<CqlIdentifier> staticColumns,
      @NonNull ImmutableSet<CqlIdentifier> regularColumns,
      @NonNull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings,
      @Nullable DseTableVertexOperation vertexOperation,
      @Nullable DseTableEdgeOperation edgeOperation,
      @NonNull ImmutableMap<String, Object> options) {
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

  @NonNull
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

  @NonNull
  @Override
  public CreateDseTable withPartitionKey(
      @NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
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

  @NonNull
  @Override
  public CreateDseTable withClusteringColumn(
      @NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
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

  @NonNull
  @Override
  public CreateDseTable withColumn(@NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
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

  @NonNull
  @Override
  public CreateDseTable withStaticColumn(
      @NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
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

  @NonNull
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

  @NonNull
  @Override
  public CreateDseTableWithOptions withClusteringOrderByIds(
      @NonNull Map<CqlIdentifier, ClusteringOrder> orderings) {
    return withClusteringOrders(ImmutableCollections.concat(this.orderings, orderings));
  }

  @NonNull
  @Override
  public CreateDseTableWithOptions withClusteringOrder(
      @NonNull CqlIdentifier columnName, @NonNull ClusteringOrder order) {
    return withClusteringOrders(ImmutableCollections.append(orderings, columnName, order));
  }

  @NonNull
  public CreateDseTableWithOptions withClusteringOrders(
      @NonNull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings) {
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

  @NonNull
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

  @NonNull
  @Override
  public CreateDseTableWithOptions withEdgeLabel(
      @Nullable CqlIdentifier edgeLabelId,
      @NonNull DseGraphEdgeSide from,
      @NonNull DseGraphEdgeSide to) {
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

  @NonNull
  @Override
  public CreateDseTable withOption(@NonNull String name, @NonNull Object value) {
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

  @NonNull
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

  @NonNull
  @Override
  public Map<String, Object> getOptions() {
    return options;
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getTable() {
    return tableName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  public boolean isCompactStorage() {
    return compactStorage;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, DataType> getColumnsInOrder() {
    return columnsInOrder;
  }

  @NonNull
  public ImmutableSet<CqlIdentifier> getPartitionKeyColumns() {
    return partitionKeyColumns;
  }

  @NonNull
  public ImmutableSet<CqlIdentifier> getClusteringKeyColumns() {
    return clusteringKeyColumns;
  }

  @NonNull
  public ImmutableSet<CqlIdentifier> getStaticColumns() {
    return staticColumns;
  }

  @NonNull
  public ImmutableSet<CqlIdentifier> getRegularColumns() {
    return regularColumns;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, ClusteringOrder> getOrderings() {
    return orderings;
  }
}
