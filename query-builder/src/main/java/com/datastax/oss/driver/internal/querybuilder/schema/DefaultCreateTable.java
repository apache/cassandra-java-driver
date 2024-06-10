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
package com.datastax.oss.driver.internal.querybuilder.schema;

import static com.datastax.oss.driver.internal.querybuilder.schema.Utils.appendSet;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTable;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateTable implements CreateTableStart, CreateTable, CreateTableWithOptions {

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

  public DefaultCreateTable(@NonNull CqlIdentifier tableName) {
    this(null, tableName);
  }

  public DefaultCreateTable(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier tableName) {
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
        ImmutableMap.of());
  }

  public DefaultCreateTable(
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
  }

  @NonNull
  @Override
  public CreateTableStart ifNotExists() {
    return new DefaultCreateTable(
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
        options);
  }

  @NonNull
  @Override
  public CreateTable withPartitionKey(
      @NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
    return new DefaultCreateTable(
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
        options);
  }

  @NonNull
  @Override
  public CreateTable withClusteringColumn(
      @NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
    return new DefaultCreateTable(
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
        options);
  }

  @NonNull
  @Override
  public CreateTable withColumn(@NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
    return new DefaultCreateTable(
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
        options);
  }

  @NonNull
  @Override
  public CreateTable withStaticColumn(
      @NonNull CqlIdentifier columnName, @NonNull DataType dataType) {
    return new DefaultCreateTable(
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
        options);
  }

  @NonNull
  @Override
  public CreateTableWithOptions withCompactStorage() {
    return new DefaultCreateTable(
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
        options);
  }

  @NonNull
  @Override
  public CreateTableWithOptions withClusteringOrderByIds(
      @NonNull Map<CqlIdentifier, ClusteringOrder> orderings) {
    return withClusteringOrders(ImmutableCollections.concat(this.orderings, orderings));
  }

  @NonNull
  @Override
  public CreateTableWithOptions withClusteringOrder(
      @NonNull CqlIdentifier columnName, @NonNull ClusteringOrder order) {
    return withClusteringOrders(ImmutableCollections.append(orderings, columnName, order));
  }

  @NonNull
  public CreateTableWithOptions withClusteringOrders(
      @NonNull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings) {
    return new DefaultCreateTable(
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
        options);
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

    if (compactStorage || !orderings.isEmpty() || !options.isEmpty()) {
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

      builder.append(OptionsUtils.buildOptions(options, firstOption));
    }

    return builder.toString();
  }

  @Override
  public String toString() {
    return asCql();
  }

  @NonNull
  @Override
  public CreateTable withOption(@NonNull String name, @NonNull Object value) {
    return new DefaultCreateTable(
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
        ImmutableCollections.append(options, name, value));
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
