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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedView;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedViewPrimaryKey;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedViewSelection;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedViewSelectionWithColumns;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedViewStart;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedViewWhere;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedViewWhereStart;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultCreateMaterializedView
    implements CreateMaterializedViewStart,
        CreateMaterializedViewSelectionWithColumns,
        CreateMaterializedViewWhere,
        CreateMaterializedViewPrimaryKey,
        CreateMaterializedView {

  private final CqlIdentifier keyspace;
  private final CqlIdentifier viewName;

  private final boolean ifNotExists;

  private final CqlIdentifier baseTableKeyspace;
  private final CqlIdentifier baseTable;

  private final ImmutableList<Selector> selectors;
  private final ImmutableList<Relation> whereRelations;
  private final ImmutableSet<CqlIdentifier> partitionKeyColumns;
  private final ImmutableSet<CqlIdentifier> clusteringKeyColumns;

  private final ImmutableMap<CqlIdentifier, ClusteringOrder> orderings;

  private final ImmutableMap<String, Object> options;

  public DefaultCreateMaterializedView(@Nonnull CqlIdentifier viewName) {
    this(null, viewName);
  }

  public DefaultCreateMaterializedView(
      @Nullable CqlIdentifier keyspace, @Nonnull CqlIdentifier viewName) {
    this(
        keyspace,
        viewName,
        false,
        null,
        null,
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableSet.of(),
        ImmutableSet.of(),
        ImmutableMap.of(),
        ImmutableMap.of());
  }

  public DefaultCreateMaterializedView(
      @Nullable CqlIdentifier keyspace,
      @Nonnull CqlIdentifier viewName,
      boolean ifNotExists,
      @Nullable CqlIdentifier baseTableKeyspace,
      @Nullable CqlIdentifier baseTable,
      @Nonnull ImmutableList<Selector> selectors,
      @Nonnull ImmutableList<Relation> whereRelations,
      @Nonnull ImmutableSet<CqlIdentifier> partitionKeyColumns,
      @Nonnull ImmutableSet<CqlIdentifier> clusteringKeyColumns,
      @Nonnull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings,
      @Nonnull ImmutableMap<String, Object> options) {
    this.keyspace = keyspace;
    this.viewName = viewName;
    this.ifNotExists = ifNotExists;
    this.baseTableKeyspace = baseTableKeyspace;
    this.baseTable = baseTable;
    this.selectors = selectors;
    this.whereRelations = whereRelations;
    this.partitionKeyColumns = partitionKeyColumns;
    this.clusteringKeyColumns = clusteringKeyColumns;
    this.orderings = orderings;
    this.options = options;
  }

  @Nonnull
  @Override
  public CreateMaterializedViewWhereStart all() {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        ImmutableCollections.append(selectors, Selector.all()),
        whereRelations,
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewSelectionWithColumns column(@Nonnull CqlIdentifier columnName) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        ImmutableCollections.append(selectors, Selector.column(columnName)),
        whereRelations,
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewSelectionWithColumns columnsIds(
      @Nonnull Iterable<CqlIdentifier> columnIds) {
    ImmutableList.Builder<Selector> columnSelectors = ImmutableList.builder();
    for (CqlIdentifier column : columnIds) {
      columnSelectors.add(Selector.column(column));
    }
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        ImmutableCollections.concat(selectors, columnSelectors.build()),
        whereRelations,
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewWhere where(@Nonnull Relation relation) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        selectors,
        ImmutableCollections.append(whereRelations, relation),
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewWhere where(@Nonnull Iterable<Relation> additionalRelations) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        selectors,
        ImmutableCollections.concat(whereRelations, additionalRelations),
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewPrimaryKey withPartitionKey(@Nonnull CqlIdentifier columnName) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        selectors,
        whereRelations,
        Utils.appendSet(partitionKeyColumns, columnName),
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewPrimaryKey withClusteringColumn(@Nonnull CqlIdentifier columnName) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        selectors,
        whereRelations,
        partitionKeyColumns,
        Utils.appendSet(clusteringKeyColumns, columnName),
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewStart ifNotExists() {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        true,
        baseTableKeyspace,
        baseTable,
        selectors,
        whereRelations,
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewSelection asSelectFrom(@Nonnull CqlIdentifier table) {
    return asSelectFrom(null, table);
  }

  @Nonnull
  @Override
  public CreateMaterializedViewSelection asSelectFrom(
      CqlIdentifier baseTableKeyspace, @Nonnull CqlIdentifier baseTable) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        selectors,
        whereRelations,
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedView withClusteringOrderByIds(
      @Nonnull Map<CqlIdentifier, ClusteringOrder> orderings) {
    return withClusteringOrders(ImmutableCollections.concat(this.orderings, orderings));
  }

  @Nonnull
  @Override
  public CreateMaterializedView withClusteringOrder(
      @Nonnull CqlIdentifier columnName, @Nonnull ClusteringOrder order) {
    return withClusteringOrders(ImmutableCollections.append(orderings, columnName, order));
  }

  @Nonnull
  public CreateMaterializedView withClusteringOrders(
      @Nonnull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        selectors,
        whereRelations,
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        options);
  }

  @Nonnull
  @Override
  public CreateMaterializedView withOption(@Nonnull String name, @Nonnull Object value) {
    return new DefaultCreateMaterializedView(
        keyspace,
        viewName,
        ifNotExists,
        baseTableKeyspace,
        baseTable,
        selectors,
        whereRelations,
        partitionKeyColumns,
        clusteringKeyColumns,
        orderings,
        ImmutableCollections.append(options, name, value));
  }

  @Nonnull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder("CREATE MATERIALIZED VIEW ");
    if (ifNotExists) {
      builder.append("IF NOT EXISTS ");
    }

    CqlHelper.qualify(keyspace, viewName, builder);

    if (selectors.isEmpty()) {
      // selectors not provided yet.
      return builder.toString();
    }

    CqlHelper.append(selectors, builder, " AS SELECT ", ",", " FROM ");

    if (baseTable == null) {
      // base table not provided yet.
      return builder.toString();
    }

    CqlHelper.qualify(baseTableKeyspace, baseTable, builder);

    if (whereRelations.isEmpty()) {
      // where clause not provided yet.
      return builder.toString();
    }

    CqlHelper.append(whereRelations, builder, " WHERE ", " AND ", " ");

    CqlHelper.buildPrimaryKey(partitionKeyColumns, clusteringKeyColumns, builder);

    if (!orderings.isEmpty() || !options.isEmpty()) {
      boolean firstOption = true;

      if (!orderings.isEmpty()) {
        builder.append(" WITH ");
        firstOption = false;
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
  public CqlIdentifier getMaterializedView() {
    return viewName;
  }

  public boolean isIfNotExists() {
    return ifNotExists;
  }

  @Nullable
  public CqlIdentifier getBaseTableKeyspace() {
    return baseTableKeyspace;
  }

  @Nullable
  public CqlIdentifier getBaseTable() {
    return baseTable;
  }

  @Nonnull
  public ImmutableList<Selector> getSelectors() {
    return selectors;
  }

  @Nonnull
  public ImmutableList<Relation> getWhereRelations() {
    return whereRelations;
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
  public ImmutableMap<CqlIdentifier, ClusteringOrder> getOrderings() {
    return orderings;
  }
}
