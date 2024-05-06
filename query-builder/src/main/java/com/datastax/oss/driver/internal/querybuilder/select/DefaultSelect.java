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
package com.datastax.oss.driver.internal.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatementBuilder;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Ann;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;
import com.datastax.oss.driver.internal.querybuilder.ImmutableCollections;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultSelect implements SelectFrom, Select {

  private static final ImmutableList<Selector> SELECT_ALL = ImmutableList.of(AllSelector.INSTANCE);

  private final CqlIdentifier keyspace;
  private final CqlIdentifier table;
  private final boolean isJson;
  private final boolean isDistinct;
  private final ImmutableList<Selector> selectors;
  private final ImmutableList<Relation> relations;
  private final ImmutableList<Selector> groupByClauses;
  private final ImmutableMap<CqlIdentifier, ClusteringOrder> orderings;
  private final Ann ann;
  private final Object limit;
  private final Object perPartitionLimit;
  private final boolean allowsFiltering;

  public DefaultSelect(@Nullable CqlIdentifier keyspace, @NonNull CqlIdentifier table) {
    this(
        keyspace,
        table,
        false,
        false,
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableList.of(),
        ImmutableMap.of(),
        null,
        null,
        null,
        false);
  }

  /**
   * This constructor is public only as a convenience for custom extensions of the query builder.
   *
   * @param selectors if it contains {@link AllSelector#INSTANCE}, that must be the only element.
   *     This isn't re-checked because methods that call this constructor internally already do it,
   *     make sure you do it yourself.
   * @param ann Approximate nearest neighbor. ANN ordering does not support secondary ordering or ASC order.
   */
  public DefaultSelect(
      @Nullable CqlIdentifier keyspace,
      @NonNull CqlIdentifier table,
      boolean isJson,
      boolean isDistinct,
      @NonNull ImmutableList<Selector> selectors,
      @NonNull ImmutableList<Relation> relations,
      @NonNull ImmutableList<Selector> groupByClauses,
      @NonNull ImmutableMap<CqlIdentifier, ClusteringOrder> orderings,
      @Nullable Ann ann,
      @Nullable Object limit,
      @Nullable Object perPartitionLimit,
      boolean allowsFiltering) {
    this.groupByClauses = groupByClauses;
    this.orderings = orderings;
    Preconditions.checkArgument(
        limit == null
            || (limit instanceof Integer && (Integer) limit > 0)
            || limit instanceof BindMarker,
        "limit must be a strictly positive integer or a bind marker");
    Preconditions.checkArgument(
            orderings.isEmpty() || ann == null,
    "ANN ordering does not support secondary ordering");
    this.ann = ann;
    this.keyspace = keyspace;
    this.table = table;
    this.isJson = isJson;
    this.isDistinct = isDistinct;
    this.selectors = selectors;
    this.relations = relations;
    this.limit = limit;
    this.perPartitionLimit = perPartitionLimit;
    this.allowsFiltering = allowsFiltering;
  }

  @NonNull
  @Override
  public SelectFrom json() {
    return new DefaultSelect(
        keyspace,
        table,
        true,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public SelectFrom distinct() {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        true,
        selectors,
        relations,
        groupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select selector(@NonNull Selector selector) {
    ImmutableList<Selector> newSelectors;
    if (selector == AllSelector.INSTANCE) {
      // '*' cancels any previous one
      newSelectors = SELECT_ALL;
    } else if (SELECT_ALL.equals(selectors)) {
      // previous '*' gets cancelled
      newSelectors = ImmutableList.of(selector);
    } else {
      newSelectors = ImmutableCollections.append(selectors, selector);
    }
    return withSelectors(newSelectors);
  }

  @NonNull
  @Override
  public Select selectors(@NonNull Iterable<Selector> additionalSelectors) {
    ImmutableList.Builder<Selector> newSelectors = ImmutableList.builder();
    if (!SELECT_ALL.equals(selectors)) { // previous '*' gets cancelled
      newSelectors.addAll(selectors);
    }
    for (Selector selector : additionalSelectors) {
      if (selector == AllSelector.INSTANCE) {
        throw new IllegalArgumentException("Can't pass the * selector to selectors()");
      }
      newSelectors.add(selector);
    }
    return withSelectors(newSelectors.build());
  }

  @NonNull
  @Override
  public Select as(@NonNull CqlIdentifier alias) {
    if (SELECT_ALL.equals(selectors)) {
      throw new IllegalStateException("Can't alias the * selector");
    } else if (selectors.isEmpty()) {
      throw new IllegalStateException("Can't alias, no selectors defined");
    }
    return withSelectors(ImmutableCollections.modifyLast(selectors, last -> last.as(alias)));
  }

  @NonNull
  public Select withSelectors(@NonNull ImmutableList<Selector> newSelectors) {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        newSelectors,
        relations,
        groupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select where(@NonNull Relation relation) {
    return withRelations(ImmutableCollections.append(relations, relation));
  }

  @NonNull
  @Override
  public Select where(@NonNull Iterable<Relation> additionalRelations) {
    return withRelations(ImmutableCollections.concat(relations, additionalRelations));
  }

  @NonNull
  public Select withRelations(@NonNull ImmutableList<Relation> newRelations) {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        newRelations,
        groupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select groupBy(@NonNull Selector groupByClause) {
    return withGroupByClauses(ImmutableCollections.append(groupByClauses, groupByClause));
  }

  @NonNull
  @Override
  public Select groupBy(@NonNull Iterable<Selector> newGroupByClauses) {
    return withGroupByClauses(ImmutableCollections.concat(groupByClauses, newGroupByClauses));
  }

  @NonNull
  public Select withGroupByClauses(@NonNull ImmutableList<Selector> newGroupByClauses) {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        newGroupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select orderBy(@NonNull CqlIdentifier columnId, @NonNull ClusteringOrder order) {
    return withOrderings(ImmutableCollections.append(orderings, columnId, order));
  }

  @NonNull
  @Override
  public Select orderByIds(@NonNull Map<CqlIdentifier, ClusteringOrder> newOrderings) {
    return withOrderings(ImmutableCollections.concat(orderings, newOrderings));
  }

  @NonNull
  @Override
  public Select orderBy(@NonNull Ann ann){
    return withAnn(ann);
  }

  @NonNull
  public Select withOrderings(@NonNull ImmutableMap<CqlIdentifier, ClusteringOrder> newOrderings) {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        newOrderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull Select withAnn(@NonNull Ann ann){
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        orderings,
        ann,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select limit(int limit) {
    Preconditions.checkArgument(limit > 0, "Limit must be strictly positive");
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select limit(@Nullable BindMarker bindMarker) {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        orderings,
        null,
        bindMarker,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select perPartitionLimit(int perPartitionLimit) {
    Preconditions.checkArgument(
        perPartitionLimit > 0, "perPartitionLimit must be strictly positive");
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select perPartitionLimit(@Nullable BindMarker bindMarker) {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        orderings,
        null,
        limit,
        bindMarker,
        allowsFiltering);
  }

  @NonNull
  @Override
  public Select allowFiltering() {
    return new DefaultSelect(
        keyspace,
        table,
        isJson,
        isDistinct,
        selectors,
        relations,
        groupByClauses,
        orderings,
        null,
        limit,
        perPartitionLimit,
        true);
  }

  @NonNull
  @Override
  public String asCql() {
    StringBuilder builder = new StringBuilder();

    builder.append("SELECT");
    if (isJson) {
      builder.append(" JSON");
    }
    if (isDistinct) {
      builder.append(" DISTINCT");
    }

    CqlHelper.append(selectors, builder, " ", ",", null);

    builder.append(" FROM ");
    CqlHelper.qualify(keyspace, table, builder);

    CqlHelper.append(relations, builder, " WHERE ", " AND ", null);
    CqlHelper.append(groupByClauses, builder, " GROUP BY ", ",", null);

    if (ann != null){
      builder.append(" ").append(this.ann.asCql());
    }else{
      boolean first = true;
      for (Map.Entry<CqlIdentifier, ClusteringOrder> entry : orderings.entrySet()) {
        if (first) {
          builder.append(" ORDER BY ");
          first = false;
        } else {
          builder.append(",");
        }
        builder.append(entry.getKey().asCql(true)).append(" ").append(entry.getValue().name());
      }
    }

    if (limit != null) {
      builder.append(" LIMIT ");
      if (limit instanceof BindMarker) {
        ((BindMarker) limit).appendTo(builder);
      } else {
        builder.append(limit);
      }
    }

    if (perPartitionLimit != null) {
      builder.append(" PER PARTITION LIMIT ");
      if (perPartitionLimit instanceof BindMarker) {
        ((BindMarker) perPartitionLimit).appendTo(builder);
      } else {
        builder.append(perPartitionLimit);
      }
    }

    if (allowsFiltering) {
      builder.append(" ALLOW FILTERING");
    }

    return builder.toString();
  }

  @NonNull
  @Override
  public SimpleStatement build() {
    return builder().build();
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Object... values) {
    return builder().addPositionalValues(values).build();
  }

  @NonNull
  @Override
  public SimpleStatement build(@NonNull Map<String, Object> namedValues) {
    SimpleStatementBuilder builder = builder();
    for (Map.Entry<String, Object> entry : namedValues.entrySet()) {
      builder.addNamedValue(entry.getKey(), entry.getValue());
    }
    return builder.build();
  }

  @NonNull
  @Override
  public SimpleStatementBuilder builder() {
    // SELECT statements are always idempotent
    return SimpleStatement.builder(asCql()).setIdempotence(true);
  }

  @Nullable
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @NonNull
  public CqlIdentifier getTable() {
    return table;
  }

  public boolean isJson() {
    return isJson;
  }

  public boolean isDistinct() {
    return isDistinct;
  }

  @NonNull
  public ImmutableList<Selector> getSelectors() {
    return selectors;
  }

  @NonNull
  public ImmutableList<Relation> getRelations() {
    return relations;
  }

  @NonNull
  public ImmutableList<Selector> getGroupByClauses() {
    return groupByClauses;
  }

  @NonNull
  public ImmutableMap<CqlIdentifier, ClusteringOrder> getOrderings() {
    return orderings;
  }

  @Nullable
  public Object getLimit() {
    return limit;
  }

  @Nullable
  public Object getPerPartitionLimit() {
    return perPartitionLimit;
  }

  public boolean allowsFiltering() {
    return allowsFiltering;
  }

  @Override
  public String toString() {
    return asCql();
  }
}
