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

/*
 * Copyright (C) 2022 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.api.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Arrays;
import java.util.Map;

/**
 * A complete SELECT query.
 *
 * <p>It knows about the table and at least one selector, and is therefore buildable. Additional
 * selectors and clauses can still be added before building.
 */
public interface Select extends OngoingSelection, OngoingWhereClause<Select>, BuildableQuery {

  /**
   * Adds the provided GROUP BY clauses to the query.
   *
   * <p>As of version 4.0, Apache Cassandra only allows grouping by columns, therefore you can use
   * the shortcuts {@link #groupByColumns(Iterable)} or {@link #groupByColumnIds(Iterable)}.
   */
  @NonNull
  Select groupBy(@NonNull Iterable<Selector> selectors);

  /** Var-arg equivalent of {@link #groupBy(Iterable)}. */
  @NonNull
  default Select groupBy(@NonNull Selector... selectors) {
    return groupBy(Arrays.asList(selectors));
  }

  /**
   * Shortcut for {@link #groupBy(Iterable)} where all the clauses are simple columns. The arguments
   * are wrapped with {@link Selector#column(CqlIdentifier)}.
   */
  @NonNull
  default Select groupByColumnIds(@NonNull Iterable<CqlIdentifier> columnIds) {
    return groupBy(Iterables.transform(columnIds, Selector::column));
  }

  /** Var-arg equivalent of {@link #groupByColumnIds(Iterable)}. */
  @NonNull
  default Select groupByColumnIds(@NonNull CqlIdentifier... columnIds) {
    return groupByColumnIds(Arrays.asList(columnIds));
  }

  /**
   * Shortcut for {@link #groupBy(Iterable)} where all the clauses are simple columns. The arguments
   * are wrapped with {@link Selector#column(String)}.
   */
  @NonNull
  default Select groupByColumns(@NonNull Iterable<String> columnNames) {
    return groupBy(Iterables.transform(columnNames, Selector::column));
  }

  /** Var-arg equivalent of {@link #groupByColumns(Iterable)}. */
  @NonNull
  default Select groupByColumns(@NonNull String... columnNames) {
    return groupByColumns(Arrays.asList(columnNames));
  }

  /**
   * Adds the provided GROUP BY clause to the query.
   *
   * <p>As of version 4.0, Apache Cassandra only allows grouping by columns, therefore you can use
   * the shortcuts {@link #groupBy(String)} or {@link #groupBy(CqlIdentifier)}.
   */
  @NonNull
  Select groupBy(@NonNull Selector selector);

  /** Shortcut for {@link #groupBy(Selector) groupBy(Selector.column(columnId))}. */
  @NonNull
  default Select groupBy(@NonNull CqlIdentifier columnId) {
    return groupBy(Selector.column(columnId));
  }

  /** Shortcut for {@link #groupBy(Selector) groupBy(Selector.column(columnName))}. */
  @NonNull
  default Select groupBy(@NonNull String columnName) {
    return groupBy(Selector.column(columnName));
  }

  /**
   * Adds the provided ORDER BY clauses to the query.
   *
   * <p>They will be appended in the iteration order of the provided map. If an ordering was already
   * defined for a given identifier, it will be removed and the new ordering will appear in its
   * position in the provided map.
   */
  @NonNull
  Select orderByIds(@NonNull Map<CqlIdentifier, ClusteringOrder> orderings);

  /**
   * Shortcut for {@link #orderByIds(Map)} with the columns specified as case-insensitive names.
   * They will be wrapped with {@link CqlIdentifier#fromCql(String)}.
   *
   * <p>Note that it's possible for two different case-insensitive names to resolve to the same
   * identifier, for example "foo" and "Foo"; if this happens, a runtime exception will be thrown.
   *
   * @throws IllegalArgumentException if two names resolve to the same identifier.
   */
  @NonNull
  default Select orderBy(@NonNull Map<String, ClusteringOrder> orderings) {
    return orderByIds(CqlIdentifiers.wrapKeys(orderings));
  }

  /**
   * Adds the provided ORDER BY clause to the query.
   *
   * <p>If an ordering was already defined for this identifier, it will be removed and the new
   * clause will be appended at the end of the current list for this query.
   */
  @NonNull
  Select orderBy(@NonNull CqlIdentifier columnId, @NonNull ClusteringOrder order);

  /**
   * Shortcut for {@link #orderBy(CqlIdentifier, ClusteringOrder)
   * orderBy(CqlIdentifier.fromCql(columnName), order)}.
   */
  @NonNull
  default Select orderBy(@NonNull String columnName, @NonNull ClusteringOrder order) {
    return orderBy(CqlIdentifier.fromCql(columnName), order);
  }

  /**
   * Adds a LIMIT clause to this query with a literal value.
   *
   * <p>If this method or {@link #limit(BindMarker)} is called multiple times, the last value is
   * used.
   */
  @NonNull
  Select limit(int limit);

  /**
   * Adds a LIMIT clause to this query with a bind marker.
   *
   * <p>To create the argument, use one of the factory methods in {@link QueryBuilder}, for example
   * {@link QueryBuilder#bindMarker() bindMarker()}.
   *
   * <p>If this method or {@link #limit(int)} is called multiple times, the last value is used.
   * {@code null} can be passed to cancel a previous limit.
   */
  @NonNull
  Select limit(@Nullable BindMarker bindMarker);

  /**
   * Adds a PER PARTITION LIMIT clause to this query with a literal value.
   *
   * <p>If this method or {@link #perPartitionLimit(BindMarker)} is called multiple times, the last
   * value is used.
   */
  @NonNull
  Select perPartitionLimit(int limit);

  /**
   * Adds a PER PARTITION LIMIT clause to this query with a bind marker.
   *
   * <p>To create the argument, use one of the factory methods in {@link QueryBuilder}, for example
   * {@link QueryBuilder#bindMarker() bindMarker()}.
   *
   * <p>If this method or {@link #perPartitionLimit(int)} is called multiple times, the last value
   * is used. {@code null} can be passed to cancel a previous limit.
   */
  @NonNull
  Select perPartitionLimit(@Nullable BindMarker bindMarker);

  /**
   * Adds an ALLOW FILTERING clause to this query.
   *
   * <p>This method is idempotent, calling it multiple times will only add a single clause.
   */
  @NonNull
  Select allowFiltering();

  /**
   * Adds a {@code USING TIMEOUT} clause to this statement with a literal value. Setting a value of
   * {@code null} will remove the {@code USING TIMEOUT} clause on this statement.
   *
   * <p>If this method or {@link #usingTimeout(BindMarker) } is called multiple times, the value
   * from the last invocation is used.
   *
   * @param timeout A timeout value controlling server-side query timeout.
   */
  @NonNull
  Select usingTimeout(@NonNull CqlDuration timeout);

  /**
   * Adds a {@code USING TIMEOUT} clause to this statement with a bind marker. Setting a value of
   * {@code null} will remove the {@code USING TIMEOUT} clause on this statement.
   *
   * <p>If this method or {@link #usingTimeout(CqlDuration) } is called multiple times, the value
   * from the last invocation is used.
   *
   * @param timeout A bind marker understood as {@link CqlDuration} controlling server-side query
   *     timeout.
   */
  @NonNull
  Select usingTimeout(@NonNull BindMarker timeout);

  /**
   * Adds an BYPASS CACHE clause to this query.
   *
   * <p>This method is idempotent, calling it multiple times will only add a single clause.
   */
  @NonNull
  Select bypassCache();
}
