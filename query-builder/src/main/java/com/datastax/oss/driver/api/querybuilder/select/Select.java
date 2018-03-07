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
package com.datastax.oss.driver.api.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
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
  Select groupBy(Iterable<Selector> selectors);

  /** Var-arg equivalent of {@link #groupBy(Iterable)}. */
  default Select groupBy(Selector... selectors) {
    return groupBy(Arrays.asList(selectors));
  }

  /**
   * Shortcut for {@link #groupBy(Iterable)} where all the clauses are simple columns. The arguments
   * are wrapped with {@link Selector#column(CqlIdentifier)}.
   */
  default Select groupByColumnIds(Iterable<CqlIdentifier> columnIds) {
    return groupBy(Iterables.transform(columnIds, Selector::column));
  }

  /** Var-arg equivalent of {@link #groupByColumnIds(Iterable)}. */
  default Select groupByColumnIds(CqlIdentifier... columnIds) {
    return groupByColumnIds(Arrays.asList(columnIds));
  }

  /**
   * Shortcut for {@link #groupBy(Iterable)} where all the clauses are simple columns. The arguments
   * are wrapped with {@link Selector#column(String)}.
   */
  default Select groupByColumns(Iterable<String> columnNames) {
    return groupBy(Iterables.transform(columnNames, Selector::column));
  }

  /** Var-arg equivalent of {@link #groupByColumns(Iterable)}. */
  default Select groupByColumns(String... columnNames) {
    return groupByColumns(Arrays.asList(columnNames));
  }

  /**
   * Adds the provided GROUP BY clause to the query.
   *
   * <p>As of version 4.0, Apache Cassandra only allows grouping by columns, therefore you can use
   * the shortcuts {@link #groupBy(String)} or {@link #groupBy(CqlIdentifier)}.
   */
  Select groupBy(Selector selector);

  /** Shortcut for {@link #groupBy(Selector) groupBy(Selector.column(columnId))}. */
  default Select groupBy(CqlIdentifier columnId) {
    return groupBy(Selector.column(columnId));
  }

  /** Shortcut for {@link #groupBy(Selector) groupBy(Selector.column(columnName))}. */
  default Select groupBy(String columnName) {
    return groupBy(Selector.column(columnName));
  }

  /**
   * Adds the provided ORDER BY clauses to the query.
   *
   * <p>They will be appended in the iteration order of the provided map. If an ordering was already
   * defined for a given identifier, it will be removed and the new ordering will appear in its
   * position in the provided map.
   */
  Select orderByIds(Map<CqlIdentifier, ClusteringOrder> orderings);

  /**
   * Shortcut for {@link #orderByIds(Map)} with the columns specified as case-insensitive names.
   * They will be wrapped with {@link CqlIdentifier#fromCql(String)}.
   *
   * <p>Note that it's possible for two different case-insensitive names to resolve to the same
   * identifier, for example "foo" and "Foo"; if this happens, a runtime exception will be thrown.
   *
   * @throws IllegalArgumentException if two names resolve to the same identifier.
   */
  default Select orderBy(Map<String, ClusteringOrder> orderings) {
    ImmutableMap.Builder<CqlIdentifier, ClusteringOrder> builder = ImmutableMap.builder();
    for (Map.Entry<String, ClusteringOrder> entry : orderings.entrySet()) {
      builder.put(CqlIdentifier.fromCql(entry.getKey()), entry.getValue());
    }
    // build() throws if there are duplicate keys
    return orderByIds(builder.build());
  }

  /**
   * Adds the provided ORDER BY clause to the query.
   *
   * <p>If an ordering was already defined for this identifier, it will be removed and the new
   * clause will be appended at the end of the current list for this query.
   */
  Select orderBy(CqlIdentifier columnId, ClusteringOrder order);

  /**
   * Shortcut for {@link #orderBy(CqlIdentifier, ClusteringOrder)
   * orderBy(CqlIdentifier.fromCql(columnName), order)}.
   */
  default Select orderBy(String columnName, ClusteringOrder order) {
    return orderBy(CqlIdentifier.fromCql(columnName), order);
  }

  /**
   * Adds a LIMIT clause to this query with a literal value.
   *
   * <p>If this method or {@link #limit(BindMarker)} is called multiple times, the last value is
   * used.
   */
  Select limit(int limit);

  /**
   * Adds a LIMIT clause to this query with a bind marker.
   *
   * <p>To create the argument, use one of the factory methods in {@link QueryBuilderDsl}, for
   * example {@link QueryBuilderDsl#bindMarker() bindMarker()}.
   *
   * <p>If this method or {@link #limit(int)} is called multiple times, the last value is used.
   */
  Select limit(BindMarker bindMarker);

  /**
   * Adds a PER PARTITION LIMIT clause to this query with a literal value.
   *
   * <p>If this method or {@link #perPartitionLimit(BindMarker)} is called multiple times, the last
   * value is used.
   */
  Select perPartitionLimit(int limit);

  /**
   * Adds a PER PARTITION LIMIT clause to this query with a bind marker.
   *
   * <p>To create the argument, use one of the factory methods in {@link QueryBuilderDsl}, for
   * example {@link QueryBuilderDsl#bindMarker() bindMarker()}.
   *
   * <p>If this method or {@link #perPartitionLimit(int)} is called multiple times, the last value
   * is used.
   */
  Select perPartitionLimit(BindMarker bindMarker);

  /**
   * Adds an ALLOW FILTERING clause to this query.
   *
   * <p>This method is idempotent, calling it multiple times will only add a single clause.
   */
  Select allowFiltering();
}
