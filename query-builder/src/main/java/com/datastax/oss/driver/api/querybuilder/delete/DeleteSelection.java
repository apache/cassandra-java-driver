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
package com.datastax.oss.driver.api.querybuilder.delete;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.OngoingWhereClause;
import com.datastax.oss.driver.api.querybuilder.select.Selector;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Arrays;

/**
 * An in-progress DELETE statement: it targets a table and optionally a list of columns to delete;
 * it needs at least one WHERE relation to become buildable.
 */
public interface DeleteSelection extends OngoingWhereClause<Delete> {

  /**
   * Adds a selector.
   *
   * <p>To create the argument, use one of the factory methods in {@link Selector}, for example
   * {@link Selector#column(CqlIdentifier) column}. This type also provides shortcuts to create and
   * add the selector in one call, for example {@link #column(CqlIdentifier)} for {@code
   * selector(column(...))}.
   *
   * <p>Note that the only valid arguments for DELETE are a column, a field in a UDT column (nested
   * UDTs are not supported), and an element in a collection column (nested collections are not
   * supported).
   *
   * <p>If you add multiple selectors as once, consider {@link #selectors(Iterable)} as a more
   * efficient alternative.
   */
  @NonNull
  DeleteSelection selector(@NonNull Selector selector);

  /**
   * Adds multiple selectors at once.
   *
   * <p>This is slightly more efficient than adding the selectors one by one (since the underlying
   * implementation of this object is immutable).
   *
   * <p>To create the arguments, use one of the factory methods in {@link Selector}, for example
   * {@link Selector#column(CqlIdentifier) column}.
   *
   * <p>Note that the only valid arguments for DELETE are a column, a field in a UDT column (nested
   * UDTs are not supported), and an element in a collection column (nested collections are not
   * supported).
   *
   * @see #selector(Selector)
   */
  @NonNull
  DeleteSelection selectors(@NonNull Iterable<Selector> additionalSelectors);

  /** Var-arg equivalent of {@link #selectors(Iterable)}. */
  @NonNull
  default DeleteSelection selectors(@NonNull Selector... additionalSelectors) {
    return selectors(Arrays.asList(additionalSelectors));
  }

  /**
   * Deletes a particular column by its CQL identifier.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.column(columnId))}.
   *
   * @see Selector#column(CqlIdentifier)
   */
  @NonNull
  default DeleteSelection column(@NonNull CqlIdentifier columnId) {
    return selector(Selector.column(columnId));
  }

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))} */
  @NonNull
  default DeleteSelection column(@NonNull String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Deletes a field inside of a UDT column, as in {@code DELETE user.name}.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.field(udtColumnId,
   * fieldId))}.
   *
   * @see Selector#field(CqlIdentifier, CqlIdentifier)
   */
  @NonNull
  default DeleteSelection field(
      @NonNull CqlIdentifier udtColumnId, @NonNull CqlIdentifier fieldId) {
    return selector(Selector.field(udtColumnId, fieldId));
  }

  /**
   * Shortcut for {@link #field(CqlIdentifier, CqlIdentifier)
   * field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName))}.
   *
   * @see Selector#field(String, String)
   */
  @NonNull
  default DeleteSelection field(@NonNull String udtColumnName, @NonNull String fieldName) {
    return field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Deletes an element in a collection column, as in {@code DELETE m['key']}.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.element(collectionId,
   * index))}.
   *
   * @see Selector#element(CqlIdentifier, Term)
   */
  @NonNull
  default DeleteSelection element(@NonNull CqlIdentifier collectionId, @NonNull Term index) {
    return selector(Selector.element(collectionId, index));
  }

  /**
   * Shortcut for {@link #element(CqlIdentifier, Term)
   * element(CqlIdentifier.fromCql(collectionName), index)}.
   *
   * @see Selector#element(String, Term)
   */
  @NonNull
  default DeleteSelection element(@NonNull String collectionName, @NonNull Term index) {
    return element(CqlIdentifier.fromCql(collectionName), index);
  }

  /**
   * Specifies an element to delete as a raw CQL snippet.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(QueryBuilder.raw(raw))}.
   *
   * <p>The contents will be appended to the query as-is, without any syntax checking or escaping.
   * This method should be used with caution, as it's possible to generate invalid CQL that will
   * fail at execution time; on the other hand, it can be used as a workaround to handle new CQL
   * features that are not yet covered by the query builder.
   *
   * @see QueryBuilder#raw(String)
   */
  @NonNull
  default DeleteSelection raw(@NonNull String raw) {
    return selector(QueryBuilder.raw(raw));
  }

  /**
   * Adds a USING TIMESTAMP clause to this statement with a literal value.
   *
   * <p>If this method or {@link #usingTimestamp(BindMarker)} is called multiple times, the last
   * value is used.
   */
  @NonNull
  DeleteSelection usingTimestamp(long timestamp);

  /**
   * Adds a USING TIMESTAMP clause to this statement with a bind marker.
   *
   * <p>If this method or {@link #usingTimestamp(long)} is called multiple times, the last value is
   * used. Passing {@code null} to this method removes any previous timestamp.
   */
  @NonNull
  DeleteSelection usingTimestamp(@Nullable BindMarker bindMarker);
}
