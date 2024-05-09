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
package com.datastax.oss.driver.api.querybuilder.relation;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.BuildableQuery;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import com.datastax.oss.driver.internal.querybuilder.relation.CustomIndexRelation;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultColumnComponentRelationBuilder;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultColumnRelationBuilder;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultMultiColumnRelationBuilder;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultTokenRelationBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;

/**
 * A relation in a WHERE clause.
 *
 * <p>To build instances of this type, use the factory methods, such as {@link #column(String)
 * column}, {@link #token(String...) token}, etc.
 *
 * <p>They are used as arguments to the {@link OngoingWhereClause#where(Iterable) where} method, for
 * example:
 *
 * <pre>{@code
 * selectFrom("foo").all().where(Relation.column("k").isEqualTo(literal(1)))
 * // SELECT * FROM foo WHERE k=1
 * }</pre>
 *
 * There are also shortcuts in the fluent API when you build a statement, for example:
 *
 * <pre>{@code
 * selectFrom("foo").all().whereColumn("k").isEqualTo(literal(1))
 * // SELECT * FROM foo WHERE k=1
 * }</pre>
 */
public interface Relation extends CqlSnippet {

  /**
   * Builds a relation testing a column.
   *
   * <p>This must be chained with an operator call, for example:
   *
   * <pre>{@code
   * Relation r = Relation.column("k").isEqualTo(bindMarker());
   * }</pre>
   */
  @NonNull
  static ColumnRelationBuilder<Relation> column(@NonNull CqlIdentifier id) {
    return new DefaultColumnRelationBuilder(id);
  }

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(name))} */
  @NonNull
  static ColumnRelationBuilder<Relation> column(@NonNull String name) {
    return column(CqlIdentifier.fromCql(name));
  }

  /** Builds a relation testing a value in a map (Cassandra 4 and above). */
  @NonNull
  static ColumnComponentRelationBuilder<Relation> mapValue(
      @NonNull CqlIdentifier columnId, @NonNull Term index) {
    // The concept could easily be extended to list elements and tuple components, so use a generic
    // name internally, we'll add other shortcuts if necessary.
    return new DefaultColumnComponentRelationBuilder(columnId, index);
  }

  /**
   * Shortcut for {@link #mapValue(CqlIdentifier, Term) mapValue(CqlIdentifier.fromCql(columnName),
   * index)}
   */
  @NonNull
  static ColumnComponentRelationBuilder<Relation> mapValue(
      @NonNull String columnName, @NonNull Term index) {
    return mapValue(CqlIdentifier.fromCql(columnName), index);
  }

  /** Builds a relation testing a token generated from a set of columns. */
  @NonNull
  static TokenRelationBuilder<Relation> tokenFromIds(@NonNull Iterable<CqlIdentifier> identifiers) {
    return new DefaultTokenRelationBuilder(identifiers);
  }

  /** Var-arg equivalent of {@link #tokenFromIds(Iterable)}. */
  @NonNull
  static TokenRelationBuilder<Relation> token(@NonNull CqlIdentifier... identifiers) {
    return tokenFromIds(Arrays.asList(identifiers));
  }

  /**
   * Equivalent of {@link #tokenFromIds(Iterable)} with raw strings; the names are converted with
   * {@link CqlIdentifier#fromCql(String)}.
   */
  @NonNull
  static TokenRelationBuilder<Relation> token(@NonNull Iterable<String> names) {
    return tokenFromIds(CqlIdentifiers.wrap(names));
  }

  /** Var-arg equivalent of {@link #token(Iterable)}. */
  @NonNull
  static TokenRelationBuilder<Relation> token(@NonNull String... names) {
    return token(Arrays.asList(names));
  }

  /** Builds a multi-column relation, as in {@code WHERE (c1, c2, c3) IN ...}. */
  @NonNull
  static MultiColumnRelationBuilder<Relation> columnIds(
      @NonNull Iterable<CqlIdentifier> identifiers) {
    return new DefaultMultiColumnRelationBuilder(identifiers);
  }

  /** Var-arg equivalent of {@link #columnIds(Iterable)}. */
  @NonNull
  static MultiColumnRelationBuilder<Relation> columns(@NonNull CqlIdentifier... identifiers) {
    return columnIds(Arrays.asList(identifiers));
  }

  /**
   * Equivalent of {@link #columnIds(Iterable)} with raw strings; the names are converted with
   * {@link CqlIdentifier#fromCql(String)}.
   */
  @NonNull
  static MultiColumnRelationBuilder<Relation> columns(@NonNull Iterable<String> names) {
    return columnIds(CqlIdentifiers.wrap(names));
  }

  /** Var-arg equivalent of {@link #columns(Iterable)}. */
  @NonNull
  static MultiColumnRelationBuilder<Relation> columns(@NonNull String... names) {
    return columns(Arrays.asList(names));
  }

  /** Builds a relation on a custom index. */
  @NonNull
  static Relation customIndex(@NonNull CqlIdentifier indexId, @NonNull Term expression) {
    return new CustomIndexRelation(indexId, expression);
  }

  /**
   * Shortcut for {@link #customIndex(CqlIdentifier, Term)
   * customIndex(CqlIdentifier.fromCql(indexName), expression)}
   */
  @NonNull
  static Relation customIndex(@NonNull String indexName, @NonNull Term expression) {
    return customIndex(CqlIdentifier.fromCql(indexName), expression);
  }

  /**
   * Whether this relation is idempotent.
   *
   * <p>That is, whether it always selects the same rows when used multiple times. For example,
   * {@code WHERE c=1} is idempotent, {@code WHERE c=now()} isn't.
   *
   * <p>This is used internally by the query builder to compute the {@link Statement#isIdempotent()}
   * flag on the UPDATE and DELETE statements generated by {@link BuildableQuery#build()} (this is
   * not relevant for SELECT statement, which are always idempotent). If a term is ambiguous (for
   * example a raw snippet or a call to a user function in the right operands), the builder is
   * pessimistic and assumes the term is not idempotent.
   */
  boolean isIdempotent();
}
