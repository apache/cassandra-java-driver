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
package com.datastax.oss.driver.api.querybuilder.relation;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.core.CqlIdentifiers;
import com.datastax.oss.driver.internal.querybuilder.DefaultRaw;
import com.datastax.oss.driver.internal.querybuilder.relation.CustomIndexRelation;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultColumnComponentRelationBuilder;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultColumnRelationBuilder;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultMultiColumnRelationBuilder;
import com.datastax.oss.driver.internal.querybuilder.relation.DefaultTokenRelationBuilder;
import edu.umd.cs.findbugs.annotations.CheckReturnValue;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Arrays;

/** A statement that is ready to accept relations in its WHERE clause. */
public interface OngoingWhereClause<SelfT extends OngoingWhereClause<SelfT>> {

  /**
   * Adds a relation in the WHERE clause. All relations are logically joined with AND.
   *
   * <p>To create the argument, use one of the factory methods in {@link Relation}, for example
   * {@link Relation#column(CqlIdentifier) column}.
   *
   * <p>If you add multiple selectors as once, consider {@link #where(Iterable)} as a more efficient
   * alternative.
   */
  @NonNull
  @CheckReturnValue
  SelfT where(@NonNull Relation relation);

  /**
   * Adds multiple relations at once. All relations are logically joined with AND.
   *
   * <p>This is slightly more efficient than adding the relations one by one (since the underlying
   * implementation of this object is immutable).
   *
   * <p>To create the arguments, use one of the factory methods in {@link Relation}, for example
   * {@link Relation#column(CqlIdentifier) column}.
   *
   * @see #where(Relation)
   */
  @NonNull
  @CheckReturnValue
  SelfT where(@NonNull Iterable<Relation> additionalRelations);

  /** Var-arg equivalent of {@link #where(Iterable)}. */
  @NonNull
  @CheckReturnValue
  default SelfT where(@NonNull Relation... additionalRelations) {
    return where(Arrays.asList(additionalRelations));
  }

  /**
   * Adds a relation testing a column.
   *
   * <p>This must be chained with an operator call, for example:
   *
   * <pre>{@code
   * selectFrom("foo").all().whereColumn("k").isEqualTo(bindMarker());
   * }</pre>
   *
   * This is the equivalent of creating a relation with {@link Relation#column(CqlIdentifier)} and
   * passing it to {@link #where(Relation)}.
   */
  @NonNull
  default ColumnRelationBuilder<SelfT> whereColumn(@NonNull CqlIdentifier id) {
    return new DefaultColumnRelationBuilder.Fluent<>(this, id);
  }

  /**
   * Shortcut for {@link #whereColumn(CqlIdentifier) whereColumn(CqlIdentifier.fromCql(name))}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#column(String)} and
   * passing it to {@link #where(Relation)}.
   */
  @NonNull
  default ColumnRelationBuilder<SelfT> whereColumn(@NonNull String name) {
    return whereColumn(CqlIdentifier.fromCql(name));
  }

  /**
   * Adds a relation testing a value in a map (Cassandra 4 and above).
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#mapValue(CqlIdentifier,
   * Term)} and passing it to {@link #where(Relation)}.
   */
  @NonNull
  default ColumnComponentRelationBuilder<SelfT> whereMapValue(
      @NonNull CqlIdentifier columnId, @NonNull Term index) {
    return new DefaultColumnComponentRelationBuilder.Fluent<>(this, columnId, index);
  }

  /**
   * Shortcut for {@link #whereMapValue(CqlIdentifier, Term)
   * whereMapValue(CqlIdentifier.fromCql(columnName), index)}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#mapValue(String, Term)}
   * and passing it to {@link #where(Relation)}.
   */
  @NonNull
  default ColumnComponentRelationBuilder<SelfT> whereMapValue(
      @NonNull String columnName, @NonNull Term index) {
    return whereMapValue(CqlIdentifier.fromCql(columnName), index);
  }

  /**
   * Adds a relation testing a token generated from a set of columns.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#tokenFromIds(Iterable)}
   * and passing it to {@link #where(Relation)}.
   */
  @NonNull
  default TokenRelationBuilder<SelfT> whereTokenFromIds(
      @NonNull Iterable<CqlIdentifier> identifiers) {
    return new DefaultTokenRelationBuilder.Fluent<>(this, identifiers);
  }

  /**
   * Var-arg equivalent of {@link #whereTokenFromIds(Iterable)}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#token(CqlIdentifier...)}
   * and passing it to {@link #where(Relation)}.
   */
  @NonNull
  default TokenRelationBuilder<SelfT> whereToken(@NonNull CqlIdentifier... identifiers) {
    return whereTokenFromIds(Arrays.asList(identifiers));
  }

  /**
   * Equivalent of {@link #whereTokenFromIds(Iterable)} with raw strings; the names are converted
   * with {@link CqlIdentifier#fromCql(String)}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#token(Iterable)} and
   * passing it to {@link #where(Relation)}.
   */
  @NonNull
  default TokenRelationBuilder<SelfT> whereToken(@NonNull Iterable<String> names) {
    return whereTokenFromIds(CqlIdentifiers.wrap(names));
  }

  /**
   * Var-arg equivalent of {@link #whereToken(Iterable)}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#token(String...)} and
   * passing it to {@link #where(Relation)}.
   */
  @NonNull
  default TokenRelationBuilder<SelfT> whereToken(@NonNull String... names) {
    return whereToken(Arrays.asList(names));
  }

  /**
   * Adds a multi-column relation, as in {@code WHERE (c1, c2, c3) IN ...}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#columnIds(Iterable)} and
   * passing it to {@link #where(Relation)}.
   */
  @NonNull
  default MultiColumnRelationBuilder<SelfT> whereColumnIds(
      @NonNull Iterable<CqlIdentifier> identifiers) {
    return new DefaultMultiColumnRelationBuilder.Fluent<>(this, identifiers);
  }

  /**
   * Var-arg equivalent of {@link #whereColumnIds(Iterable)}.
   *
   * <p>This is the equivalent of creating a relation with {@link
   * Relation#columns(CqlIdentifier...)} and passing it to {@link #where(Relation)}.
   */
  @NonNull
  default MultiColumnRelationBuilder<SelfT> whereColumns(@NonNull CqlIdentifier... identifiers) {
    return whereColumnIds(Arrays.asList(identifiers));
  }

  /**
   * Equivalent of {@link #whereColumnIds(Iterable)} with raw strings; the names are converted with
   * {@link CqlIdentifier#fromCql(String)}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#columns(Iterable)} and
   * passing it to {@link #where(Relation)}.
   */
  @NonNull
  default MultiColumnRelationBuilder<SelfT> whereColumns(@NonNull Iterable<String> names) {
    return whereColumnIds(CqlIdentifiers.wrap(names));
  }

  /**
   * Var-arg equivalent of {@link #whereColumns(Iterable)}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#columns(String...)} and
   * passing it to {@link #where(Relation)}.
   */
  @NonNull
  default MultiColumnRelationBuilder<SelfT> whereColumns(@NonNull String... names) {
    return whereColumns(Arrays.asList(names));
  }

  /**
   * Adds a relation on a custom index.
   *
   * <p>This is the equivalent of creating a relation with {@link
   * Relation#customIndex(CqlIdentifier, Term)} and passing it to {@link #where(Relation)}.
   */
  @NonNull
  @CheckReturnValue
  default SelfT whereCustomIndex(@NonNull CqlIdentifier indexId, @NonNull Term expression) {
    return where(new CustomIndexRelation(indexId, expression));
  }

  /**
   * Shortcut for {@link #whereCustomIndex(CqlIdentifier, Term)
   * whereCustomIndex(CqlIdentifier.fromCql(indexName), expression)}.
   *
   * <p>This is the equivalent of creating a relation with {@link Relation#customIndex(String,
   * Term)} and passing it to {@link #where(Relation)}.
   */
  @NonNull
  @CheckReturnValue
  default SelfT whereCustomIndex(@NonNull String indexName, @NonNull Term expression) {
    return whereCustomIndex(CqlIdentifier.fromCql(indexName), expression);
  }

  /**
   * Adds a raw CQL snippet as a relation.
   *
   * <p>This is the equivalent of creating a relation with {@link QueryBuilder#raw(String)} and
   * passing it to {@link #where(Relation)}.
   *
   * <p>The contents will be appended to the query as-is, without any syntax checking or escaping.
   * This method should be used with caution, as it's possible to generate invalid CQL that will
   * fail at execution time; on the other hand, it can be used as a workaround to handle new CQL
   * features that are not yet covered by the query builder.
   */
  @NonNull
  @CheckReturnValue
  default SelfT whereRaw(@NonNull String raw) {
    return where(new DefaultRaw(raw));
  }
}
