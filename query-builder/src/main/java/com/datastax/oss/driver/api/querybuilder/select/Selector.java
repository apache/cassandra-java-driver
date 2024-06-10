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
package com.datastax.oss.driver.api.querybuilder.select;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.ArithmeticOperator;
import com.datastax.oss.driver.internal.querybuilder.select.AllSelector;
import com.datastax.oss.driver.internal.querybuilder.select.BinaryArithmeticSelector;
import com.datastax.oss.driver.internal.querybuilder.select.CastSelector;
import com.datastax.oss.driver.internal.querybuilder.select.ColumnSelector;
import com.datastax.oss.driver.internal.querybuilder.select.CountAllSelector;
import com.datastax.oss.driver.internal.querybuilder.select.ElementSelector;
import com.datastax.oss.driver.internal.querybuilder.select.FieldSelector;
import com.datastax.oss.driver.internal.querybuilder.select.FunctionSelector;
import com.datastax.oss.driver.internal.querybuilder.select.ListSelector;
import com.datastax.oss.driver.internal.querybuilder.select.MapSelector;
import com.datastax.oss.driver.internal.querybuilder.select.OppositeSelector;
import com.datastax.oss.driver.internal.querybuilder.select.RangeSelector;
import com.datastax.oss.driver.internal.querybuilder.select.SetSelector;
import com.datastax.oss.driver.internal.querybuilder.select.TupleSelector;
import com.datastax.oss.driver.internal.querybuilder.select.TypeHintSelector;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Arrays;
import java.util.Map;

/**
 * A selected element in a SELECT query.
 *
 * <p>To build instances of this type, use the factory methods, such as {@link
 * #column(CqlIdentifier) column}, {@link #function(CqlIdentifier, Iterable) function}, etc.
 *
 * <p>They are used as arguments to the {@link OngoingSelection#selectors(Iterable) selectors}
 * method, for example:
 *
 * <pre>{@code
 * selectFrom("foo").selectors(Selector.column("bar"), Selector.column("baz"))
 * // SELECT bar,baz FROM foo
 * }</pre>
 *
 * <p>There are also shortcuts in the fluent API when you build a statement, for example:
 *
 * <pre>{@code
 * selectFrom("foo").column("bar").column("baz")
 * // SELECT bar,baz FROM foo
 * }</pre>
 */
public interface Selector extends CqlSnippet {

  /** Selects all columns, as in {@code SELECT *}. */
  @NonNull
  static Selector all() {
    return AllSelector.INSTANCE;
  }

  /** Selects the count of all returned rows, as in {@code SELECT count(*)}. */
  @NonNull
  static Selector countAll() {
    return new CountAllSelector();
  }

  /** Selects a particular column by its CQL identifier. */
  @NonNull
  static Selector column(@NonNull CqlIdentifier columnId) {
    return new ColumnSelector(columnId);
  }

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))} */
  @NonNull
  static Selector column(@NonNull String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Selects the sum of two arguments, as in {@code SELECT col1 + col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   */
  @NonNull
  static Selector add(@NonNull Selector left, @NonNull Selector right) {
    return new BinaryArithmeticSelector(ArithmeticOperator.SUM, left, right);
  }

  /**
   * Selects the difference of two arguments, as in {@code SELECT col1 - col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   */
  @NonNull
  static Selector subtract(@NonNull Selector left, @NonNull Selector right) {
    return new BinaryArithmeticSelector(ArithmeticOperator.DIFFERENCE, left, right);
  }

  /**
   * Selects the product of two arguments, as in {@code SELECT col1 * col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>The arguments will be parenthesized if they are instances of {@link #add} or {@link
   * #subtract}. If they are raw selectors, you might have to parenthesize them yourself.
   */
  @NonNull
  static Selector multiply(@NonNull Selector left, @NonNull Selector right) {
    return new BinaryArithmeticSelector(ArithmeticOperator.PRODUCT, left, right);
  }

  /**
   * Selects the quotient of two arguments, as in {@code SELECT col1 / col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>The arguments will be parenthesized if they are instances of {@link #add} or {@link
   * #subtract}. If they are raw selectors, you might have to parenthesize them yourself.
   */
  @NonNull
  static Selector divide(@NonNull Selector left, @NonNull Selector right) {
    return new BinaryArithmeticSelector(ArithmeticOperator.QUOTIENT, left, right);
  }

  /**
   * Selects the remainder of two arguments, as in {@code SELECT col1 % col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>The arguments will be parenthesized if they are instances of {@link #add} or {@link
   * #subtract}. If they are raw selectors, you might have to parenthesize them yourself.
   */
  @NonNull
  static Selector remainder(@NonNull Selector left, @NonNull Selector right) {
    return new BinaryArithmeticSelector(ArithmeticOperator.REMAINDER, left, right);
  }

  /**
   * Selects the opposite of an argument, as in {@code SELECT -col1}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>The argument will be parenthesized if it is an instance of {@link #add} or {@link
   * #subtract}. If it is a raw selector, you might have to parenthesize it yourself.
   */
  @NonNull
  static Selector negate(@NonNull Selector argument) {
    return new OppositeSelector(argument);
  }

  /** Selects a field inside of a UDT column, as in {@code SELECT user.name}. */
  @NonNull
  static Selector field(@NonNull Selector udt, @NonNull CqlIdentifier fieldId) {
    return new FieldSelector(udt, fieldId);
  }

  /**
   * Shortcut for {@link #field(Selector, CqlIdentifier) getUdtField(udt,
   * CqlIdentifier.fromCql(fieldName))}.
   */
  @NonNull
  static Selector field(@NonNull Selector udt, @NonNull String fieldName) {
    return field(udt, CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Shortcut to select a UDT field when the UDT is a simple column (as opposed to a more complex
   * selection, like a nested UDT).
   */
  @NonNull
  static Selector field(@NonNull CqlIdentifier udtColumnId, @NonNull CqlIdentifier fieldId) {
    return field(column(udtColumnId), fieldId);
  }

  /**
   * Shortcut for {@link #field(CqlIdentifier, CqlIdentifier)
   * field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName))}.
   */
  @NonNull
  static Selector field(@NonNull String udtColumnName, @NonNull String fieldName) {
    return field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Selects an element in a collection column, as in {@code SELECT m['key']}.
   *
   * <p>As of Cassandra 4, this is only allowed in SELECT for map and set columns. DELETE accepts
   * list elements as well.
   */
  @NonNull
  static Selector element(@NonNull Selector collection, @NonNull Term index) {
    return new ElementSelector(collection, index);
  }

  /**
   * Shortcut for element selection when the target collection is a simple column.
   *
   * <p>In other words, this is the equivalent of {@link #element(Selector, Term)
   * element(column(collectionId), index)}.
   */
  @NonNull
  static Selector element(@NonNull CqlIdentifier collectionId, @NonNull Term index) {
    return element(column(collectionId), index);
  }

  /**
   * Shortcut for {@link #element(CqlIdentifier, Term)
   * element(CqlIdentifier.fromCql(collectionName), index)}.
   */
  @NonNull
  static Selector element(@NonNull String collectionName, @NonNull Term index) {
    return element(CqlIdentifier.fromCql(collectionName), index);
  }

  /**
   * Selects a slice in a collection column, as in {@code SELECT s[4..8]}.
   *
   * <p>As of Cassandra 4, this is only allowed for set and map columns. Those collections are
   * ordered, the elements (or keys in the case of a map), will be compared to the bounds for
   * inclusions. Either bound can be unspecified, but not both.
   *
   * @param left the left bound (inclusive). Can be {@code null} to indicate that the slice is only
   *     right-bound.
   * @param right the right bound (inclusive). Can be {@code null} to indicate that the slice is
   *     only left-bound.
   */
  @NonNull
  static Selector range(@NonNull Selector collection, @Nullable Term left, @Nullable Term right) {
    return new RangeSelector(collection, left, right);
  }

  /**
   * Shortcut for slice selection when the target collection is a simple column.
   *
   * <p>In other words, this is the equivalent of {@link #range(Selector, Term, Term)}
   * range(column(collectionId), left, right)}.
   */
  @NonNull
  static Selector range(
      @NonNull CqlIdentifier collectionId, @Nullable Term left, @Nullable Term right) {
    return range(column(collectionId), left, right);
  }

  /**
   * Shortcut for {@link #range(CqlIdentifier, Term, Term)
   * range(CqlIdentifier.fromCql(collectionName), left, right)}.
   */
  @NonNull
  static Selector range(@NonNull String collectionName, @Nullable Term left, @Nullable Term right) {
    return range(CqlIdentifier.fromCql(collectionName), left, right);
  }

  /**
   * Selects a group of elements as a list, as in {@code SELECT [a,b,c]}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime), and they
   * should all produce the same data type (the query builder can't check this, so the query will
   * fail at execution time).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  @NonNull
  static Selector listOf(@NonNull Iterable<Selector> elementSelectors) {
    return new ListSelector(elementSelectors);
  }

  /** Var-arg equivalent of {@link #listOf(Iterable)}. */
  @NonNull
  static Selector listOf(@NonNull Selector... elementSelectors) {
    return listOf(Arrays.asList(elementSelectors));
  }

  /**
   * Selects a group of elements as a set, as in {@code SELECT {a,b,c}}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime), and they
   * should all produce the same data type (the query builder can't check this, so the query will
   * fail at execution time).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  @NonNull
  static Selector setOf(@NonNull Iterable<Selector> elementSelectors) {
    return new SetSelector(elementSelectors);
  }

  /** Var-arg equivalent of {@link #setOf(Iterable)}. */
  @NonNull
  static Selector setOf(@NonNull Selector... elementSelectors) {
    return setOf(Arrays.asList(elementSelectors));
  }

  /**
   * Selects a group of elements as a tuple, as in {@code SELECT (a,b,c)}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  @NonNull
  static Selector tupleOf(@NonNull Iterable<Selector> elementSelectors) {
    return new TupleSelector(elementSelectors);
  }

  /** Var-arg equivalent of {@link #tupleOf(Iterable)}. */
  @NonNull
  static Selector tupleOf(@NonNull Selector... elementSelectors) {
    return tupleOf(Arrays.asList(elementSelectors));
  }

  /**
   * Selects a group of elements as a map, as in {@code SELECT {a:b,c:d}}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime). In
   * addition, all key selectors should produce the same type, and all value selectors as well (the
   * key and value types can be different); the query builder can't check this, so the query will
   * fail at execution time if the types are not uniform.
   *
   * <p>Note that Cassandra often has trouble inferring the exact map type. This will manifest as
   * the error message:
   *
   * <pre>
   *   Cannot infer type for term xxx in selection clause (try using a cast to force a type)
   * </pre>
   *
   * If you run into this, consider providing the types explicitly with {@link #mapOf(Map, DataType,
   * DataType)}.
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  @NonNull
  static Selector mapOf(@NonNull Map<Selector, Selector> elementSelectors) {
    return mapOf(elementSelectors, null, null);
  }

  /**
   * Selects a group of elements as a map and force the resulting map type, as in {@code SELECT
   * (map<int,text>){a:b,c:d}}.
   *
   * <p>To create the data types, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilder#udt(CqlIdentifier)}.
   *
   * @see #mapOf(Map)
   */
  @NonNull
  static Selector mapOf(
      @NonNull Map<Selector, Selector> elementSelectors,
      @Nullable DataType keyType,
      @Nullable DataType valueType) {
    return new MapSelector(elementSelectors, keyType, valueType);
  }

  /**
   * Provides a type hint for a selector, as in {@code SELECT (double)1/3}.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilder#udt(CqlIdentifier)}.
   */
  @NonNull
  static Selector typeHint(@NonNull Selector selector, @NonNull DataType targetType) {
    return new TypeHintSelector(selector, targetType);
  }

  /**
   * Selects the result of a function call, as is {@code SELECT f(a,b)}
   *
   * <p>None of the arguments should be aliased (the query builder checks this at runtime).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  @NonNull
  static Selector function(
      @NonNull CqlIdentifier functionId, @NonNull Iterable<Selector> arguments) {
    return new FunctionSelector(null, functionId, arguments);
  }

  /** Var-arg equivalent of {@link #function(CqlIdentifier, Iterable)}. */
  @NonNull
  static Selector function(@NonNull CqlIdentifier functionId, @NonNull Selector... arguments) {
    return function(functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, Iterable)
   * function(CqlIdentifier.fromCql(functionName), arguments)}.
   */
  @NonNull
  static Selector function(@NonNull String functionName, @NonNull Iterable<Selector> arguments) {
    return function(CqlIdentifier.fromCql(functionName), arguments);
  }

  /** Var-arg equivalent of {@link #function(String, Iterable)}. */
  @NonNull
  static Selector function(@NonNull String functionName, @NonNull Selector... arguments) {
    return function(functionName, Arrays.asList(arguments));
  }

  /**
   * Selects the result of a function call, as is {@code SELECT ks.f(a,b)}
   *
   * <p>None of the arguments should be aliased (the query builder checks this at runtime).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  @NonNull
  static Selector function(
      @Nullable CqlIdentifier keyspaceId,
      @NonNull CqlIdentifier functionId,
      @NonNull Iterable<Selector> arguments) {
    return new FunctionSelector(keyspaceId, functionId, arguments);
  }

  /** Var-arg equivalent of {@link #function(CqlIdentifier, CqlIdentifier, Iterable)}. */
  @NonNull
  static Selector function(
      @Nullable CqlIdentifier keyspaceId,
      @NonNull CqlIdentifier functionId,
      @NonNull Selector... arguments) {
    return function(keyspaceId, functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, CqlIdentifier, Iterable)}
   * function(CqlIdentifier.fromCql(functionName), arguments)}.
   */
  @NonNull
  static Selector function(
      @Nullable String keyspaceName,
      @NonNull String functionName,
      @NonNull Iterable<Selector> arguments) {
    return function(
        keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName),
        CqlIdentifier.fromCql(functionName),
        arguments);
  }

  /** Var-arg equivalent of {@link #function(String, String, Iterable)}. */
  @NonNull
  static Selector function(
      @Nullable String keyspaceName, @NonNull String functionName, @NonNull Selector... arguments) {
    return function(keyspaceName, functionName, Arrays.asList(arguments));
  }

  /**
   * Shortcut to select the result of the built-in {@code writetime} function, as in {@code SELECT
   * writetime(c)}.
   */
  @NonNull
  static Selector writeTime(@NonNull CqlIdentifier columnId) {
    return function("writetime", column(columnId));
  }

  /**
   * Shortcut for {@link #writeTime(CqlIdentifier) writeTime(CqlIdentifier.fromCql(columnName))}.
   */
  @NonNull
  static Selector writeTime(@NonNull String columnName) {
    return writeTime(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code ttl} function, as in {@code SELECT
   * ttl(c)}.
   */
  @NonNull
  static Selector ttl(@NonNull CqlIdentifier columnId) {
    return function("ttl", column(columnId));
  }

  /** Shortcut for {@link #ttl(CqlIdentifier) ttl(CqlIdentifier.fromCql(columnName))}. */
  @NonNull
  static Selector ttl(@NonNull String columnName) {
    return ttl(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Casts a selector to a type, as in {@code SELECT CAST(a AS double)}.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilder#udt(CqlIdentifier)}.
   *
   * @throws IllegalArgumentException if the selector is aliased.
   */
  @NonNull
  static Selector cast(@NonNull Selector selector, @NonNull DataType targetType) {
    return new CastSelector(selector, targetType);
  }

  /** Shortcut to select the result of the built-in {@code toDate} function on a simple column. */
  @NonNull
  static Selector toDate(@NonNull CqlIdentifier columnId) {
    return function("todate", Selector.column(columnId));
  }

  /** Shortcut for {@link #toDate(CqlIdentifier) toDate(CqlIdentifier.fromCql(columnName))}. */
  @NonNull
  static Selector toDate(@NonNull String columnName) {
    return toDate(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code toTimestamp} function on a simple column.
   */
  @NonNull
  static Selector toTimestamp(@NonNull CqlIdentifier columnId) {
    return function("totimestamp", Selector.column(columnId));
  }

  /**
   * Shortcut for {@link #toTimestamp(CqlIdentifier)
   * toTimestamp(CqlIdentifier.fromCql(columnName))}.
   */
  @NonNull
  static Selector toTimestamp(@NonNull String columnName) {
    return toTimestamp(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code toUnixTimestamp} function on a simple
   * column.
   */
  @NonNull
  static Selector toUnixTimestamp(@NonNull CqlIdentifier columnId) {
    return function("tounixtimestamp", Selector.column(columnId));
  }

  /**
   * Shortcut for {@link #toUnixTimestamp(CqlIdentifier)
   * toUnixTimestamp(CqlIdentifier.fromCql(columnName))}.
   */
  @NonNull
  static Selector toUnixTimestamp(@NonNull String columnName) {
    return toUnixTimestamp(CqlIdentifier.fromCql(columnName));
  }

  /** Aliases the selector, as in {@code SELECT count(*) AS total}. */
  @NonNull
  Selector as(@NonNull CqlIdentifier alias);

  /** Shortcut for {@link #as(CqlIdentifier) as(CqlIdentifier.fromCql(alias))} */
  @NonNull
  default Selector as(@NonNull String alias) {
    return as(CqlIdentifier.fromCql(alias));
  }

  /** @return null if the selector is not aliased. */
  @Nullable
  CqlIdentifier getAlias();
}
