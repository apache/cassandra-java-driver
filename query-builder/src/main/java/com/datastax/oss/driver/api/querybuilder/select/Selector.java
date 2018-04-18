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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.CqlSnippet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl;
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
  static Selector all() {
    return AllSelector.INSTANCE;
  }

  /** Selects the count of all returned rows, as in {@code SELECT count(*)}. */
  static Selector countAll() {
    return new CountAllSelector();
  }

  /** Selects a particular column by its CQL identifier. */
  static Selector column(CqlIdentifier columnId) {
    return new ColumnSelector(columnId);
  }

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))} */
  static Selector column(String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Selects the sum of two arguments, as in {@code SELECT col1 + col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   */
  static Selector add(Selector left, Selector right) {
    return new BinaryArithmeticSelector(ArithmeticOperator.SUM, left, right);
  }

  /**
   * Selects the difference of two arguments, as in {@code SELECT col1 - col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   */
  static Selector subtract(Selector left, Selector right) {
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
  static Selector multiply(Selector left, Selector right) {
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
  static Selector divide(Selector left, Selector right) {
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
  static Selector remainder(Selector left, Selector right) {
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
  static Selector negate(Selector argument) {
    return new OppositeSelector(argument);
  }

  /** Selects a field inside of a UDT column, as in {@code SELECT user.name}. */
  static Selector field(Selector udt, CqlIdentifier fieldId) {
    return new FieldSelector(udt, fieldId);
  }

  /**
   * Shortcut for {@link #field(Selector, CqlIdentifier) getUdtField(udt,
   * CqlIdentifier.fromCql(fieldName))}.
   */
  static Selector field(Selector udt, String fieldName) {
    return field(udt, CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Shortcut to select a UDT field when the UDT is a simple column (as opposed to a more complex
   * selection, like a nested UDT).
   */
  static Selector field(CqlIdentifier udtColumnId, CqlIdentifier fieldId) {
    return field(column(udtColumnId), fieldId);
  }

  /**
   * Shortcut for {@link #field(CqlIdentifier, CqlIdentifier)
   * field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName))}.
   */
  static Selector field(String udtColumnName, String fieldName) {
    return field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Selects an element in a collection column, as in {@code SELECT m['key']}.
   *
   * <p>As of Cassandra 4, this is only allowed in SELECT for map and set columns. DELETE accepts
   * list elements as well.
   */
  static Selector element(Selector collection, Term index) {
    return new ElementSelector(collection, index);
  }

  /**
   * Shortcut for element selection when the target collection is a simple column.
   *
   * <p>In other words, this is the equivalent of {@link #element(Selector, Term)
   * element(column(collectionId), index)}.
   */
  static Selector element(CqlIdentifier collectionId, Term index) {
    return element(column(collectionId), index);
  }

  /**
   * Shortcut for {@link #element(CqlIdentifier, Term)
   * element(CqlIdentifier.fromCql(collectionName), index)}.
   */
  static Selector element(String collectionName, Term index) {
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
  static Selector range(Selector collection, Term left, Term right) {
    return new RangeSelector(collection, left, right);
  }

  /**
   * Shortcut for slice selection when the target collection is a simple column.
   *
   * <p>In other words, this is the equivalent of {@link #range(Selector, Term, Term)}
   * range(column(collectionId), left, right)}.
   */
  static Selector range(CqlIdentifier collectionId, Term left, Term right) {
    return range(column(collectionId), left, right);
  }

  /**
   * Shortcut for {@link #range(CqlIdentifier, Term, Term)
   * range(CqlIdentifier.fromCql(collectionName), left, right)}.
   */
  static Selector range(String collectionName, Term left, Term right) {
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
  static Selector listOf(Iterable<Selector> elementSelectors) {
    return new ListSelector(elementSelectors);
  }

  /** Var-arg equivalent of {@link #listOf(Iterable)}. */
  static Selector listOf(Selector... elementSelectors) {
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
  static Selector setOf(Iterable<Selector> elementSelectors) {
    return new SetSelector(elementSelectors);
  }

  /** Var-arg equivalent of {@link #setOf(Iterable)}. */
  static Selector setOf(Selector... elementSelectors) {
    return setOf(Arrays.asList(elementSelectors));
  }

  /**
   * Selects a group of elements as a tuple, as in {@code SELECT (a,b,c)}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  static Selector tupleOf(Iterable<Selector> elementSelectors) {
    return new TupleSelector(elementSelectors);
  }

  /** Var-arg equivalent of {@link #tupleOf(Iterable)}. */
  static Selector tupleOf(Selector... elementSelectors) {
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
  static Selector mapOf(Map<Selector, Selector> elementSelectors) {
    return mapOf(elementSelectors, null, null);
  }

  /**
   * Selects a group of elements as a map and force the resulting map type, as in {@code SELECT
   * (map<int,text>){a:b,c:d}}.
   *
   * <p>To create the data types, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilderDsl#udt(CqlIdentifier)}.
   *
   * @see #mapOf(Map)
   */
  static Selector mapOf(
      Map<Selector, Selector> elementSelectors, DataType keyType, DataType valueType) {
    return new MapSelector(elementSelectors, keyType, valueType);
  }

  /**
   * Provides a type hint for a selector, as in {@code SELECT (double)1/3}.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilderDsl#udt(CqlIdentifier)}.
   */
  static Selector typeHint(Selector selector, DataType targetType) {
    return new TypeHintSelector(selector, targetType);
  }

  /**
   * Selects the result of a function call, as is {@code SELECT f(a,b)}
   *
   * <p>None of the arguments should be aliased (the query builder checks this at runtime).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  static Selector function(CqlIdentifier functionId, Iterable<Selector> arguments) {
    return new FunctionSelector(null, functionId, arguments);
  }

  /** Var-arg equivalent of {@link #function(CqlIdentifier, Iterable)}. */
  static Selector function(CqlIdentifier functionId, Selector... arguments) {
    return function(functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, Iterable)
   * function(CqlIdentifier.fromCql(functionName), arguments)}.
   */
  static Selector function(String functionName, Iterable<Selector> arguments) {
    return function(CqlIdentifier.fromCql(functionName), arguments);
  }

  /** Var-arg equivalent of {@link #function(String, Iterable)}. */
  static Selector function(String functionName, Selector... arguments) {
    return function(functionName, Arrays.asList(arguments));
  }

  /**
   * Selects the result of a function call, as is {@code SELECT ks.f(a,b)}
   *
   * <p>None of the arguments should be aliased (the query builder checks this at runtime).
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   */
  static Selector function(
      CqlIdentifier keyspaceId, CqlIdentifier functionId, Iterable<Selector> arguments) {
    return new FunctionSelector(keyspaceId, functionId, arguments);
  }

  /** Var-arg equivalent of {@link #function(CqlIdentifier, CqlIdentifier, Iterable)}. */
  static Selector function(
      CqlIdentifier keyspaceId, CqlIdentifier functionId, Selector... arguments) {
    return function(keyspaceId, functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, CqlIdentifier, Iterable)}
   * function(CqlIdentifier.fromCql(functionName), arguments)}.
   */
  static Selector function(String keyspaceName, String functionName, Iterable<Selector> arguments) {
    return function(
        CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(functionName), arguments);
  }

  /** Var-arg equivalent of {@link #function(String, String, Iterable)}. */
  static Selector function(String keyspaceName, String functionName, Selector... arguments) {
    return function(keyspaceName, functionName, Arrays.asList(arguments));
  }

  /**
   * Shortcut to select the result of the built-in {@code writetime} function, as in {@code SELECT
   * writetime(c)}.
   */
  static Selector writeTime(CqlIdentifier columnId) {
    return function("writetime", column(columnId));
  }

  /**
   * Shortcut for {@link #writeTime(CqlIdentifier) writeTime(CqlIdentifier.fromCql(columnName))}.
   */
  static Selector writeTime(String columnName) {
    return writeTime(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code ttl} function, as in {@code SELECT
   * ttl(c)}.
   */
  static Selector ttl(CqlIdentifier columnId) {
    return function("ttl", column(columnId));
  }

  /** Shortcut for {@link #ttl(CqlIdentifier) ttl(CqlIdentifier.fromCql(columnName))}. */
  static Selector ttl(String columnName) {
    return ttl(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Casts a selector to a type, as in {@code SELECT CAST(a AS double)}.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilderDsl#udt(CqlIdentifier)}.
   *
   * @throws IllegalArgumentException if the selector is aliased.
   */
  static Selector cast(Selector selector, DataType targetType) {
    return new CastSelector(selector, targetType);
  }

  /** Shortcut to select the result of the built-in {@code toDate} function on a simple column. */
  static Selector toDate(CqlIdentifier columnId) {
    return function("todate", Selector.column(columnId));
  }

  /** Shortcut for {@link #toDate(CqlIdentifier) toDate(CqlIdentifier.fromCql(columnName))}. */
  static Selector toDate(String columnName) {
    return toDate(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code toTimestamp} function on a simple column.
   */
  static Selector toTimestamp(CqlIdentifier columnId) {
    return function("totimestamp", Selector.column(columnId));
  }

  /**
   * Shortcut for {@link #toTimestamp(CqlIdentifier)
   * toTimestamp(CqlIdentifier.fromCql(columnName))}.
   */
  static Selector toTimestamp(String columnName) {
    return toTimestamp(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code toUnixTimestamp} function on a simple
   * column.
   */
  static Selector toUnixTimestamp(CqlIdentifier columnId) {
    return function("tounixtimestamp", Selector.column(columnId));
  }

  /**
   * Shortcut for {@link #toUnixTimestamp(CqlIdentifier)
   * toUnixTimestamp(CqlIdentifier.fromCql(columnName))}.
   */
  static Selector toUnixTimestamp(String columnName) {
    return toUnixTimestamp(CqlIdentifier.fromCql(columnName));
  }

  /** Aliases the selector, as in {@code SELECT count(*) AS total}. */
  Selector as(CqlIdentifier alias);

  /** Shortcut for {@link #as(CqlIdentifier) as(CqlIdentifier.fromCql(alias))} */
  default Selector as(String alias) {
    return as(CqlIdentifier.fromCql(alias));
  }

  /** @return null if the selector is not aliased. */
  CqlIdentifier getAlias();
}
