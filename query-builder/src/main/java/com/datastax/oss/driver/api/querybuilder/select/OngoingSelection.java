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
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.google.common.collect.Iterables;
import java.util.Arrays;
import java.util.Map;

/**
 * A SELECT query that accepts additional selectors (that is, elements in the SELECT clause to
 * return as columns in the result set, as in: {@code SELECT count(*), sku, price...}).
 */
public interface OngoingSelection {

  /**
   * Adds a selector.
   *
   * <p>To create the argument, use one of the factory methods in {@link Selector}, for example
   * {@link Selector#column(CqlIdentifier) column}. This type also provides shortcuts to create and
   * add the selector in one call, for example {@link #column(CqlIdentifier)} for {@code
   * selector(Selector.column(...))}.
   *
   * <p>If you add multiple selectors as once, consider {@link #selectors(Iterable)} as a more
   * efficient alternative.
   */
  Select selector(Selector selector);

  /**
   * Adds multiple selectors at once.
   *
   * <p>This is slightly more efficient than adding the selectors one by one (since the underlying
   * implementation of this object is immutable).
   *
   * <p>To create the arguments, use one of the factory methods in {@link Selector}, for example
   * {@link Selector#column(CqlIdentifier) column}.
   *
   * @throws IllegalArgumentException if one of the selectors is {@link Selector#all()} ({@code *}
   *     can only be used on its own).
   * @see #selector(Selector)
   */
  Select selectors(Iterable<Selector> additionalSelectors);

  /** Var-arg equivalent of {@link #selectors(Iterable)}. */
  default Select selectors(Selector... additionalSelectors) {
    return selectors(Arrays.asList(additionalSelectors));
  }

  /**
   * Selects all columns, as in {@code SELECT *}.
   *
   * <p>This will clear any previously configured selector. Similarly, if any other selector is
   * added later, it will cancel this one.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.all())}.
   *
   * @see Selector#all()
   */
  default Select all() {
    return selector(Selector.all());
  }

  /**
   * Selects the count of all returned rows, as in {@code SELECT count(*)}.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.countAll())}.
   *
   * @see Selector#countAll()
   */
  default Select countAll() {
    return selector(Selector.countAll());
  }

  /**
   * Selects a particular column by its CQL identifier.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.column(columnId))}.
   *
   * @see Selector#column(CqlIdentifier)
   */
  default Select column(CqlIdentifier columnId) {
    return selector(Selector.column(columnId));
  }

  /** Shortcut for {@link #column(CqlIdentifier) column(CqlIdentifier.fromCql(columnName))} */
  default Select column(String columnName) {
    return column(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(CqlIdentifier)} for each element.
   */
  default Select columnsIds(Iterable<CqlIdentifier> columnIds) {
    return selectors(Iterables.transform(columnIds, Selector::column));
  }

  /** Var-arg equivalent of {@link #columnsIds(Iterable)}. */
  default Select columns(CqlIdentifier... columnIds) {
    return columnsIds(Arrays.asList(columnIds));
  }

  /**
   * Convenience method to select multiple simple columns at once, as in {@code SELECT a,b,c}.
   *
   * <p>This is the same as calling {@link #column(String)} for each element.
   */
  default Select columns(Iterable<String> columnNames) {
    return selectors(Iterables.transform(columnNames, Selector::column));
  }

  /** Var-arg equivalent of {@link #columns(Iterable)}. */
  default Select columns(String... columnNames) {
    return columns(Arrays.asList(columnNames));
  }

  /**
   * Selects the sum of two arguments, as in {@code SELECT col1 + col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.add(left, right))}.
   *
   * @see Selector#add(Selector, Selector)
   */
  default Select add(Selector left, Selector right) {
    return selector(Selector.add(left, right));
  }

  /**
   * Selects the difference of two terms, as in {@code SELECT col1 - col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.subtract(left, right))}.
   *
   * @see Selector#subtract(Selector, Selector)
   */
  default Select subtract(Selector left, Selector right) {
    return selector(Selector.subtract(left, right));
  }

  /**
   * Selects the product of two arguments, as in {@code SELECT col1 * col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.multiply(left, right))}.
   *
   * <p>The arguments will be parenthesized if they are instances of {@link Selector#add} or {@link
   * Selector#subtract}. If they are raw selectors, you might have to parenthesize them yourself.
   *
   * @see Selector#multiply(Selector, Selector)
   */
  default Select multiply(Selector left, Selector right) {
    return selector(Selector.multiply(left, right));
  }

  /**
   * Selects the quotient of two arguments, as in {@code SELECT col1 / col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.divide(left, right))}.
   *
   * <p>The arguments will be parenthesized if they are instances of {@link Selector#add} or {@link
   * Selector#subtract}. If they are raw selectors, you might have to parenthesize them yourself.
   *
   * @see Selector#divide(Selector, Selector)
   */
  default Select divide(Selector left, Selector right) {
    return selector(Selector.divide(left, right));
  }

  /**
   * Selects the remainder of two arguments, as in {@code SELECT col1 % col2}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.remainder(left,
   * right))}.
   *
   * <p>The arguments will be parenthesized if they are instances of {@link Selector#add} or {@link
   * Selector#subtract}. If they are raw selectors, you might have to parenthesize them yourself.
   *
   * @see Selector#remainder(Selector, Selector)
   */
  default Select remainder(Selector left, Selector right) {
    return selector(Selector.remainder(left, right));
  }

  /**
   * Selects the opposite of an argument, as in {@code SELECT -col1}.
   *
   * <p>This is available in Cassandra 4 and above.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.negate(argument))}.
   *
   * <p>The argument will be parenthesized if it is an instance of {@link Selector#add} or {@link
   * Selector#subtract}. If it is a raw selector, you might have to parenthesize it yourself.
   *
   * @see Selector#negate(Selector)
   */
  default Select negate(Selector argument) {
    return selector(Selector.negate(argument));
  }

  /**
   * Selects a field inside of a UDT column, as in {@code SELECT user.name}.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.field(udt, fieldId))}.
   *
   * @see Selector#field(Selector, CqlIdentifier)
   */
  default Select field(Selector udt, CqlIdentifier fieldId) {
    return selector(Selector.field(udt, fieldId));
  }

  /**
   * Shortcut for {@link #field(Selector, CqlIdentifier) field(udt,
   * CqlIdentifier.fromCql(fieldName))}.
   */
  default Select field(Selector udt, String fieldName) {
    return field(udt, CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Shortcut to select a UDT field when the UDT is a simple column (as opposed to a more complex
   * selection, like a nested UDT).
   *
   * <p>In other words, this is a shortcut for {{@link #field(Selector, CqlIdentifier)
   * field(QueryBuilderDsl.column(udtColumnId), fieldId)}.
   *
   * @see Selector#field(CqlIdentifier, CqlIdentifier)
   */
  default Select field(CqlIdentifier udtColumnId, CqlIdentifier fieldId) {
    return field(Selector.column(udtColumnId), fieldId);
  }

  /**
   * Shortcut for {@link #field(CqlIdentifier, CqlIdentifier)
   * field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName))}.
   *
   * @see Selector#field(String, String)
   */
  default Select field(String udtColumnName, String fieldName) {
    return field(CqlIdentifier.fromCql(udtColumnName), CqlIdentifier.fromCql(fieldName));
  }

  /**
   * Selects an element in a collection column, as in {@code SELECT m['key']}.
   *
   * <p>As of Cassandra 4, this is only allowed for map and set columns.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.element(collection,
   * index))}.
   *
   * @see Selector#element(Selector, Term)
   */
  default Select element(Selector collection, Term index) {
    return selector(Selector.element(collection, index));
  }

  /**
   * Shortcut for element selection when the target collection is a simple column.
   *
   * <p>In other words, this is the equivalent of {@link #element(Selector, Term)
   * element(QueryBuilderDsl.column(collection), index)}.
   *
   * @see Selector#element(CqlIdentifier, Term)
   */
  default Select element(CqlIdentifier collectionId, Term index) {
    return element(Selector.column(collectionId), index);
  }

  /**
   * Shortcut for {@link #element(CqlIdentifier, Term)
   * element(CqlIdentifier.fromCql(collectionName), index)}.
   *
   * @see Selector#element(String, Term)
   */
  default Select element(String collectionName, Term index) {
    return element(CqlIdentifier.fromCql(collectionName), index);
  }

  /**
   * Selects a slice in a collection column, as in {@code SELECT s[4..8]}.
   *
   * <p>As of Cassandra 4, this is only allowed for set and map columns. Those collections are
   * ordered, the elements (or keys in the case of a map), will be compared to the bounds for
   * inclusions. Either bound can be unspecified, but not both.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.range(collection, left,
   * right))}.
   *
   * @param left the left bound (inclusive). Can be {@code null} to indicate that the slice is only
   *     right-bound.
   * @param right the right bound (inclusive). Can be {@code null} to indicate that the slice is
   *     only left-bound.
   * @see Selector#range(Selector, Term, Term)
   */
  default Select range(Selector collection, Term left, Term right) {
    return selector(Selector.range(collection, left, right));
  }

  /**
   * Shortcut for slice selection when the target collection is a simple column.
   *
   * <p>In other words, this is the equivalent of {@link #range(Selector, Term, Term)}
   * range(Selector.column(collectionId), left, right)}.
   *
   * @see Selector#range(CqlIdentifier, Term, Term)
   */
  default Select range(CqlIdentifier collectionId, Term left, Term right) {
    return range(Selector.column(collectionId), left, right);
  }

  /**
   * Shortcut for {@link #range(CqlIdentifier, Term, Term)
   * range(CqlIdentifier.fromCql(collectionName), left, right)}.
   *
   * @see Selector#range(String, Term, Term)
   */
  default Select range(String collectionName, Term left, Term right) {
    return range(CqlIdentifier.fromCql(collectionName), left, right);
  }

  /**
   * Selects a group of elements as a list, as in {@code SELECT [a,b,c]}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime), and they
   * should all produce the same data type (the query builder can't check this, so the query will
   * fail at execution time).
   *
   * <p>This is a shortcut for {@link #selector(Selector)
   * selector(Selector.listOf(elementSelectors))}.
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   * @see Selector#listOf(Iterable)
   */
  default Select listOf(Iterable<Selector> elementSelectors) {
    return selector(Selector.listOf(elementSelectors));
  }

  /** Var-arg equivalent of {@link #listOf(Iterable)}. */
  default Select listOf(Selector... elementSelectors) {
    return listOf(Arrays.asList(elementSelectors));
  }

  /**
   * Selects a group of elements as a set, as in {@code SELECT {a,b,c}}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime), and they
   * should all produce the same data type (the query builder can't check this, so the query will
   * fail at execution time).
   *
   * <p>This is a shortcut for {@link #selector(Selector)
   * selector(Selector.setOf(elementSelectors))}.
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   * @see Selector#setOf(Iterable)
   */
  default Select setOf(Iterable<Selector> elementSelectors) {
    return selector(Selector.setOf(elementSelectors));
  }

  /** Var-arg equivalent of {@link #setOf(Iterable)}. */
  default Select setOf(Selector... elementSelectors) {
    return setOf(Arrays.asList(elementSelectors));
  }

  /**
   * Selects a group of elements as a tuple, as in {@code SELECT (a,b,c)}.
   *
   * <p>None of the selectors should be aliased (the query builder checks this at runtime).
   *
   * <p>This is a shortcut for {@link #selector(Selector)
   * selector(Selector.tupleOf(elementSelectors))}.
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   * @see Selector#tupleOf(Iterable)
   */
  default Select tupleOf(Iterable<Selector> elementSelectors) {
    return selector(Selector.tupleOf(elementSelectors));
  }

  /** Var-arg equivalent of {@link #tupleOf(Iterable)}. */
  default Select tupleOf(Selector... elementSelectors) {
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
   * <p>This is a shortcut for {@link #selector(Selector)
   * selector(Selector.mapOf(elementSelectors))}.
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
   * @see Selector#mapOf(Map)
   */
  default Select mapOf(Map<Selector, Selector> elementSelectors) {
    return selector(Selector.mapOf(elementSelectors));
  }

  /**
   * Selects a group of elements as a map and force the resulting map type, as in {@code SELECT
   * (map<int,text>){a:b,c:d}}.
   *
   * <p>To create the data types, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilderDsl#udt(CqlIdentifier)}.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.mapOf(elementSelectors,
   * keyType, valueType))}.
   *
   * @see #mapOf(Map)
   * @see Selector#mapOf(Map, DataType, DataType)
   */
  default Select mapOf(
      Map<Selector, Selector> elementSelectors, DataType keyType, DataType valueType) {
    return selector(Selector.mapOf(elementSelectors, keyType, valueType));
  }

  /**
   * Provides a type hint for a selector, as in {@code SELECT (double)1/3}.
   *
   * <p>Use the constants and static methods in {@link DataTypes} to create the data type.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.typeHint(selector,
   * targetType))}.
   *
   * @see Selector#typeHint(Selector, DataType)
   */
  default Select typeHint(Selector selector, DataType targetType) {
    return selector(Selector.typeHint(selector, targetType));
  }

  /**
   * Selects the result of a function call, as is {@code SELECT f(a,b)}
   *
   * <p>None of the arguments should be aliased (the query builder checks this at runtime).
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.function(functionId,
   * arguments))}.
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   * @see Selector#function(CqlIdentifier, Iterable)
   */
  default Select function(CqlIdentifier functionId, Iterable<Selector> arguments) {
    return selector(Selector.function(functionId, arguments));
  }

  /**
   * Var-arg equivalent of {@link #function(CqlIdentifier, Iterable)}.
   *
   * @see Selector#function(CqlIdentifier, Selector...)
   */
  default Select function(CqlIdentifier functionId, Selector... arguments) {
    return function(functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, Iterable)
   * function(CqlIdentifier.fromCql(functionName), arguments)}.
   *
   * @see Selector#function(String, Iterable)
   */
  default Select function(String functionName, Iterable<Selector> arguments) {
    return function(CqlIdentifier.fromCql(functionName), arguments);
  }

  /**
   * Var-arg equivalent of {@link #function(String, Iterable)}.
   *
   * @see Selector#function(String, Selector...)
   */
  default Select function(String functionName, Selector... arguments) {
    return function(functionName, Arrays.asList(arguments));
  }

  /**
   * Selects the result of a function call, as is {@code SELECT f(a,b)}
   *
   * <p>None of the arguments should be aliased (the query builder checks this at runtime).
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.function(keyspaceId,
   * functionId, arguments))}.
   *
   * @throws IllegalArgumentException if any of the selectors is aliased.
   * @see Selector#function(CqlIdentifier, CqlIdentifier, Iterable)
   */
  default Select function(
      CqlIdentifier keyspaceId, CqlIdentifier functionId, Iterable<Selector> arguments) {
    return selector(Selector.function(keyspaceId, functionId, arguments));
  }

  /**
   * Var-arg equivalent of {@link #function(CqlIdentifier,CqlIdentifier, Iterable)}.
   *
   * @see Selector#function(CqlIdentifier, CqlIdentifier, Selector...)
   */
  default Select function(
      CqlIdentifier keyspaceId, CqlIdentifier functionId, Selector... arguments) {
    return function(keyspaceId, functionId, Arrays.asList(arguments));
  }

  /**
   * Shortcut for {@link #function(CqlIdentifier, CqlIdentifier, Iterable)
   * function(CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(functionName), arguments)}.
   *
   * @see Selector#function(String, String, Iterable)
   */
  default Select function(String keyspaceName, String functionName, Iterable<Selector> arguments) {
    return function(
        CqlIdentifier.fromCql(keyspaceName), CqlIdentifier.fromCql(functionName), arguments);
  }

  /**
   * Var-arg equivalent of {@link #function(String, String, Iterable)}.
   *
   * @see Selector#function(String, String, Selector...)
   */
  default Select function(String keyspaceName, String functionName, Selector... arguments) {
    return function(keyspaceName, functionName, Arrays.asList(arguments));
  }

  /**
   * Shortcut to select the result of the built-in {@code writetime} function, as in {@code SELECT
   * writetime(c)}.
   *
   * @see Selector#writeTime(CqlIdentifier)
   */
  default Select writeTime(CqlIdentifier columnId) {
    return selector(Selector.writeTime(columnId));
  }

  /**
   * Shortcut for {@link #writeTime(CqlIdentifier) writeTime(CqlIdentifier.fromCql(columnName))}.
   *
   * @see Selector#writeTime(String)
   */
  default Select writeTime(String columnName) {
    return writeTime(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code ttl} function, as in {@code SELECT
   * ttl(c)}.
   *
   * @see Selector#ttl(CqlIdentifier)
   */
  default Select ttl(CqlIdentifier columnId) {
    return selector(Selector.ttl(columnId));
  }

  /**
   * Shortcut for {@link #ttl(CqlIdentifier) ttl(CqlIdentifier.fromCql(columnName))}.
   *
   * @see Selector#ttl(String)
   */
  default Select ttl(String columnName) {
    return ttl(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Casts a selector to a type, as in {@code SELECT CAST(a AS double)}.
   *
   * <p>To create the data type, use the constants and static methods in {@link DataTypes}, or
   * {@link QueryBuilderDsl#udt(CqlIdentifier)}.
   *
   * <p>This is a shortcut for {@link #selector(Selector) selector(Selector.function(keyspaceId,
   * functionId, arguments))}.
   *
   * @throws IllegalArgumentException if the selector is aliased.
   * @see Selector#cast(Selector, DataType)
   */
  default Select cast(Selector selector, DataType targetType) {
    return selector(Selector.cast(selector, targetType));
  }

  /**
   * Shortcut to select the result of the built-in {@code toDate} function.
   *
   * @see Selector#toDate(CqlIdentifier)
   */
  default Select toDate(CqlIdentifier columnId) {
    return selector(Selector.toDate(columnId));
  }

  /** Shortcut for {@link #toDate(CqlIdentifier) toDate(CqlIdentifier.fromCql(columnName))}. */
  default Select toDate(String columnName) {
    return toDate(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code toTimestamp} function.
   *
   * @see Selector#toTimestamp(CqlIdentifier)
   */
  default Select toTimestamp(CqlIdentifier columnId) {
    return selector(Selector.toTimestamp(columnId));
  }

  /**
   * Shortcut for {@link #toTimestamp(CqlIdentifier)
   * toTimestamp(CqlIdentifier.fromCql(columnName))}.
   */
  default Select toTimestamp(String columnName) {
    return toTimestamp(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Shortcut to select the result of the built-in {@code toUnixTimestamp} function.
   *
   * @see Selector#toUnixTimestamp(CqlIdentifier)
   */
  default Select toUnixTimestamp(CqlIdentifier columnId) {
    return selector(Selector.toUnixTimestamp(columnId));
  }

  /**
   * Shortcut for {@link #toUnixTimestamp(CqlIdentifier)
   * toUnixTimestamp(CqlIdentifier.fromCql(columnName))}.
   */
  default Select toUnixTimestamp(String columnName) {
    return toUnixTimestamp(CqlIdentifier.fromCql(columnName));
  }

  /**
   * Selects literal value, as in {@code WHERE k = 1}.
   *
   * <p>This method can process any type for which there is a default Java to CQL mapping, namely:
   * primitive types ({@code Integer=>int, Long=>bigint, String=>text, etc.}), and collections,
   * tuples, and user defined types thereof.
   *
   * <p>A null argument will be rendered as {@code NULL}.
   *
   * <p>For custom mappings, use {@link #literal(Object, CodecRegistry)} or {@link #literal(Object,
   * TypeCodec)}.
   *
   * @throws CodecNotFoundException if there is no default CQL mapping for the Java type of {@code
   *     value}.
   * @see QueryBuilderDsl#literal(Object)
   */
  default Select literal(Object value) {
    return literal(value, CodecRegistry.DEFAULT);
  }

  /**
   * Selects a literal value, as in {@code WHERE k = 1}.
   *
   * <p>This is an alternative to {@link #literal(Object)} for custom type mappings. The provided
   * registry should contain a codec that can format the value. Typically, this will be your
   * session's registry, which is accessible via {@code session.getContext().codecRegistry()}.
   *
   * @see DriverContext#codecRegistry()
   * @throws CodecNotFoundException if {@code codecRegistry} does not contain any codec that can
   *     handle {@code value}.
   * @see QueryBuilderDsl#literal(Object, CodecRegistry)
   */
  default Select literal(Object value, CodecRegistry codecRegistry) {
    return literal(value, (value == null) ? null : codecRegistry.codecFor(value));
  }

  /**
   * Selects a literal value, as in {@code WHERE k = 1}.
   *
   * <p>This is an alternative to {@link #literal(Object)} for custom type mappings. The value will
   * be turned into a string with {@link TypeCodec#format(Object)}, and inlined in the query.
   *
   * @see QueryBuilderDsl#literal(Object, TypeCodec)
   */
  default <T> Select literal(T value, TypeCodec<T> codec) {
    return selector(QueryBuilderDsl.literal(value, codec));
  }

  /**
   * Selects an arbitrary expression expressed as a raw string.
   *
   * <p>The contents will be appended to the query as-is, without any syntax checking or escaping.
   * This method should be used with caution, as it's possible to generate invalid CQL that will
   * fail at execution time; on the other hand, it can be used as a workaround to handle new CQL
   * features that are not yet covered by the query builder.
   *
   * <p>This is a shortcut for {@link #selector(Selector)
   * selector(QueryBuilderDsl.raw(rawExpression))}.
   */
  default Select raw(String rawExpression) {
    return selector(QueryBuilderDsl.raw(rawExpression));
  }

  /**
   * Aliases the last added selector, as in {@code SELECT count(*) AS total}.
   *
   * <p>It is the caller's responsibility to ensure that this method is called at most once after
   * each selector, and that this selector can legally be aliased:
   *
   * <ul>
   *   <li>if it is called multiple times ({@code countAll().as("total1").as("total2")}), the last
   *       alias will override the previous ones.
   *   <li>if it is called before any selector was set, or after {@link #all()}, an {@link
   *       IllegalStateException} is thrown.
   *   <li>if it is called after a {@link #raw(String)} selector that already defines an alias, the
   *       query will fail at runtime.
   * </ul>
   */
  Select as(CqlIdentifier alias);

  /** Shortcut for {@link #as(CqlIdentifier) as(CqlIdentifier.fromCql(alias))} */
  default Select as(String alias) {
    return as(CqlIdentifier.fromCql(alias));
  }
}
