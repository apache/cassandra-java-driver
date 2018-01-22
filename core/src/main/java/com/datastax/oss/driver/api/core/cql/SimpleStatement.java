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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A one-off CQL statement consisting of a query string with optional placeholders, and a set of
 * values for these placeholders.
 *
 * <p>To create instances, client applications can use the {@code newInstance} factory methods on
 * this interface for common cases, or {@link #builder(String)} for more control over the
 * parameters. They can then be passed to {@link CqlSession#execute(Statement)}.
 *
 * <p>Simple statements should be reserved for queries that will only be executed a few times by an
 * application. For more frequent queries, {@link PreparedStatement} provides many advantages: it is
 * more efficient because the server parses the query only once and caches the result; it allows the
 * server to return metadata about the bind variables, which allows the driver to validate the
 * values earlier, and apply certain optimizations like token-aware routing.
 *
 * <p>The default implementation returned by the driver is <b>immutable</b> and <b>thread-safe</b>.
 * All mutating methods return a new instance. See also the static factory methods and builders in
 * this interface.
 *
 * <p>If an application reuses the same statement more than once, it is recommended to cache it (for
 * example in a final field).
 */
public interface SimpleStatement extends BatchableStatement<SimpleStatement> {

  /**
   * Shortcut to create an instance of the default implementation with only a CQL query (see {@link
   * SimpleStatementBuilder} for the defaults for the other fields).
   */
  static SimpleStatement newInstance(String cqlQuery) {
    return new DefaultSimpleStatement(
        cqlQuery,
        Collections.emptyList(),
        Collections.emptyMap(),
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Long.MIN_VALUE,
        null);
  }

  /**
   * Shortcut to create an instance of the default implementation with only a CQL query and
   * positional values (see {@link SimpleStatementBuilder} for the defaults for the other fields).
   */
  static SimpleStatement newInstance(String cqlQuery, Object... positionalValues) {
    return new DefaultSimpleStatement(
        cqlQuery,
        Arrays.asList(positionalValues),
        Collections.emptyMap(),
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Long.MIN_VALUE,
        null);
  }

  /**
   * Shortcut to create an instance of the default implementation with only a CQL query and named
   * values (see {@link SimpleStatementBuilder} for the defaults for other fields).
   */
  static SimpleStatement newInstance(String cqlQuery, Map<String, Object> namedValues) {
    return new DefaultSimpleStatement(
        cqlQuery,
        Collections.emptyList(),
        namedValues,
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Long.MIN_VALUE,
        null);
  }

  /** Returns a builder to create an instance of the default implementation. */
  static SimpleStatementBuilder builder(String query) {
    return new SimpleStatementBuilder(query);
  }

  /**
   * Returns a builder to create an instance of the default implementation, copying the fields of
   * the given statement.
   */
  static SimpleStatementBuilder builder(SimpleStatement template) {
    return new SimpleStatementBuilder(template);
  }

  String getQuery();

  /**
   * Sets the CQL query to execute.
   *
   * <p>It may contain anonymous placeholders identified by a question mark, as in:
   *
   * <pre>
   *   SELECT username FROM user WHERE id = ?
   * </pre>
   *
   * Or named placeholders prefixed by a column, as in:
   *
   * <pre>
   *   SELECT username FROM user WHERE id = :i
   * </pre>
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #setPositionalValues(List)
   * @see #setNamedValues(Map)
   */
  SimpleStatement setQuery(String newQuery);

  /**
   * Sets the CQL keyspace to associate with the query.
   *
   * <p>This feature is only available with {@link CoreProtocolVersion#V5 native protocol v5} or
   * higher. Specifying a per-request keyspace with lower protocol versions will cause a runtime
   * error.
   *
   * @see Request#getKeyspace()
   */
  SimpleStatement setKeyspace(CqlIdentifier newKeyspace);

  List<Object> getPositionalValues();

  /**
   * Sets the positional values to bind to anonymous placeholders.
   *
   * <p>You can use either positional or named values, but not both. Therefore if this method
   * returns a non-empty list, then {@link #getNamedValues()} must return an empty map, otherwise a
   * runtime error will be thrown.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #setQuery(String)
   */
  SimpleStatement setPositionalValues(List<Object> newPositionalValues);

  Map<String, Object> getNamedValues();

  /**
   * Sets the named values to bind to named placeholders.
   *
   * <p>Names must be stripped of the leading column.
   *
   * <p>You can use either positional or named values, but not both. Therefore if this method
   * returns a non-empty map, then {@link #getPositionalValues()} must return an empty list,
   * otherwise a runtime error will be thrown.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #setQuery(String)
   */
  SimpleStatement setNamedValues(Map<String, Object> newNamedValues);
}
