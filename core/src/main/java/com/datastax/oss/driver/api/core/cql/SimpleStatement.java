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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement;
import com.datastax.oss.driver.internal.core.time.ServerSideTimestampGenerator;
import com.datastax.oss.driver.internal.core.util.Sizes;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
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
   *
   * <p>Note that the returned object is <b>immutable</b>.
   */
  static SimpleStatement newInstance(@NonNull String cqlQuery) {
    return new DefaultSimpleStatement(
        cqlQuery,
        NullAllowingImmutableList.of(),
        NullAllowingImmutableMap.of(),
        null,
        null,
        null,
        null,
        null,
        null,
        NullAllowingImmutableMap.of(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        null,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }

  /**
   * Shortcut to create an instance of the default implementation with only a CQL query and
   * positional values (see {@link SimpleStatementBuilder} for the defaults for the other fields).
   *
   * <p>Note that the returned object is <b>immutable</b>.
   *
   * @param positionalValues the values for placeholders in the query string. Individual values can
   *     be {@code null}, but the vararg array itself can't.
   */
  static SimpleStatement newInstance(
      @NonNull String cqlQuery, @NonNull Object... positionalValues) {
    return new DefaultSimpleStatement(
        cqlQuery,
        NullAllowingImmutableList.of(positionalValues),
        NullAllowingImmutableMap.of(),
        null,
        null,
        null,
        null,
        null,
        null,
        NullAllowingImmutableMap.of(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        null,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }

  /**
   * Shortcut to create an instance of the default implementation with only a CQL query and named
   * values (see {@link SimpleStatementBuilder} for the defaults for other fields).
   *
   * <p>Note that the returned object is <b>immutable</b>.
   */
  static SimpleStatement newInstance(
      @NonNull String cqlQuery, @NonNull Map<String, Object> namedValues) {
    return new DefaultSimpleStatement(
        cqlQuery,
        NullAllowingImmutableList.of(),
        DefaultSimpleStatement.wrapKeys(namedValues),
        null,
        null,
        null,
        null,
        null,
        null,
        NullAllowingImmutableMap.of(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        null,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }

  /**
   * Returns a builder to create an instance of the default implementation.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static SimpleStatementBuilder builder(@NonNull String query) {
    return new SimpleStatementBuilder(query);
  }

  /**
   * Returns a builder to create an instance of the default implementation, copying the fields of
   * the given statement.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static SimpleStatementBuilder builder(@NonNull SimpleStatement template) {
    return new SimpleStatementBuilder(template);
  }

  @NonNull
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
   * @see #setNamedValuesWithIds(Map)
   */
  @NonNull
  SimpleStatement setQuery(@NonNull String newQuery);

  /**
   * Sets the CQL keyspace to associate with the query.
   *
   * <p>This feature is only available with {@link DefaultProtocolVersion#V5 native protocol v5} or
   * higher. Specifying a per-request keyspace with lower protocol versions will cause a runtime
   * error.
   *
   * @see Request#getKeyspace()
   */
  @NonNull
  SimpleStatement setKeyspace(@Nullable CqlIdentifier newKeyspace);

  /**
   * Shortcut for {@link #setKeyspace(CqlIdentifier)
   * setKeyspace(CqlIdentifier.fromCql(newKeyspaceName))}.
   */
  @NonNull
  default SimpleStatement setKeyspace(@NonNull String newKeyspaceName) {
    return setKeyspace(CqlIdentifier.fromCql(newKeyspaceName));
  }

  @NonNull
  List<Object> getPositionalValues();

  /**
   * Sets the positional values to bind to anonymous placeholders.
   *
   * <p>You can use either positional or named values, but not both. Therefore if you call this
   * method but {@link #getNamedValues()} returns a non-empty map, an {@link
   * IllegalArgumentException} will be thrown.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #setQuery(String)
   */
  @NonNull
  SimpleStatement setPositionalValues(@NonNull List<Object> newPositionalValues);

  @NonNull
  Map<CqlIdentifier, Object> getNamedValues();

  /**
   * Sets the named values to bind to named placeholders.
   *
   * <p>Names must be stripped of the leading column.
   *
   * <p>You can use either positional or named values, but not both. Therefore if you call this
   * method but {@link #getPositionalValues()} returns a non-empty list, an {@link
   * IllegalArgumentException} will be thrown.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   *
   * @see #setQuery(String)
   */
  @NonNull
  SimpleStatement setNamedValuesWithIds(@NonNull Map<CqlIdentifier, Object> newNamedValues);

  /**
   * Shortcut for {@link #setNamedValuesWithIds(Map)} with raw strings as value names. The keys are
   * converted on the fly with {@link CqlIdentifier#fromCql(String)}.
   */
  @NonNull
  default SimpleStatement setNamedValues(@NonNull Map<String, Object> newNamedValues) {
    return setNamedValuesWithIds(DefaultSimpleStatement.wrapKeys(newNamedValues));
  }

  @Override
  default int computeSizeInBytes(@NonNull DriverContext context) {
    int size = Sizes.minimumStatementSize(this, context);

    // SimpleStatement's additional elements to take into account are:
    // - query string
    // - parameters (named or not)
    // - per-query keyspace
    // - page size
    // - paging state
    // - timestamp

    // query
    size += PrimitiveSizes.sizeOfLongString(getQuery());

    // parameters
    size +=
        Sizes.sizeOfSimpleStatementValues(
            this, context.getProtocolVersion(), context.getCodecRegistry());

    // per-query keyspace
    if (getKeyspace() != null) {
      size += PrimitiveSizes.sizeOfString(getKeyspace().asInternal());
    }

    // page size
    size += PrimitiveSizes.INT;

    // paging state
    if (getPagingState() != null) {
      size += PrimitiveSizes.sizeOfBytes(getPagingState());
    }

    // timestamp
    if (!(context.getTimestampGenerator() instanceof ServerSideTimestampGenerator)
        || getQueryTimestamp() != Statement.NO_DEFAULT_TIMESTAMP) {
      size += PrimitiveSizes.LONG;
    }

    return size;
  }
}
