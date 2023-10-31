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
import com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableList;
import com.datastax.oss.protocol.internal.util.collection.NullAllowingImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import net.jcip.annotations.NotThreadSafe;

/**
 * A builder to create a simple statement.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class SimpleStatementBuilder
    extends StatementBuilder<SimpleStatementBuilder, SimpleStatement> {

  @Nonnull private String query;
  @Nullable private CqlIdentifier keyspace;
  @Nullable private NullAllowingImmutableList.Builder<Object> positionalValuesBuilder;
  @Nullable private NullAllowingImmutableMap.Builder<CqlIdentifier, Object> namedValuesBuilder;

  public SimpleStatementBuilder(@Nonnull String query) {
    this.query = query;
  }

  public SimpleStatementBuilder(@Nonnull SimpleStatement template) {
    super(template);
    if (!template.getPositionalValues().isEmpty() && !template.getNamedValues().isEmpty()) {
      throw new IllegalArgumentException(
          "Illegal statement to copy, can't have both named and positional values");
    }

    this.query = template.getQuery();
    if (!template.getPositionalValues().isEmpty()) {
      this.positionalValuesBuilder =
          NullAllowingImmutableList.builder(template.getPositionalValues().size())
              .addAll(template.getPositionalValues());
    }
    if (!template.getNamedValues().isEmpty()) {
      this.namedValuesBuilder =
          NullAllowingImmutableMap.<CqlIdentifier, Object>builder(template.getNamedValues().size())
              .putAll(template.getNamedValues());
    }
  }

  /** @see SimpleStatement#getQuery() */
  @Nonnull
  public SimpleStatementBuilder setQuery(@Nonnull String query) {
    this.query = query;
    return this;
  }

  /** @see SimpleStatement#getKeyspace() */
  @Nonnull
  public SimpleStatementBuilder setKeyspace(@Nullable CqlIdentifier keyspace) {
    this.keyspace = keyspace;
    return this;
  }

  /**
   * Shortcut for {@link #setKeyspace(CqlIdentifier)
   * setKeyspace(CqlIdentifier.fromCql(keyspaceName))}.
   */
  @Nonnull
  public SimpleStatementBuilder setKeyspace(@Nullable String keyspaceName) {
    return setKeyspace(keyspaceName == null ? null : CqlIdentifier.fromCql(keyspaceName));
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  @Nonnull
  public SimpleStatementBuilder addPositionalValue(@Nullable Object value) {
    if (namedValuesBuilder != null) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (positionalValuesBuilder == null) {
      positionalValuesBuilder = NullAllowingImmutableList.builder();
    }
    positionalValuesBuilder.add(value);
    return this;
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  @Nonnull
  public SimpleStatementBuilder addPositionalValues(@Nonnull Iterable<Object> values) {
    if (namedValuesBuilder != null) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (positionalValuesBuilder == null) {
      positionalValuesBuilder = NullAllowingImmutableList.builder();
    }
    positionalValuesBuilder.addAll(values);
    return this;
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  @Nonnull
  public SimpleStatementBuilder addPositionalValues(@Nonnull Object... values) {
    return addPositionalValues(Arrays.asList(values));
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  @Nonnull
  public SimpleStatementBuilder clearPositionalValues() {
    positionalValuesBuilder = NullAllowingImmutableList.builder();
    return this;
  }

  /** @see SimpleStatement#setNamedValuesWithIds(Map) */
  @Nonnull
  public SimpleStatementBuilder addNamedValue(@Nonnull CqlIdentifier name, @Nullable Object value) {
    if (positionalValuesBuilder != null) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (namedValuesBuilder == null) {
      namedValuesBuilder = NullAllowingImmutableMap.builder();
    }
    namedValuesBuilder.put(name, value);
    return this;
  }

  /**
   * Shortcut for {@link #addNamedValue(CqlIdentifier, Object)
   * addNamedValue(CqlIdentifier.fromCql(name), value)}.
   */
  @Nonnull
  public SimpleStatementBuilder addNamedValue(@Nonnull String name, @Nullable Object value) {
    return addNamedValue(CqlIdentifier.fromCql(name), value);
  }

  /** @see SimpleStatement#setNamedValuesWithIds(Map) */
  @Nonnull
  public SimpleStatementBuilder clearNamedValues() {
    namedValuesBuilder = NullAllowingImmutableMap.builder();
    return this;
  }

  @Nonnull
  @Override
  public SimpleStatement build() {
    return new DefaultSimpleStatement(
        query,
        (positionalValuesBuilder == null)
            ? NullAllowingImmutableList.of()
            : positionalValuesBuilder.build(),
        (namedValuesBuilder == null) ? NullAllowingImmutableMap.of() : namedValuesBuilder.build(),
        executionProfileName,
        executionProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        buildCustomPayload(),
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        node,
        nowInSeconds);
  }
}
