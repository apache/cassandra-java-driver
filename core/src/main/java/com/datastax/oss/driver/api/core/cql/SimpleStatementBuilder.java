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

import com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleStatementBuilder
    extends StatementBuilder<SimpleStatementBuilder, SimpleStatement> {

  private String query;
  private ImmutableList.Builder<Object> positionalValuesBuilder;
  private ImmutableMap.Builder<String, Object> namedValuesBuilder;

  public SimpleStatementBuilder(String query) {
    this.query = query;
  }

  public SimpleStatementBuilder(SimpleStatement template) {
    super(template);
    if (!template.getPositionalValues().isEmpty() && !template.getNamedValues().isEmpty()) {
      throw new IllegalArgumentException(
          "Illegal statement to copy, can't have both named and positional values");
    }

    this.query = template.getQuery();
    if (!template.getPositionalValues().isEmpty()) {
      this.positionalValuesBuilder = ImmutableList.builder().addAll(template.getPositionalValues());
    }
    if (!template.getNamedValues().isEmpty()) {
      this.namedValuesBuilder =
          ImmutableMap.<String, Object>builder().putAll(template.getNamedValues());
    }
  }

  public SimpleStatementBuilder withQuery(String query) {
    this.query = query;
    return this;
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder addPositionalValue(Object value) {
    if (namedValuesBuilder != null) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (positionalValuesBuilder == null) {
      positionalValuesBuilder = ImmutableList.builder();
    }
    positionalValuesBuilder.add(value);
    return this;
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder addPositionalValues(Iterable<Object> values) {
    if (namedValuesBuilder != null) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (positionalValuesBuilder == null) {
      positionalValuesBuilder = ImmutableList.builder();
    }
    positionalValuesBuilder.addAll(values);
    return this;
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder addPositionalValues(Object... values) {
    return addPositionalValues(Arrays.asList(values));
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder clearPositionalValues() {
    positionalValuesBuilder = null;
    return this;
  }

  /** @see SimpleStatement#setNamedValues(Map) */
  public SimpleStatementBuilder addNamedValue(String name, Object value) {
    if (positionalValuesBuilder != null) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (namedValuesBuilder == null) {
      namedValuesBuilder = ImmutableMap.builder();
    }
    namedValuesBuilder.put(name, value);
    return this;
  }

  /** @see SimpleStatement#setNamedValues(Map) */
  public SimpleStatementBuilder clearNamedValues() {
    namedValuesBuilder = null;
    return this;
  }

  public SimpleStatement build() {
    return new DefaultSimpleStatement(
        query,
        (positionalValuesBuilder == null)
            ? Collections.emptyList()
            : positionalValuesBuilder.build(),
        (namedValuesBuilder == null) ? Collections.emptyMap() : namedValuesBuilder.build(),
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        buildCustomPayload(),
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }
}
