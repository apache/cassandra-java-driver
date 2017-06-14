/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SimpleStatementBuilder {

  private final String query;
  private List<Object> positionalValues = Collections.emptyList();
  private ImmutableMap.Builder<String, Object> namedValues;
  private String configProfileName;
  private DriverConfigProfile configProfile;
  private ImmutableMap.Builder<String, ByteBuffer> customPayload;
  private Boolean idempotent;
  private boolean tracing;
  private ByteBuffer pagingState;

  public SimpleStatementBuilder(String query) {
    this.query = query;
  }

  /**
   * Adds a value for an anonymous placeholder.
   *
   * <p>Values must be added in the order they appear in the query string. You can also use {@link
   * #withPositionalValues(Object...)} to add multiple values at once.
   *
   * @throws IllegalArgumentException if named values were already added for this statement; you
   *     can't mix both.
   * @see SimpleStatement#getPositionalValues()
   */
  public SimpleStatementBuilder withPositionalValue(Object value) {
    return withPositionalValues(value);
  }

  /**
   * Adds values for multiple anonymous placeholders.
   *
   * <p>Values must be added in the order they appear in the query string.
   *
   * @throws IllegalArgumentException if named values were already added for this statement; you
   *     can't mix both.
   * @see SimpleStatement#getPositionalValues()
   */
  public SimpleStatementBuilder withPositionalValues(Object... values) {
    if (namedValues != null) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (positionalValues.isEmpty()) {
      positionalValues = new ArrayList<>();
    }
    Collections.addAll(positionalValues, values);
    return this;
  }

  /**
   * Adds a value for a named placeholder in the query string.
   *
   * @throws IllegalArgumentException if positional values were already added for this statement;
   *     you can't mix both.
   * @see SimpleStatement#getPositionalValues()
   */
  public SimpleStatementBuilder withNamedValue(String name, Object value) {
    if (!positionalValues.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    if (namedValues == null) {
      namedValues = ImmutableMap.builder();
    }
    namedValues.put(name, value);
    return this;
  }

  /**
   * Sets the name of the config profile to use for this statement.
   *
   * <p>Note that {@link #withConfigProfile(DriverConfigProfile)} will override this.
   *
   * @see SimpleStatement#getConfigProfileName()
   */
  public SimpleStatementBuilder withConfigProfileName(String configProfileName) {
    this.configProfileName = configProfileName;
    return this;
  }

  /**
   * Sets the config profile to use for this statement.
   *
   * @see SimpleStatement#getConfigProfile()
   */
  public SimpleStatementBuilder withConfigProfile(DriverConfigProfile configProfile) {
    this.configProfile = configProfile;
    this.configProfileName = null;
    return this;
  }

  /**
   * Adds an entry in the custom payload of this statement.
   *
   * @see SimpleStatement#getCustomPayload()
   */
  public SimpleStatementBuilder withCustomPayload(String key, ByteBuffer value) {
    if (customPayload == null) {
      customPayload = ImmutableMap.builder();
    }
    customPayload.put(key, value);
    return this;
  }

  /**
   * Indicates whether this statement is idempotent.
   *
   * <p>If this method is not called, the statement will fallback to the default defined in the
   * configuration.
   *
   * @see SimpleStatement#isIdempotent()
   */
  public SimpleStatementBuilder withIdempotence(boolean idempotent) {
    this.idempotent = idempotent;
    return this;
  }

  /**
   * Indicates that tracing information should be requested when executing this statement.
   *
   * <p>If this method is not called, the statement will not be tracing.
   *
   * @see SimpleStatement#isTracing()
   */
  public SimpleStatementBuilder withTracing() {
    this.tracing = true;
    return this;
  }

  /**
   * Sets the paging state to use for this statement.
   *
   * <p>Note that it is also possible to set the paging state on an existing statement with {@link
   * Statement#copy(ByteBuffer)}.
   *
   * @see Statement#getPagingState()
   */
  public SimpleStatementBuilder withPagingState(ByteBuffer pagingState) {
    this.pagingState = pagingState;
    return this;
  }

  public SimpleStatement build() {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        (namedValues == null) ? Collections.emptyMap() : namedValues.build(),
        configProfileName,
        configProfile,
        (customPayload == null) ? Collections.emptyMap() : customPayload.build(),
        idempotent,
        tracing,
        pagingState);
  }
}
