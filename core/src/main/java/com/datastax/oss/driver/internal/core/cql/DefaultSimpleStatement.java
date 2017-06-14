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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DefaultSimpleStatement implements SimpleStatement {

  private final String query;
  private final List<Object> positionalValues;
  private final Map<String, Object> namedValues;
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final boolean tracing;
  private final ByteBuffer pagingState;

  public DefaultSimpleStatement(String query, List<Object> positionalValues) {
    this(
        query,
        positionalValues,
        Collections.emptyMap(),
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        null);
  }

  public DefaultSimpleStatement(String query, Map<String, Object> namedValues) {
    this(
        query,
        Collections.emptyList(),
        namedValues,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        null);
  }

  /** @see SimpleStatement#builder(String) */
  public DefaultSimpleStatement(
      String query,
      List<Object> positionalValues,
      Map<String, Object> namedValues,
      String configProfileName,
      DriverConfigProfile configProfile,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      ByteBuffer pagingState) {
    this.query = query;
    this.positionalValues = positionalValues;
    this.namedValues = namedValues;
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
    this.tracing = tracing;
    this.pagingState = pagingState;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public List<Object> getPositionalValues() {
    return positionalValues;
  }

  @Override
  public Map<String, Object> getNamedValues() {
    return namedValues;
  }

  @Override
  public String getConfigProfileName() {
    return configProfileName;
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @Override
  public String getKeyspace() {
    // Not implemented yet, waiting for CASSANDRA-10145 to land in a release
    return null;
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public DefaultSimpleStatement copy(ByteBuffer newPagingState) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        customPayload,
        idempotent,
        tracing,
        newPagingState);
  }
}
