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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class DefaultSimpleStatement implements SimpleStatement {

  private final String query;
  private final List<Object> positionalValues;
  private final Map<String, Object> namedValues;
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier routingKeyspace;
  private final ByteBuffer routingKey;
  private final Token routingToken;

  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final boolean tracing;
  private final long timestamp;
  private final ByteBuffer pagingState;

  /** @see SimpleStatement#builder(String) */
  public DefaultSimpleStatement(
      String query,
      List<Object> positionalValues,
      Map<String, Object> namedValues,
      String configProfileName,
      DriverConfigProfile configProfile,
      CqlIdentifier keyspace,
      CqlIdentifier routingKeyspace,
      ByteBuffer routingKey,
      Token routingToken,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      long timestamp,
      ByteBuffer pagingState) {
    if (!positionalValues.isEmpty() && !namedValues.isEmpty()) {
      throw new IllegalArgumentException("Can't have both positional and named values");
    }
    this.query = query;
    this.positionalValues = ImmutableList.copyOf(positionalValues);
    this.namedValues = ImmutableMap.copyOf(namedValues);
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.keyspace = keyspace;
    this.routingKeyspace = routingKeyspace;
    this.routingKey = routingKey;
    this.routingToken = routingToken;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
    this.tracing = tracing;
    this.timestamp = timestamp;
    this.pagingState = pagingState;
  }

  @Override
  public String getQuery() {
    return query;
  }

  @Override
  public SimpleStatement setQuery(String newQuery) {
    return new DefaultSimpleStatement(
        newQuery,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public List<Object> getPositionalValues() {
    return positionalValues;
  }

  @Override
  public SimpleStatement setPositionalValues(List<Object> newPositionalValues) {
    return new DefaultSimpleStatement(
        query,
        newPositionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Map<String, Object> getNamedValues() {
    return namedValues;
  }

  @Override
  public SimpleStatement setNamedValues(Map<String, Object> newNamedValues) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        newNamedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public String getConfigProfileName() {
    return configProfileName;
  }

  @Override
  public SimpleStatement setConfigProfileName(String newConfigProfileName) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        newConfigProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @Override
  public SimpleStatement setConfigProfile(DriverConfigProfile newProfile) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        null,
        newProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  @Override
  public SimpleStatement setKeyspace(CqlIdentifier newKeyspace) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        newKeyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    return routingKeyspace;
  }

  @Override
  public SimpleStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        newRoutingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public ByteBuffer getRoutingKey() {
    return routingKey;
  }

  @Override
  public SimpleStatement setRoutingKey(ByteBuffer newRoutingKey) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        newRoutingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Token getRoutingToken() {
    return routingToken;
  }

  @Override
  public SimpleStatement setRoutingToken(Token newRoutingToken) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        newRoutingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @Override
  public SimpleStatement setCustomPayload(Map<String, ByteBuffer> newCustomPayload) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        newCustomPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Override
  public SimpleStatement setIdempotent(Boolean newIdempotence) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        newIdempotence,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public SimpleStatement setTracing(boolean newTracing) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        newTracing,
        timestamp,
        pagingState);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public SimpleStatement setTimestamp(long newTimestamp) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        newTimestamp,
        pagingState);
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public SimpleStatement setPagingState(ByteBuffer newPagingState) {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        newPagingState);
  }
}
