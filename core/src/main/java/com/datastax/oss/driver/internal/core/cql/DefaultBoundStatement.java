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
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.util.RoutingKey;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DefaultBoundStatement implements BoundStatement {

  private final PreparedStatement preparedStatement;
  private final ColumnDefinitions variableDefinitions;
  private final ByteBuffer[] values;
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final CqlIdentifier routingKeyspace;
  private final ByteBuffer routingKey;
  private final Token routingToken;
  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final boolean tracing;
  private final long timestamp;
  private final ByteBuffer pagingState;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;

  public DefaultBoundStatement(
      PreparedStatement preparedStatement,
      ColumnDefinitions variableDefinitions,
      ByteBuffer[] values,
      String configProfileName,
      DriverConfigProfile configProfile,
      CqlIdentifier routingKeyspace,
      ByteBuffer routingKey,
      Token routingToken,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      long timestamp,
      ByteBuffer pagingState,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    this.preparedStatement = preparedStatement;
    this.variableDefinitions = variableDefinitions;
    this.values = values;
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.routingKeyspace = routingKeyspace;
    this.routingKey = routingKey;
    this.routingToken = routingToken;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
    this.tracing = tracing;
    this.timestamp = timestamp;
    this.pagingState = pagingState;
    this.codecRegistry = codecRegistry;
    this.protocolVersion = protocolVersion;
  }

  @Override
  public int size() {
    return variableDefinitions.size();
  }

  @Override
  public DataType getType(int i) {
    return variableDefinitions.get(i).getType();
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return variableDefinitions.firstIndexOf(id);
  }

  @Override
  public int firstIndexOf(String name) {
    return variableDefinitions.firstIndexOf(name);
  }

  @Override
  public CodecRegistry codecRegistry() {
    return codecRegistry;
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return protocolVersion;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return values[i];
  }

  @Override
  public BoundStatement setBytesUnsafe(int i, ByteBuffer v) {
    ByteBuffer[] newValues = Arrays.copyOf(values, values.length);
    newValues[i] = v;
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        newValues,
        configProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public PreparedStatement getPreparedStatement() {
    return preparedStatement;
  }

  @Override
  public List<ByteBuffer> getValues() {
    return Arrays.asList(values);
  }

  @Override
  public String getConfigProfileName() {
    return configProfileName;
  }

  @Override
  public BoundStatement setConfigProfileName(String newConfigProfileName) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        newConfigProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @Override
  public BoundStatement setConfigProfile(DriverConfigProfile newConfigProfile) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        newConfigProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    // If it was set explicitly, use that value, else try to infer it from the prepared statement's
    // metadata
    if (routingKeyspace != null) {
      return routingKeyspace;
    } else {
      ColumnDefinitions definitions = preparedStatement.getResultSetDefinitions();
      return (definitions.size() == 0) ? null : definitions.get(0).getKeyspace();
    }
  }

  @Override
  public BoundStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        newRoutingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public ByteBuffer getRoutingKey() {
    if (routingKey != null) {
      return routingKey;
    } else {
      List<Integer> indices = preparedStatement.getPrimaryKeyIndices();
      if (indices.isEmpty()) {
        return null;
      } else if (indices.size() == 1) {
        return getBytesUnsafe(indices.get(0));
      } else {
        ByteBuffer[] components = new ByteBuffer[indices.size()];
        for (Integer index : indices) {
          ByteBuffer value;
          if (!isSet(index) || (value = getBytesUnsafe(index)) == null) {
            return null;
          } else {
            components[index] = value;
          }
        }
        return RoutingKey.compose(components);
      }
    }
  }

  @Override
  public BoundStatement setRoutingKey(ByteBuffer newRoutingKey) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        routingKeyspace,
        newRoutingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public Token getRoutingToken() {
    return routingToken;
  }

  @Override
  public BoundStatement setRoutingToken(Token newRoutingToken) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        newRoutingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @Override
  public BoundStatement setCustomPayload(Map<String, ByteBuffer> newCustomPayload) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        newCustomPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Override
  public BoundStatement setIdempotent(Boolean newIdempotence) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        newIdempotence,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public BoundStatement setTracing(boolean newTracing) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        newTracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public BoundStatement setTimestamp(long newTimestamp) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        newTimestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public BoundStatement setPagingState(ByteBuffer newPagingState) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        newPagingState,
        codecRegistry,
        protocolVersion);
  }
}
