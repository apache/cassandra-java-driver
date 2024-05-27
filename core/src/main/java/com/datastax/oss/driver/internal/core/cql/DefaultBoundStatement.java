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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.util.RoutingKey;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultBoundStatement implements BoundStatement {

  private final PreparedStatement preparedStatement;
  private final ColumnDefinitions variableDefinitions;
  private final ByteBuffer[] values;
  private final String executionProfileName;
  private final DriverExecutionProfile executionProfile;
  private final CqlIdentifier routingKeyspace;
  private final ByteBuffer routingKey;
  private final Token routingToken;
  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final boolean tracing;
  private final long timestamp;
  private final ByteBuffer pagingState;
  private final int pageSize;
  private final ConsistencyLevel consistencyLevel;
  private final ConsistencyLevel serialConsistencyLevel;
  private final Duration timeout;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;
  private final Node node;
  private final int nowInSeconds;

  public DefaultBoundStatement(
      PreparedStatement preparedStatement,
      ColumnDefinitions variableDefinitions,
      ByteBuffer[] values,
      String executionProfileName,
      DriverExecutionProfile executionProfile,
      CqlIdentifier routingKeyspace,
      ByteBuffer routingKey,
      Token routingToken,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      long timestamp,
      ByteBuffer pagingState,
      int pageSize,
      ConsistencyLevel consistencyLevel,
      ConsistencyLevel serialConsistencyLevel,
      Duration timeout,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion,
      Node node,
      int nowInSeconds) {
    this.preparedStatement = preparedStatement;
    this.variableDefinitions = variableDefinitions;
    this.values = values;
    this.executionProfileName = executionProfileName;
    this.executionProfile = executionProfile;
    this.routingKeyspace = routingKeyspace;
    this.routingKey = routingKey;
    this.routingToken = routingToken;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
    this.tracing = tracing;
    this.timestamp = timestamp;
    this.pagingState = pagingState;
    this.pageSize = pageSize;
    this.consistencyLevel = consistencyLevel;
    this.serialConsistencyLevel = serialConsistencyLevel;
    this.timeout = timeout;
    this.codecRegistry = codecRegistry;
    this.protocolVersion = protocolVersion;
    this.node = node;
    this.nowInSeconds = nowInSeconds;
  }

  @Override
  public int size() {
    return variableDefinitions.size();
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return variableDefinitions.get(i).getType();
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull CqlIdentifier id) {
    List<Integer> indices = variableDefinitions.allIndicesOf(id);
    if (indices.isEmpty()) {
      throw new IllegalArgumentException(id + " is not a variable in this bound statement");
    }
    return indices;
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    int indexOf = variableDefinitions.firstIndexOf(id);
    if (indexOf == -1) {
      throw new IllegalArgumentException(id + " is not a variable in this bound statement");
    }
    return indexOf;
  }

  @NonNull
  @Override
  public List<Integer> allIndicesOf(@NonNull String name) {
    List<Integer> indices = variableDefinitions.allIndicesOf(name);
    if (indices.isEmpty()) {
      throw new IllegalArgumentException(name + " is not a variable in this bound statement");
    }
    return indices;
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    int indexOf = variableDefinitions.firstIndexOf(name);
    if (indexOf == -1) {
      throw new IllegalArgumentException(name + " is not a variable in this bound statement");
    }
    return indexOf;
  }

  @NonNull
  @Override
  public CodecRegistry codecRegistry() {
    return codecRegistry;
  }

  @NonNull
  @Override
  public ProtocolVersion protocolVersion() {
    return protocolVersion;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return values[i];
  }

  @NonNull
  @Override
  public BoundStatement setBytesUnsafe(int i, ByteBuffer v) {
    ByteBuffer[] newValues = Arrays.copyOf(values, values.length);
    newValues[i] = v;
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        newValues,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @NonNull
  @Override
  public PreparedStatement getPreparedStatement() {
    return preparedStatement;
  }

  @NonNull
  @Override
  public List<ByteBuffer> getValues() {
    return Arrays.asList(values);
  }

  @Override
  public String getExecutionProfileName() {
    return executionProfileName;
  }

  @NonNull
  @Override
  public BoundStatement setExecutionProfileName(@Nullable String newConfigProfileName) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        newConfigProfileName,
        (newConfigProfileName == null) ? executionProfile : null,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return executionProfile;
  }

  @NonNull
  @Override
  public BoundStatement setExecutionProfile(@Nullable DriverExecutionProfile newProfile) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        (newProfile == null) ? executionProfileName : null,
        newProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public Partitioner getPartitioner() {
    return preparedStatement.getPartitioner();
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    // If it was set explicitly, use that value, else try to infer it from the prepared statement's
    // metadata
    if (routingKeyspace != null) {
      return routingKeyspace;
    } else {
      ColumnDefinitions definitions = preparedStatement.getVariableDefinitions();
      return (definitions.size() == 0) ? null : definitions.get(0).getKeyspace();
    }
  }

  @Override
  public CqlIdentifier getRoutingTable() {
    ColumnDefinitions definitions = preparedStatement.getVariableDefinitions();
    return (definitions.size() == 0) ? null : definitions.get(0).getTable();
  }

  @NonNull
  @Override
  public BoundStatement setRoutingKeyspace(@Nullable CqlIdentifier newRoutingKeyspace) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        newRoutingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @NonNull
  @Override
  public BoundStatement setNode(@Nullable Node newNode) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        newNode,
        nowInSeconds);
  }

  @Nullable
  @Override
  public Node getNode() {
    return node;
  }

  @Override
  public ByteBuffer getRoutingKey() {
    if (routingKey != null) {
      return routingKey;
    } else {
      List<Integer> indices = preparedStatement.getPartitionKeyIndices();
      if (indices.isEmpty()) {
        return null;
      } else if (indices.size() == 1) {
        return getBytesUnsafe(indices.get(0));
      } else {
        ByteBuffer[] components = new ByteBuffer[indices.size()];
        for (int i = 0; i < components.length; i++) {
          ByteBuffer value;
          int index = indices.get(i);
          if (!isSet(index) || (value = getBytesUnsafe(index)) == null) {
            return null;
          } else {
            components[i] = value;
          }
        }
        return RoutingKey.compose(components);
      }
    }
  }

  @NonNull
  @Override
  public BoundStatement setRoutingKey(@Nullable ByteBuffer newRoutingKey) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        newRoutingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public Token getRoutingToken() {
    return routingToken;
  }

  @NonNull
  @Override
  public BoundStatement setRoutingToken(@Nullable Token newRoutingToken) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        newRoutingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @NonNull
  @Override
  public BoundStatement setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        newCustomPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @NonNull
  @Override
  public BoundStatement setIdempotent(@Nullable Boolean newIdempotence) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        newIdempotence,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @NonNull
  @Override
  public BoundStatement setTracing(boolean newTracing) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        newTracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public long getQueryTimestamp() {
    return timestamp;
  }

  @NonNull
  @Override
  public BoundStatement setQueryTimestamp(long newTimestamp) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        newTimestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Nullable
  @Override
  public Duration getTimeout() {
    return timeout;
  }

  @NonNull
  @Override
  public BoundStatement setTimeout(@Nullable Duration newTimeout) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        newTimeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @NonNull
  @Override
  public BoundStatement setPagingState(@Nullable ByteBuffer newPagingState) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        newPagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public int getPageSize() {
    return pageSize;
  }

  @NonNull
  @Override
  public BoundStatement setPageSize(int newPageSize) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        newPageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Nullable
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @NonNull
  @Override
  public BoundStatement setConsistencyLevel(@Nullable ConsistencyLevel newConsistencyLevel) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        newConsistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Nullable
  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistencyLevel;
  }

  @NonNull
  @Override
  public BoundStatement setSerialConsistencyLevel(
      @Nullable ConsistencyLevel newSerialConsistencyLevel) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        newSerialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }

  @Override
  public int getNowInSeconds() {
    return nowInSeconds;
  }

  @NonNull
  @Override
  public BoundStatement setNowInSeconds(int newNowInSeconds) {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState,
        pageSize,
        consistencyLevel,
        serialConsistencyLevel,
        timeout,
        codecRegistry,
        protocolVersion,
        node,
        newNowInSeconds);
  }

  @Override
  public boolean isLWT() {
    return this.getPreparedStatement().isLWT();
  }
}
