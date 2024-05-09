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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultBoundStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

/**
 * A builder to create a bound statement.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class BoundStatementBuilder extends StatementBuilder<BoundStatementBuilder, BoundStatement>
    implements Bindable<BoundStatementBuilder> {

  @NonNull private final PreparedStatement preparedStatement;
  @NonNull private final ColumnDefinitions variableDefinitions;
  @NonNull private final ByteBuffer[] values;
  @NonNull private final CodecRegistry codecRegistry;
  @NonNull private final ProtocolVersion protocolVersion;

  public BoundStatementBuilder(
      @NonNull PreparedStatement preparedStatement,
      @NonNull ColumnDefinitions variableDefinitions,
      @NonNull ByteBuffer[] values,
      @Nullable String executionProfileName,
      @Nullable DriverExecutionProfile executionProfile,
      @Nullable CqlIdentifier routingKeyspace,
      @Nullable ByteBuffer routingKey,
      @Nullable Token routingToken,
      @NonNull Map<String, ByteBuffer> customPayload,
      @Nullable Boolean idempotent,
      boolean tracing,
      long timestamp,
      @Nullable ByteBuffer pagingState,
      int pageSize,
      @Nullable ConsistencyLevel consistencyLevel,
      @Nullable ConsistencyLevel serialConsistencyLevel,
      @Nullable Duration timeout,
      @NonNull CodecRegistry codecRegistry,
      @NonNull ProtocolVersion protocolVersion) {
    this.preparedStatement = preparedStatement;
    this.variableDefinitions = variableDefinitions;
    this.values = values;
    this.executionProfileName = executionProfileName;
    this.executionProfile = executionProfile;
    this.routingKeyspace = routingKeyspace;
    this.routingKey = routingKey;
    this.routingToken = routingToken;
    for (Map.Entry<String, ByteBuffer> entry : customPayload.entrySet()) {
      this.addCustomPayload(entry.getKey(), entry.getValue());
    }
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
  }

  public BoundStatementBuilder(@NonNull BoundStatement template) {
    super(template);
    this.preparedStatement = template.getPreparedStatement();
    this.variableDefinitions = template.getPreparedStatement().getVariableDefinitions();
    this.values = template.getValues().toArray(new ByteBuffer[this.variableDefinitions.size()]);
    this.codecRegistry = template.codecRegistry();
    this.protocolVersion = template.protocolVersion();
    this.node = template.getNode();
  }

  /** The prepared statement that was used to create this statement. */
  @NonNull
  public PreparedStatement getPreparedStatement() {
    return preparedStatement;
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
  public BoundStatementBuilder setBytesUnsafe(int i, ByteBuffer v) {
    values[i] = v;
    return this;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return values[i];
  }

  @Override
  public int size() {
    return values.length;
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return variableDefinitions.get(i).getType();
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

  @NonNull
  @Override
  public BoundStatement build() {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        executionProfileName,
        executionProfile,
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
        codecRegistry,
        protocolVersion,
        node,
        nowInSeconds);
  }
}
