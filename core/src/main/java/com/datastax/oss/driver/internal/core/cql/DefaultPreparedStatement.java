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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultPreparedStatement implements PreparedStatement {

  private final ByteBuffer id;
  private final RepreparePayload repreparePayload;
  private final ColumnDefinitions variableDefinitions;
  private final List<Integer> partitionKeyIndices;
  private volatile ResultMetadata resultMetadata;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;
  private final String executionProfileNameForBoundStatements;
  private final DriverExecutionProfile executionProfileForBoundStatements;
  private final ByteBuffer pagingStateForBoundStatements;
  private final CqlIdentifier routingKeyspaceForBoundStatements;
  private final ByteBuffer routingKeyForBoundStatements;
  private final Token routingTokenForBoundStatements;
  private final Map<String, ByteBuffer> customPayloadForBoundStatements;
  private final Boolean areBoundStatementsIdempotent;
  private final boolean areBoundStatementsTracing;
  private final int pageSizeForBoundStatements;
  private final ConsistencyLevel consistencyLevelForBoundStatements;
  private final ConsistencyLevel serialConsistencyLevelForBoundStatements;
  private final Duration timeoutForBoundStatements;

  public DefaultPreparedStatement(
      ByteBuffer id,
      String query,
      ColumnDefinitions variableDefinitions,
      List<Integer> partitionKeyIndices,
      ByteBuffer resultMetadataId,
      ColumnDefinitions resultSetDefinitions,
      CqlIdentifier keyspace,
      Map<String, ByteBuffer> customPayloadForPrepare,
      String executionProfileNameForBoundStatements,
      DriverExecutionProfile executionProfileForBoundStatements,
      CqlIdentifier routingKeyspaceForBoundStatements,
      ByteBuffer routingKeyForBoundStatements,
      Token routingTokenForBoundStatements,
      Map<String, ByteBuffer> customPayloadForBoundStatements,
      Boolean areBoundStatementsIdempotent,
      Duration timeoutForBoundStatements,
      ByteBuffer pagingStateForBoundStatements,
      int pageSizeForBoundStatements,
      ConsistencyLevel consistencyLevelForBoundStatements,
      ConsistencyLevel serialConsistencyLevelForBoundStatements,
      boolean areBoundStatementsTracing,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    this.id = id;
    this.partitionKeyIndices = partitionKeyIndices;
    // It's important that we keep a reference to this object, so that it only gets evicted from
    // the map in DefaultSession if no client reference the PreparedStatement anymore.
    this.repreparePayload = new RepreparePayload(id, query, keyspace, customPayloadForPrepare);
    this.variableDefinitions = variableDefinitions;
    this.resultMetadata = new ResultMetadata(resultMetadataId, resultSetDefinitions);

    this.executionProfileNameForBoundStatements = executionProfileNameForBoundStatements;
    this.executionProfileForBoundStatements = executionProfileForBoundStatements;
    this.routingKeyspaceForBoundStatements = routingKeyspaceForBoundStatements;
    this.routingKeyForBoundStatements = routingKeyForBoundStatements;
    this.routingTokenForBoundStatements = routingTokenForBoundStatements;
    this.customPayloadForBoundStatements = customPayloadForBoundStatements;
    this.areBoundStatementsIdempotent = areBoundStatementsIdempotent;
    this.timeoutForBoundStatements = timeoutForBoundStatements;
    this.pagingStateForBoundStatements = pagingStateForBoundStatements;
    this.pageSizeForBoundStatements = pageSizeForBoundStatements;
    this.consistencyLevelForBoundStatements = consistencyLevelForBoundStatements;
    this.serialConsistencyLevelForBoundStatements = serialConsistencyLevelForBoundStatements;
    this.areBoundStatementsTracing = areBoundStatementsTracing;

    this.codecRegistry = codecRegistry;
    this.protocolVersion = protocolVersion;
  }

  @NonNull
  @Override
  public ByteBuffer getId() {
    return id;
  }

  @NonNull
  @Override
  public String getQuery() {
    return repreparePayload.query;
  }

  @NonNull
  @Override
  public ColumnDefinitions getVariableDefinitions() {
    return variableDefinitions;
  }

  @NonNull
  @Override
  public List<Integer> getPartitionKeyIndices() {
    return partitionKeyIndices;
  }

  @Override
  public ByteBuffer getResultMetadataId() {
    return resultMetadata.resultMetadataId;
  }

  @NonNull
  @Override
  public ColumnDefinitions getResultSetDefinitions() {
    return resultMetadata.resultSetDefinitions;
  }

  @Override
  public void setResultMetadata(
      @NonNull ByteBuffer newResultMetadataId, @NonNull ColumnDefinitions newResultSetDefinitions) {
    this.resultMetadata = new ResultMetadata(newResultMetadataId, newResultSetDefinitions);
  }

  @NonNull
  @Override
  public BoundStatement bind(@NonNull Object... values) {
    return new DefaultBoundStatement(
        this,
        variableDefinitions,
        ValuesHelper.encodePreparedValues(
            values, variableDefinitions, codecRegistry, protocolVersion),
        executionProfileNameForBoundStatements,
        executionProfileForBoundStatements,
        routingKeyspaceForBoundStatements,
        routingKeyForBoundStatements,
        routingTokenForBoundStatements,
        customPayloadForBoundStatements,
        areBoundStatementsIdempotent,
        areBoundStatementsTracing,
        Statement.NO_DEFAULT_TIMESTAMP,
        pagingStateForBoundStatements,
        pageSizeForBoundStatements,
        consistencyLevelForBoundStatements,
        serialConsistencyLevelForBoundStatements,
        timeoutForBoundStatements,
        codecRegistry,
        protocolVersion,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }

  @NonNull
  @Override
  public BoundStatementBuilder boundStatementBuilder(@NonNull Object... values) {
    return new BoundStatementBuilder(
        this,
        variableDefinitions,
        ValuesHelper.encodePreparedValues(
            values, variableDefinitions, codecRegistry, protocolVersion),
        executionProfileNameForBoundStatements,
        executionProfileForBoundStatements,
        routingKeyspaceForBoundStatements,
        routingKeyForBoundStatements,
        routingTokenForBoundStatements,
        customPayloadForBoundStatements,
        areBoundStatementsIdempotent,
        areBoundStatementsTracing,
        Statement.NO_DEFAULT_TIMESTAMP,
        pagingStateForBoundStatements,
        pageSizeForBoundStatements,
        consistencyLevelForBoundStatements,
        serialConsistencyLevelForBoundStatements,
        timeoutForBoundStatements,
        codecRegistry,
        protocolVersion);
  }

  public RepreparePayload getRepreparePayload() {
    return this.repreparePayload;
  }

  private static class ResultMetadata {
    private ByteBuffer resultMetadataId;
    private ColumnDefinitions resultSetDefinitions;

    private ResultMetadata(ByteBuffer resultMetadataId, ColumnDefinitions resultSetDefinitions) {
      this.resultMetadataId = resultMetadataId;
      this.resultSetDefinitions = resultSetDefinitions;
    }
  }
}
