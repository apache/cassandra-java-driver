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
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.data.ValuesHelper;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class DefaultPreparedStatement implements PreparedStatement {

  private final ByteBuffer id;
  private final RepreparePayload repreparePayload;
  private final ColumnDefinitions variableDefinitions;
  private final List<Integer> primaryKeyIndices;
  private volatile ResultMetadata resultMetadata;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;
  // The options to propagate to the bound statements:
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final Map<String, ByteBuffer> customPayloadForBoundStatements;
  private final Boolean idempotent;

  public DefaultPreparedStatement(
      ByteBuffer id,
      String query,
      ColumnDefinitions variableDefinitions,
      List<Integer> primaryKeyIndices,
      ByteBuffer resultMetadataId,
      ColumnDefinitions resultSetDefinitions,
      String configProfileName,
      DriverConfigProfile configProfile,
      CqlIdentifier keyspace,
      Map<String, ByteBuffer> customPayloadForBoundStatements,
      Boolean idempotent,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion,
      Map<String, ByteBuffer> customPayloadForPrepare) {
    this.id = id;
    this.primaryKeyIndices = primaryKeyIndices;
    // It's important that we keep a reference to this object, so that it only gets evicted from
    // the map in DefaultSession if no client reference the PreparedStatement anymore.
    this.repreparePayload = new RepreparePayload(id, query, keyspace, customPayloadForPrepare);
    this.variableDefinitions = variableDefinitions;
    this.resultMetadata = new ResultMetadata(resultMetadataId, resultSetDefinitions);
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.customPayloadForBoundStatements = customPayloadForBoundStatements;
    this.idempotent = idempotent;
    this.codecRegistry = codecRegistry;
    this.protocolVersion = protocolVersion;
  }

  @Override
  public ByteBuffer getId() {
    return id;
  }

  @Override
  public String getQuery() {
    return repreparePayload.query;
  }

  @Override
  public ColumnDefinitions getVariableDefinitions() {
    return variableDefinitions;
  }

  @Override
  public List<Integer> getPrimaryKeyIndices() {
    return primaryKeyIndices;
  }

  @Override
  public ByteBuffer getResultMetadataId() {
    return resultMetadata.resultMetadataId;
  }

  @Override
  public ColumnDefinitions getResultSetDefinitions() {
    return resultMetadata.resultSetDefinitions;
  }

  @Override
  public void setResultMetadata(
      ByteBuffer newResultMetadataId, ColumnDefinitions newResultSetDefinitions) {
    this.resultMetadata = new ResultMetadata(newResultMetadataId, newResultSetDefinitions);
  }

  @Override
  public BoundStatement bind(Object... values) {
    return new DefaultBoundStatement(
        this,
        variableDefinitions,
        ValuesHelper.encodePreparedValues(
            values, variableDefinitions, codecRegistry, protocolVersion),
        configProfileName,
        configProfile,
        // If the prepared statement had a per-request keyspace, we want to use that as the routing
        // keyspace.
        repreparePayload.keyspace,
        null,
        null,
        customPayloadForBoundStatements,
        idempotent,
        false,
        Long.MIN_VALUE,
        null,
        codecRegistry,
        protocolVersion);
  }

  @Override
  public BoundStatementBuilder boundStatementBuilder(Object... values) {
    return new BoundStatementBuilder(
        this,
        variableDefinitions,
        ValuesHelper.encodePreparedValues(
            values, variableDefinitions, codecRegistry, protocolVersion),
        repreparePayload.keyspace,
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
