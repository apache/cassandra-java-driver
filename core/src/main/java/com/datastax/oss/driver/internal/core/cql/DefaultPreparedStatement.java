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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultPreparedStatement implements PreparedStatement {

  private final ByteBuffer id;
  private final RepreparePayload repreparePayload;
  private final ColumnDefinitions variableDefinitions;
  private final ColumnDefinitions resultSetDefinitions;
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
      ColumnDefinitions resultSetDefinitions,
      String configProfileName,
      DriverConfigProfile configProfile,
      String keyspace,
      Map<String, ByteBuffer> customPayloadForBoundStatements,
      Boolean idempotent,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion,
      Map<String, ByteBuffer> customPayloadForPrepare) {
    this.id = id;
    // It's important that we keep a reference to this object, so that it only gets evicted from
    // the map in DefaultSession if no client reference the PreparedStatement anymore.
    this.repreparePayload = new RepreparePayload(query, keyspace, customPayloadForPrepare);
    this.variableDefinitions = variableDefinitions;
    this.resultSetDefinitions = resultSetDefinitions;
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
  public ColumnDefinitions getResultSetDefinitions() {
    return resultSetDefinitions;
  }

  @Override
  public BoundStatement bind() {
    return new DefaultBoundStatement(
        this,
        variableDefinitions,
        configProfileName,
        configProfile,
        repreparePayload.keyspace,
        customPayloadForBoundStatements,
        idempotent,
        codecRegistry,
        protocolVersion);
  }

  public RepreparePayload getRepreparePayload() {
    return this.repreparePayload;
  }
}
