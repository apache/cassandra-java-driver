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
import java.nio.ByteBuffer;
import java.util.Map;

public class DefaultPreparedStatement implements PreparedStatement {

  private final ByteBuffer id;
  private final String query;
  private final ColumnDefinitions variableDefinitions;
  private final ColumnDefinitions resultSetDefinitions;
  // The options to propagate to the bound statements:
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final String keyspace;
  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;

  public DefaultPreparedStatement(
      ByteBuffer id,
      String query,
      ColumnDefinitions variableDefinitions,
      ColumnDefinitions resultSetDefinitions,
      String configProfileName,
      DriverConfigProfile configProfile,
      String keyspace,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    this.id = id;
    this.query = query;
    this.variableDefinitions = variableDefinitions;
    this.resultSetDefinitions = resultSetDefinitions;
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.keyspace = keyspace;
    this.customPayload = customPayload;
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
    return query;
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
        keyspace,
        customPayload,
        idempotent,
        codecRegistry,
        protocolVersion);
  }
}
