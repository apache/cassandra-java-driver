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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultBoundStatement implements BoundStatement {

  private final DefaultPreparedStatement preparedStatement;
  private final ColumnDefinitions variableDefinitions;
  private final List<ByteBuffer> values;
  private String configProfileName;
  private DriverConfigProfile configProfile;
  private final String keyspace;
  private Map<String, ByteBuffer> customPayload;
  private Boolean idempotent;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;
  private boolean tracing;
  private long timestamp = Long.MIN_VALUE;
  private ByteBuffer pagingState;

  public DefaultBoundStatement(
      DefaultPreparedStatement preparedStatement,
      ColumnDefinitions variableDefinitions,
      String configProfileName,
      DriverConfigProfile configProfile,
      String keyspace,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    this.preparedStatement = preparedStatement;
    this.variableDefinitions = variableDefinitions;
    this.values = new ArrayList<>(variableDefinitions.size());
    for (int i = 0; i < variableDefinitions.size(); i++) {
      this.values.add(null);
    }
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.keyspace = keyspace;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
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
    return values.get(i);
  }

  @Override
  public BoundStatement setBytesUnsafe(int i, ByteBuffer v) {
    values.set(i, v);
    return this;
  }

  @Override
  public DefaultPreparedStatement getPreparedStatement() {
    return preparedStatement;
  }

  @Override
  public List<ByteBuffer> getValues() {
    return values;
  }

  @Override
  public String getConfigProfileName() {
    return configProfileName;
  }

  @Override
  public BoundStatement setConfigProfileName(String configProfileName) {
    this.configProfileName = configProfileName;
    return this;
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @Override
  public BoundStatement setConfigProfile(DriverConfigProfile configProfile) {
    this.configProfile = configProfile;
    return this;
  }

  @Override
  public String getKeyspace() {
    return keyspace;
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @Override
  public BoundStatement setCustomPayload(Map<String, ByteBuffer> customPayload) {
    this.customPayload = customPayload;
    return this;
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Override
  public BoundStatement setIdempotent(Boolean idempotent) {
    this.idempotent = idempotent;
    return this;
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public BoundStatement setTracing() {
    this.tracing = true;
    return this;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public BoundStatement setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public Statement copy(ByteBuffer newPagingState) {
    this.pagingState = newPagingState;
    return this;
  }
}
