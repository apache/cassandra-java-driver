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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.cql.DefaultBoundStatement;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import java.nio.ByteBuffer;
import java.util.Collections;

public class BoundStatementBuilder extends StatementBuilder<BoundStatementBuilder, BoundStatement>
    implements Bindable<BoundStatementBuilder> {

  private final PreparedStatement preparedStatement;
  private final ColumnDefinitions variableDefinitions;
  private final ByteBuffer[] values;
  private final CodecRegistry codecRegistry;
  private final ProtocolVersion protocolVersion;

  public BoundStatementBuilder(
      PreparedStatement preparedStatement,
      ColumnDefinitions variableDefinitions,
      CodecRegistry codecRegistry,
      ProtocolVersion protocolVersion) {
    this.preparedStatement = preparedStatement;
    this.variableDefinitions = variableDefinitions;
    this.values = new ByteBuffer[variableDefinitions.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = ProtocolConstants.UNSET_VALUE;
    }
    this.codecRegistry = codecRegistry;
    this.protocolVersion = protocolVersion;
  }

  public BoundStatementBuilder(BoundStatement template) {
    super(template);
    this.preparedStatement = template.getPreparedStatement();
    this.variableDefinitions = template.getPreparedStatement().getVariableDefinitions();
    this.values = template.getValues().toArray(new ByteBuffer[this.variableDefinitions.size()]);
    this.codecRegistry = template.codecRegistry();
    this.protocolVersion = template.protocolVersion();
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

  @Override
  public DataType getType(int i) {
    return variableDefinitions.get(i).getType();
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
  public BoundStatement build() {
    return new DefaultBoundStatement(
        preparedStatement,
        variableDefinitions,
        values,
        configProfileName,
        configProfile,
        keyspace,
        (customPayloadBuilder == null) ? Collections.emptyMap() : customPayloadBuilder.build(),
        idempotent,
        tracing,
        timestamp,
        pagingState,
        codecRegistry,
        protocolVersion);
  }
}
