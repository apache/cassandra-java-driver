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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DefaultBatchStatement implements BatchStatement {

  private final BatchType batchType;
  private final List<BatchableStatement> statements;
  private String configProfileName;
  private DriverConfigProfile configProfile;
  private final String keyspace;
  private Map<String, ByteBuffer> customPayload;
  private Boolean idempotent;
  private boolean tracing;
  private long timestamp;
  private ByteBuffer pagingState;

  public DefaultBatchStatement(BatchType batchType) {
    this(
        batchType,
        new ArrayList<>(),
        null,
        null,
        null,
        Collections.emptyMap(),
        false,
        false,
        Long.MIN_VALUE,
        null);
  }

  private DefaultBatchStatement(
      BatchType batchType,
      List<BatchableStatement> statements,
      String configProfileName,
      DriverConfigProfile configProfile,
      String keyspace,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      long timestamp,
      ByteBuffer pagingState) {
    this.batchType = batchType;
    this.statements = statements;
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.keyspace = keyspace;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
    this.tracing = tracing;
    this.timestamp = timestamp;
    this.pagingState = pagingState;
  }

  @Override
  public BatchType getBatchType() {
    return batchType;
  }

  @Override
  public BatchStatement add(BatchableStatement statement) {
    if (statements.size() >= 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    } else if (statement instanceof SimpleStatement
        && ((SimpleStatement) statement).getNamedValues().size() > 0) {
      throw new IllegalArgumentException(
          String.format(
              "Batch statements cannot contain simple statements with named values "
                  + "(offending statement: %s)",
              ((SimpleStatement) statement).getQuery()));
    } else {
      statements.add(statement);
      return this;
    }
  }

  @Override
  public int size() {
    return statements.size();
  }

  @Override
  public BatchStatement clear() {
    statements.clear();
    return this;
  }

  @Override
  public Iterator<BatchableStatement> iterator() {
    return statements.iterator();
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public BatchStatement copy(ByteBuffer newPagingState) {
    return new DefaultBatchStatement(
        batchType,
        new ArrayList<>(statements),
        configProfileName,
        configProfile,
        keyspace,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        newPagingState);
  }

  @Override
  public String getConfigProfileName() {
    return configProfileName;
  }

  @Override
  public BatchStatement setConfigProfileName(String configProfileName) {
    this.configProfileName = configProfileName;
    return this;
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @Override
  public DefaultBatchStatement setConfigProfile(DriverConfigProfile configProfile) {
    this.configProfile = configProfile;
    return this;
  }

  @Override
  public String getKeyspace() {
    return keyspace;
  }

  public DefaultBatchStatement setKeyspace(@SuppressWarnings("unused") String keyspace) {
    throw new UnsupportedOperationException(
        "Per-statement keyspaces are not supported currently, "
            + "this feature will be available in Cassandra 4");
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @Override
  public DefaultBatchStatement setCustomPayload(Map<String, ByteBuffer> customPayload) {
    this.customPayload = customPayload;
    return this;
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Override
  public DefaultBatchStatement setIdempotent(Boolean idempotent) {
    this.idempotent = idempotent;
    return this;
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public BatchStatement setTracing() {
    this.tracing = true;
    return this;
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public BatchStatement setTimestamp(long timestamp) {
    this.timestamp = timestamp;
    return this;
  }
}
