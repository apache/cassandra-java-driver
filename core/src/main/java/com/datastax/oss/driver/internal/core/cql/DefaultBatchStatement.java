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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DefaultBatchStatement implements BatchStatement {

  private final BatchType batchType;
  private final List<BatchableStatement<?>> statements;
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final String keyspace;
  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final boolean tracing;
  private final long timestamp;
  private final ByteBuffer pagingState;

  public DefaultBatchStatement(
      BatchType batchType,
      List<BatchableStatement<?>> statements,
      String configProfileName,
      DriverConfigProfile configProfile,
      String keyspace,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      long timestamp,
      ByteBuffer pagingState) {
    this.batchType = batchType;
    this.statements = ImmutableList.copyOf(statements);
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
  public BatchStatement setBatchType(BatchType newBatchType) {
    return new DefaultBatchStatement(
        newBatchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public BatchStatement add(BatchableStatement<?> statement) {
    if (statements.size() >= 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    } else {
      return new DefaultBatchStatement(
          batchType,
          ImmutableList.<BatchableStatement<?>>builder().addAll(statements).add(statement).build(),
          configProfileName,
          configProfile,
          keyspace,
          customPayload,
          idempotent,
          tracing,
          timestamp,
          pagingState);
    }
  }

  @Override
  public BatchStatement addAll(Iterable<? extends BatchableStatement<?>> newStatements) {
    if (statements.size() + Iterables.size(newStatements) > 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    } else {
      return new DefaultBatchStatement(
          batchType,
          ImmutableList.<BatchableStatement<?>>builder()
              .addAll(statements)
              .addAll(newStatements)
              .build(),
          configProfileName,
          configProfile,
          keyspace,
          customPayload,
          idempotent,
          tracing,
          timestamp,
          pagingState);
    }
  }

  @Override
  public int size() {
    return statements.size();
  }

  @Override
  public BatchStatement clear() {
    return new DefaultBatchStatement(
        batchType,
        ImmutableList.of(),
        configProfileName,
        configProfile,
        keyspace,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Iterator<BatchableStatement<?>> iterator() {
    return statements.iterator();
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public BatchStatement setPagingState(ByteBuffer newPagingState) {
    return new DefaultBatchStatement(
        batchType,
        statements,
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
  public BatchStatement setConfigProfileName(String newConfigProfileName) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        newConfigProfileName,
        configProfile,
        keyspace,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @Override
  public DefaultBatchStatement setConfigProfile(DriverConfigProfile newProfile) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        newProfile,
        keyspace,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public String getKeyspace() {
    return keyspace;
  }

  @Override
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
  public DefaultBatchStatement setCustomPayload(Map<String, ByteBuffer> newCustomPayload) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        newCustomPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Override
  public DefaultBatchStatement setIdempotent(Boolean newIdempotence) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        customPayload,
        newIdempotence,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public BatchStatement setTracing(boolean newTracing) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        customPayload,
        idempotent,
        newTracing,
        timestamp,
        pagingState);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public BatchStatement setTimestamp(long newTimestamp) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        customPayload,
        idempotent,
        tracing,
        newTimestamp,
        pagingState);
  }
}
