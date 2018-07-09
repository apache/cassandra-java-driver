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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultBatchStatement implements BatchStatement {

  private final BatchType batchType;
  private final List<BatchableStatement<?>> statements;
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final CqlIdentifier keyspace;
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

  public DefaultBatchStatement(
      BatchType batchType,
      List<BatchableStatement<?>> statements,
      String configProfileName,
      DriverConfigProfile configProfile,
      CqlIdentifier keyspace,
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
      Duration timeout) {
    this.batchType = batchType;
    this.statements = ImmutableList.copyOf(statements);
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.keyspace = keyspace;
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
  }

  @NonNull
  @Override
  public BatchType getBatchType() {
    return batchType;
  }

  @NonNull
  @Override
  public BatchStatement setBatchType(@NonNull BatchType newBatchType) {
    return new DefaultBatchStatement(
        newBatchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @NonNull
  @Override
  public BatchStatement setKeyspace(@Nullable CqlIdentifier newKeyspace) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        newKeyspace,
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
        timeout);
  }

  @NonNull
  @Override
  public BatchStatement add(@NonNull BatchableStatement<?> statement) {
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
          timeout);
    }
  }

  @NonNull
  @Override
  public BatchStatement addAll(@NonNull Iterable<? extends BatchableStatement<?>> newStatements) {
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
          timeout);
    }
  }

  @Override
  public int size() {
    return statements.size();
  }

  @NonNull
  @Override
  public BatchStatement clear() {
    return new DefaultBatchStatement(
        batchType,
        ImmutableList.of(),
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @NonNull
  @Override
  public Iterator<BatchableStatement<?>> iterator() {
    return statements.iterator();
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @NonNull
  @Override
  public BatchStatement setPagingState(ByteBuffer newPagingState) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public int getPageSize() {
    return pageSize;
  }

  @NonNull
  @Override
  public BatchStatement setPageSize(int newPageSize) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Nullable
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @Override
  public BatchStatement setConsistencyLevel(@Nullable ConsistencyLevel newConsistencyLevel) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Nullable
  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistencyLevel;
  }

  @NonNull
  @Override
  public BatchStatement setSerialConsistencyLevel(
      @Nullable ConsistencyLevel newSerialConsistencyLevel) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public String getConfigProfileName() {
    return configProfileName;
  }

  @NonNull
  @Override
  public BatchStatement setConfigProfileName(@Nullable String newConfigProfileName) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        newConfigProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @NonNull
  @Override
  public DefaultBatchStatement setConfigProfile(@Nullable DriverConfigProfile newProfile) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        newProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public CqlIdentifier getKeyspace() {
    if (keyspace != null) {
      return keyspace;
    } else {
      for (BatchableStatement<?> statement : statements) {
        if (statement instanceof SimpleStatement && statement.getKeyspace() != null) {
          return statement.getKeyspace();
        }
      }
    }
    return null;
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    if (routingKeyspace != null) {
      return routingKeyspace;
    } else {
      for (BatchableStatement<?> statement : statements) {
        CqlIdentifier ks = statement.getRoutingKeyspace();
        if (ks != null) {
          return ks;
        }
      }
    }
    return null;
  }

  @NonNull
  @Override
  public BatchStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public ByteBuffer getRoutingKey() {
    if (routingKey != null) {
      return routingKey;
    } else {
      for (BatchableStatement<?> statement : statements) {
        ByteBuffer key = statement.getRoutingKey();
        if (key != null) {
          return key;
        }
      }
    }
    return null;
  }

  @NonNull
  @Override
  public BatchStatement setRoutingKey(ByteBuffer newRoutingKey) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public Token getRoutingToken() {
    if (routingToken != null) {
      return routingToken;
    } else {
      for (BatchableStatement<?> statement : statements) {
        Token token = statement.getRoutingToken();
        if (token != null) {
          return token;
        }
      }
    }
    return null;
  }

  @NonNull
  @Override
  public BatchStatement setRoutingToken(Token newRoutingToken) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @NonNull
  @Override
  public DefaultBatchStatement setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Nullable
  @Override
  public Duration getTimeout() {
    return null;
  }

  @NonNull
  @Override
  public DefaultBatchStatement setIdempotent(Boolean newIdempotence) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @NonNull
  @Override
  public BatchStatement setTracing(boolean newTracing) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @NonNull
  @Override
  public BatchStatement setTimestamp(long newTimestamp) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        timeout);
  }

  @NonNull
  @Override
  public BatchStatement setTimeout(@Nullable Duration newTimeout) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
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
        newTimeout);
  }
}
