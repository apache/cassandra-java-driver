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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Arrays;
import net.jcip.annotations.NotThreadSafe;

/**
 * A builder to create a batch statement.
 *
 * <p>This class is mutable and not thread-safe.
 */
@NotThreadSafe
public class BatchStatementBuilder extends StatementBuilder<BatchStatementBuilder, BatchStatement> {

  @NonNull private BatchType batchType;
  @Nullable private CqlIdentifier keyspace;
  @NonNull private ImmutableList.Builder<BatchableStatement<?>> statementsBuilder;
  private int statementsCount;

  public BatchStatementBuilder(@NonNull BatchType batchType) {
    this.batchType = batchType;
    this.statementsBuilder = ImmutableList.builder();
  }

  public BatchStatementBuilder(@NonNull BatchStatement template) {
    super(template);
    this.batchType = template.getBatchType();
    this.statementsBuilder = ImmutableList.<BatchableStatement<?>>builder().addAll(template);
    this.statementsCount = template.size();
  }

  /**
   * Sets the CQL keyspace to execute this batch in.
   *
   * @return this builder; never {@code null}.
   * @see BatchStatement#getKeyspace()
   */
  @NonNull
  public BatchStatementBuilder setKeyspace(@NonNull CqlIdentifier keyspace) {
    this.keyspace = keyspace;
    return this;
  }

  /**
   * Sets the CQL keyspace to execute this batch in. Shortcut for {@link #setKeyspace(CqlIdentifier)
   * setKeyspace(CqlIdentifier.fromCql(keyspaceName))}.
   *
   * @return this builder; never {@code null}.
   */
  @NonNull
  public BatchStatementBuilder setKeyspace(@NonNull String keyspaceName) {
    return setKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * Adds a new statement to the batch.
   *
   * @return this builder; never {@code null}.
   * @see BatchStatement#add(BatchableStatement)
   */
  @NonNull
  public BatchStatementBuilder addStatement(@NonNull BatchableStatement<?> statement) {
    if (statementsCount >= 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    }
    statementsCount += 1;
    statementsBuilder.add(statement);
    return this;
  }

  /**
   * Adds new statements to the batch.
   *
   * @return this builder; never {@code null}.
   * @see BatchStatement#addAll(Iterable)
   */
  @NonNull
  public BatchStatementBuilder addStatements(@NonNull Iterable<BatchableStatement<?>> statements) {
    int delta = Iterables.size(statements);
    if (statementsCount + delta > 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    }
    statementsCount += delta;
    statementsBuilder.addAll(statements);
    return this;
  }

  /**
   * Adds new statements to the batch.
   *
   * @return this builder; never {@code null}.
   * @see BatchStatement#addAll(BatchableStatement[])
   */
  @NonNull
  public BatchStatementBuilder addStatements(@NonNull BatchableStatement<?>... statements) {
    return addStatements(Arrays.asList(statements));
  }

  /**
   * Clears all the statements in this batch.
   *
   * @return this builder; never {@code null}.
   */
  @NonNull
  public BatchStatementBuilder clearStatements() {
    statementsBuilder = ImmutableList.builder();
    statementsCount = 0;
    return this;
  }

  /** @return a newly-allocated {@linkplain BatchStatement batch}; never {@code null}.. */
  @Override
  @NonNull
  public BatchStatement build() {
    return new DefaultBatchStatement(
        batchType,
        statementsBuilder.build(),
        executionProfileName,
        executionProfile,
        keyspace,
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
        node,
        nowInSeconds);
  }

  public int getStatementsCount() {
    return this.statementsCount;
  }
}
