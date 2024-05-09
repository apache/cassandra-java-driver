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
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import com.datastax.oss.driver.internal.core.time.ServerSideTimestampGenerator;
import com.datastax.oss.driver.internal.core.util.Sizes;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

/**
 * A statement that groups a number of other statements, so that they can be executed as a batch
 * (i.e. sent together as a single protocol frame).
 *
 * <p>The default implementation returned by the driver is <b>immutable</b> and <b>thread-safe</b>.
 * All mutating methods return a new instance. See also the static factory methods and builders in
 * this interface.
 */
public interface BatchStatement extends Statement<BatchStatement>, Iterable<BatchableStatement<?>> {

  /**
   * Creates an instance of the default implementation for the given batch type.
   *
   * <p>Note that the returned object is <b>immutable</b>.
   */
  @NonNull
  static BatchStatement newInstance(@NonNull BatchType batchType) {
    return new DefaultBatchStatement(
        batchType,
        new ArrayList<>(),
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        null,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }

  /**
   * Creates an instance of the default implementation for the given batch type, containing the
   * given statements.
   *
   * <p>Note that the returned object is <b>immutable</b>.
   */
  @NonNull
  static BatchStatement newInstance(
      @NonNull BatchType batchType, @NonNull Iterable<BatchableStatement<?>> statements) {
    return new DefaultBatchStatement(
        batchType,
        ImmutableList.copyOf(statements),
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        null,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }

  /**
   * Creates an instance of the default implementation for the given batch type, containing the
   * given statements.
   *
   * <p>Note that the returned object is <b>immutable</b>.
   */
  @NonNull
  static BatchStatement newInstance(
      @NonNull BatchType batchType, @NonNull BatchableStatement<?>... statements) {
    return new DefaultBatchStatement(
        batchType,
        ImmutableList.copyOf(statements),
        null,
        null,
        null,
        null,
        null,
        null,
        Collections.emptyMap(),
        null,
        false,
        Statement.NO_DEFAULT_TIMESTAMP,
        null,
        Integer.MIN_VALUE,
        null,
        null,
        null,
        null,
        Statement.NO_NOW_IN_SECONDS);
  }

  /**
   * Returns a builder to create an instance of the default implementation.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static BatchStatementBuilder builder(@NonNull BatchType batchType) {
    return new BatchStatementBuilder(batchType);
  }

  /**
   * Returns a builder to create an instance of the default implementation, copying the fields of
   * the given statement.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   */
  @NonNull
  static BatchStatementBuilder builder(@NonNull BatchStatement template) {
    return new BatchStatementBuilder(template);
  }

  @NonNull
  BatchType getBatchType();

  /**
   * Sets the batch type.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  BatchStatement setBatchType(@NonNull BatchType newBatchType);

  /**
   * Sets the CQL keyspace to associate with this batch.
   *
   * <p>If the keyspace is not set explicitly with this method, it will be inferred from the first
   * simple statement in the batch that has a keyspace set (or will be null if no such statement
   * exists).
   *
   * <p>This feature is only available with {@link DefaultProtocolVersion#V5 native protocol v5} or
   * higher. Specifying a per-request keyspace with lower protocol versions will cause a runtime
   * error.
   *
   * @see Request#getKeyspace()
   */
  @NonNull
  BatchStatement setKeyspace(@Nullable CqlIdentifier newKeyspace);

  /**
   * Shortcut for {@link #setKeyspace(CqlIdentifier)
   * setKeyspace(CqlIdentifier.fromCql(newKeyspaceName))}.
   */
  @NonNull
  default BatchStatement setKeyspace(@NonNull String newKeyspaceName) {
    return setKeyspace(CqlIdentifier.fromCql(newKeyspaceName));
  }

  /**
   * Adds a new statement to the batch.
   *
   * <p>Note that, due to protocol limitations, simple statements with named values are currently
   * not supported.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  BatchStatement add(@NonNull BatchableStatement<?> statement);

  /**
   * Adds new statements to the batch.
   *
   * <p>Note that, due to protocol limitations, simple statements with named values are currently
   * not supported.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  BatchStatement addAll(@NonNull Iterable<? extends BatchableStatement<?>> statements);

  /** @see #addAll(Iterable) */
  @NonNull
  default BatchStatement addAll(@NonNull BatchableStatement<?>... statements) {
    return addAll(Arrays.asList(statements));
  }

  /** @return The number of child statements in this batch. */
  int size();

  /**
   * Clears the batch, removing all the statements added so far.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  @NonNull
  BatchStatement clear();

  @Override
  default int computeSizeInBytes(@NonNull DriverContext context) {
    int size = Sizes.minimumStatementSize(this, context);

    // BatchStatement's additional elements to take into account are:
    // - batch type
    // - inner statements (simple or bound)
    // - per-query keyspace
    // - timestamp

    // batch type
    size += PrimitiveSizes.BYTE;

    // inner statements
    size += PrimitiveSizes.SHORT; // number of statements

    for (BatchableStatement<?> batchableStatement : this) {
      size +=
          Sizes.sizeOfInnerBatchStatementInBytes(
              batchableStatement, context.getProtocolVersion(), context.getCodecRegistry());
    }

    // per-query keyspace
    if (getKeyspace() != null) {
      size += PrimitiveSizes.sizeOfString(getKeyspace().asInternal());
    }

    // timestamp
    if (!(context.getTimestampGenerator() instanceof ServerSideTimestampGenerator)
        || getQueryTimestamp() != Statement.NO_DEFAULT_TIMESTAMP) {

      size += PrimitiveSizes.LONG;
    }

    return size;
  }
}
