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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import com.google.common.collect.ImmutableList;
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

  /** Creates an instance of the default implementation for the given batch type. */
  static BatchStatement newInstance(BatchType batchType) {
    return new DefaultBatchStatement(
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

  /**
   * Creates an instance of the default implementation for the given batch type, containing the
   * given statements.
   */
  static BatchStatement newInstance(BatchType batchType, BatchableStatement<?>... statements) {
    return new DefaultBatchStatement(
        batchType,
        ImmutableList.copyOf(statements),
        null,
        null,
        null,
        Collections.emptyMap(),
        false,
        false,
        Long.MIN_VALUE,
        null);
  }

  /** Returns a builder to create an instance of the default implementation. */
  static BatchStatementBuilder builder(BatchType batchType) {
    return new BatchStatementBuilder(batchType);
  }

  /**
   * Returns a builder to create an instance of the default implementation, copying the fields of
   * the given statement.
   */
  static BatchStatementBuilder builder(BatchStatement template) {
    return new BatchStatementBuilder(template);
  }

  BatchType getBatchType();

  /**
   * Sets the batch type.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  BatchStatement setBatchType(BatchType newBatchType);

  /**
   * Adds a new statement to the batch.
   *
   * <p>Note that, due to protocol limitations, simple statements with named values are currently
   * not supported.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  BatchStatement add(BatchableStatement<?> statement);

  /**
   * Adds new statements to the batch.
   *
   * <p>Note that, due to protocol limitations, simple statements with named values are currently
   * not supported.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  BatchStatement addAll(Iterable<? extends BatchableStatement<?>> statements);

  /** @see #addAll(Iterable) */
  default BatchStatement addAll(BatchableStatement<?>... statements) {
    return addAll(Arrays.asList(statements));
  }

  int size();

  /**
   * Clears the batch, removing all the statements added so far.
   *
   * <p>The driver's built-in implementation is immutable, and returns a new instance from this
   * method. However custom implementations may choose to be mutable and return the same instance.
   */
  BatchStatement clear();
}
