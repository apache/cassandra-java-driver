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

import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.internal.core.cql.DefaultBatchStatement;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * A statement that groups a number of other statements, so that they can be executed as a batch
 * (i.e. sent together as a single protocol frame).
 *
 * <p>The default implementation returned by the driver is <b>mutable</b> and <b>NOT
 * thread-safe</b>: methods that return {@code BatchStatement} mutate the statement and return the
 * same instance (except {@link #copy(ByteBuffer)}); all methods should be called from the thread
 * that created the statement; if you use {@link Session#executeAsync(Statement)} asynchronous
 * execution}, do not reuse the same instance for successive calls, as you run the risk of modifying
 * the statement before the driver is done processing it.
 */
public interface BatchStatement extends Statement, Iterable<BatchableStatement> {

  /** Creates an instance of the default implementation for the given batch type. */
  static BatchStatement newInstance(BatchType batchType) {
    return new DefaultBatchStatement(batchType);
  }

  BatchType getBatchType();

  /**
   * Adds a new statement to the batch.
   *
   * <p>Note that, due to protocol limitations, simple statements with named values are currently
   * not supported.
   */
  BatchStatement add(BatchableStatement statement);

  default BatchStatement addAll(Iterable<? extends BatchableStatement> statements) {
    BatchStatement result = this;
    for (BatchableStatement statement : statements) {
      result = result.add(statement);
    }
    return result;
  }

  default BatchStatement addAll(BatchableStatement... statements) {
    BatchStatement result = this;
    for (BatchableStatement statement : statements) {
      result = result.add(statement);
    }
    return result;
  }

  int size();

  /** Clears the batch, removing all the statements added so far. */
  BatchStatement clear();

  /**
   * Sets the name of the configuration profile to use.
   *
   * <p>Note that this will be ignored if {@link #getConfigProfile()} return a non-null value.
   */
  BatchStatement setConfigProfileName(String configProfileName);

  /** Sets the configuration profile to use. */
  DefaultBatchStatement setConfigProfile(DriverConfigProfile configProfile);

  /** Sets the custom payload to send alongside the request. */
  DefaultBatchStatement setCustomPayload(Map<String, ByteBuffer> customPayload);

  /**
   * Indicates whether the statement is idempotent.
   *
   * @param idempotent true or false, or {@code null} to use the default defined in the
   *     configuration.
   */
  DefaultBatchStatement setIdempotent(Boolean idempotent);

  /** Request tracing information for this statement. */
  BatchStatement setTracing();

  /**
   * Sets the timestamp for this statement. If left unset, the {@link TimestampGenerator} will
   * assign it when the statement gets executed.
   */
  BatchStatement setTimestamp(long timestamp);
}
