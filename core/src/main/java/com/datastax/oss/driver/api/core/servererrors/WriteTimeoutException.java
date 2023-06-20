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
package com.datastax.oss.driver.api.core.servererrors;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A server-side timeout during a write query.
 *
 * <p>This exception is processed by {@link RetryPolicy#onWriteTimeoutVerdict(Request,
 * ConsistencyLevel, WriteType, int, int, int)}, which will decide if it is rethrown directly to the
 * client or if the request should be retried. If all other tried nodes also fail, this exception
 * will appear in the {@link AllNodesFailedException} thrown to the client.
 */
public class WriteTimeoutException extends QueryConsistencyException {

  private final WriteType writeType;

  public WriteTimeoutException(
      @NonNull Node coordinator,
      @NonNull ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      @NonNull WriteType writeType) {
    this(
        coordinator,
        String.format(
            "Cassandra timeout during %s write query at consistency %s "
                + "(%d replica were required but only %d acknowledged the write)",
            writeType, consistencyLevel, blockFor, received),
        consistencyLevel,
        received,
        blockFor,
        writeType,
        null,
        false);
  }

  private WriteTimeoutException(
      @NonNull Node coordinator,
      @NonNull String message,
      @NonNull ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      @NonNull WriteType writeType,
      @Nullable ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(
        coordinator,
        message,
        consistencyLevel,
        received,
        blockFor,
        executionInfo,
        writableStackTrace);
    this.writeType = writeType;
  }

  /** The type of the write for which a timeout was raised. */
  @NonNull
  public WriteType getWriteType() {
    return writeType;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new WriteTimeoutException(
        getCoordinator(),
        getMessage(),
        getConsistencyLevel(),
        getReceived(),
        getBlockFor(),
        writeType,
        getExecutionInfo(),
        true);
  }
}
