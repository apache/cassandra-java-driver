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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.WriteType;
import com.datastax.oss.driver.api.core.session.Request;

/**
 * A server-side timeout during a write query.
 *
 * <p>This exception is processed by {@link RetryPolicy#onWriteTimeout(Request, ConsistencyLevel,
 * WriteType, int, int, int)}, which will decide if it is rethrown directly to the client or if the
 * request should be retried. If all other tried nodes also fail, this exception will appear in the
 * {@link AllNodesFailedException} thrown to the client.
 */
public class WriteTimeoutException extends QueryConsistencyException {

  private final WriteType writeType;

  public WriteTimeoutException(
      Node coordinator,
      ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      WriteType writeType) {
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
        false);
  }

  private WriteTimeoutException(
      Node coordinator,
      String message,
      ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      WriteType writeType,
      boolean writableStackTrace) {
    super(coordinator, message, consistencyLevel, received, blockFor, writableStackTrace);
    this.writeType = writeType;
  }

  /** The type of the write for which a timeout was raised. */
  public WriteType getWriteType() {
    return writeType;
  }

  @Override
  public DriverException copy() {
    return new WriteTimeoutException(
        getCoordinator(),
        getMessage(),
        getConsistencyLevel(),
        getReceived(),
        getBlockFor(),
        writeType,
        true);
  }
}
