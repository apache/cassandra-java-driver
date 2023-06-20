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

/**
 * A server-side timeout during a read query.
 *
 * <p>This exception is processed by {@link RetryPolicy#onReadTimeoutVerdict(Request,
 * ConsistencyLevel, int, int, boolean, int)}, which will decide if it is rethrown directly to the
 * client or if the request should be retried. If all other tried nodes also fail, this exception
 * will appear in the {@link AllNodesFailedException} thrown to the client.
 */
public class ReadTimeoutException extends QueryConsistencyException {

  private final boolean dataPresent;

  public ReadTimeoutException(
      @NonNull Node coordinator,
      @NonNull ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      boolean dataPresent) {
    this(
        coordinator,
        String.format(
            "Cassandra timeout during read query at consistency %s (%s). "
                + "In case this was generated during read repair, the consistency level is not representative of the actual consistency.",
            consistencyLevel, formatDetails(received, blockFor, dataPresent)),
        consistencyLevel,
        received,
        blockFor,
        dataPresent,
        null,
        false);
  }

  private ReadTimeoutException(
      @NonNull Node coordinator,
      @NonNull String message,
      @NonNull ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      boolean dataPresent,
      ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(
        coordinator,
        message,
        consistencyLevel,
        received,
        blockFor,
        executionInfo,
        writableStackTrace);
    this.dataPresent = dataPresent;
  }

  private static String formatDetails(int received, int blockFor, boolean dataPresent) {
    if (received < blockFor) {
      return String.format(
          "%d responses were required but only %d replica responded", blockFor, received);
    } else if (!dataPresent) {
      return "the replica queried for data didn't respond";
    } else {
      return "timeout while waiting for repair of inconsistent replica";
    }
  }

  /**
   * Whether the actual data was amongst the received replica responses.
   *
   * <p>During reads, Cassandra doesn't request data from every replica to minimize internal network
   * traffic. Instead, some replicas are only asked for a checksum of the data. A read timeout may
   * occur even if enough replicas have responded to fulfill the consistency level, if only checksum
   * responses have been received. This method allows to detect that case.
   */
  public boolean wasDataPresent() {
    return dataPresent;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new ReadTimeoutException(
        getCoordinator(),
        getMessage(),
        getConsistencyLevel(),
        getReceived(),
        getBlockFor(),
        dataPresent,
        getExecutionInfo(),
        true);
  }
}
