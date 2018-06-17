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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A failure to reach the required consistency level during the execution of a query.
 *
 * <p>Such an exception is returned when the query has been tried by Cassandra but cannot be
 * achieved with the requested consistency level because either:
 *
 * <ul>
 *   <li>the coordinator did not receive enough replica responses within the rpc timeout set for
 *       Cassandra;
 *   <li>some replicas replied with an error.
 * </ul>
 */
public abstract class QueryConsistencyException extends QueryExecutionException {

  private final ConsistencyLevel consistencyLevel;
  private final int received;
  private final int blockFor;

  protected QueryConsistencyException(
      @NonNull Node coordinator,
      @NonNull String message,
      @NonNull ConsistencyLevel consistencyLevel,
      int received,
      int blockFor,
      ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(coordinator, message, executionInfo, writableStackTrace);
    this.consistencyLevel = consistencyLevel;
    this.received = received;
    this.blockFor = blockFor;
  }

  /** The consistency level of the operation that failed. */
  @NonNull
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  /** The number of replica that had acknowledged/responded to the operation before it failed. */
  public int getReceived() {
    return received;
  }

  /**
   * The minimum number of replica acknowledgements/responses that were required to fulfill the
   * operation.
   */
  public int getBlockFor() {
    return blockFor;
  }
}
