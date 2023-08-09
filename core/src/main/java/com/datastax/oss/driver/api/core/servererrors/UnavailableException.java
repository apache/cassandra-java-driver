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
 * Thrown when the coordinator knows there is not enough replicas alive to perform a query with the
 * requested consistency level.
 *
 * <p>This exception is processed by {@link RetryPolicy#onUnavailableVerdict(Request,
 * ConsistencyLevel, int, int, int)}, which will decide if it is rethrown directly to the client or
 * if the request should be retried. If all other tried nodes also fail, this exception will appear
 * in the {@link AllNodesFailedException} thrown to the client.
 */
public class UnavailableException extends QueryExecutionException {
  private final ConsistencyLevel consistencyLevel;
  private final int required;
  private final int alive;

  public UnavailableException(
      @NonNull Node coordinator,
      @NonNull ConsistencyLevel consistencyLevel,
      int required,
      int alive) {
    this(
        coordinator,
        String.format(
            "Not enough replicas available for query at consistency %s (%d required but only %d alive)",
            consistencyLevel, required, alive),
        consistencyLevel,
        required,
        alive,
        null,
        false);
  }

  private UnavailableException(
      @NonNull Node coordinator,
      @NonNull String message,
      @NonNull ConsistencyLevel consistencyLevel,
      int required,
      int alive,
      ExecutionInfo executionInfo,
      boolean writableStackTrace) {
    super(coordinator, message, executionInfo, writableStackTrace);
    this.consistencyLevel = consistencyLevel;
    this.required = required;
    this.alive = alive;
  }

  /** The consistency level of the operation triggering this exception. */
  @NonNull
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  /**
   * The number of replica acknowledgements/responses required to perform the operation (with its
   * required consistency level).
   */
  public int getRequired() {
    return required;
  }

  /**
   * The number of replicas that were known to be alive by the coordinator node when it tried to
   * execute the operation.
   */
  public int getAlive() {
    return alive;
  }

  @NonNull
  @Override
  public DriverException copy() {
    return new UnavailableException(
        getCoordinator(),
        getMessage(),
        consistencyLevel,
        required,
        alive,
        getExecutionInfo(),
        true);
  }
}
