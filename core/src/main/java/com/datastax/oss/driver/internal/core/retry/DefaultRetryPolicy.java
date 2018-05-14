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
package com.datastax.oss.driver.internal.core.retry;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.DefaultWriteType;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import net.jcip.annotations.ThreadSafe;

/**
 * The default retry policy. This is a very conservative implementation: it triggers a maximum of
 * one retry per request, and only in cases that have a high chance of success (see the method
 * javadocs for detailed explanations of each case).
 */
@ThreadSafe
public class DefaultRetryPolicy implements RetryPolicy {

  public DefaultRetryPolicy(
      @SuppressWarnings("unused") DriverContext context,
      @SuppressWarnings("unused") String profileName) {
    // nothing to do
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry (to the same node), and only if enough
   * replicas had responded to the read request but data was not retrieved amongst those. That
   * usually means that enough replicas are alive to satisfy the consistency, but the coordinator
   * picked a dead one for data retrieval, not having detected that replica as dead yet. The
   * reasoning is that by the time we get the timeout, the dead replica will likely have been
   * detected as dead and the retry has a high chance of success.
   *
   * <p>Otherwise, the exception is rethrown.
   */
  @Override
  public RetryDecision onReadTimeout(
      Request request,
      ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {

    return (retryCount == 0 && received >= blockFor && !dataPresent)
        ? RetryDecision.RETRY_SAME
        : RetryDecision.RETHROW;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry (to the same node), and only for a
   * {@code WriteType.BATCH_LOG} write. The reasoning is that the coordinator tries to write the
   * distributed batch log against a small subset of nodes in the local datacenter; a timeout
   * usually means that none of these nodes were alive but the coordinator hadn't detected them as
   * dead yet. By the time we get the timeout, the dead nodes will likely have been detected as
   * dead, and the retry has thus a high chance of success.
   *
   * <p>Otherwise, the exception is rethrown.
   */
  @Override
  public RetryDecision onWriteTimeout(
      Request request,
      ConsistencyLevel cl,
      WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {

    return (retryCount == 0 && writeType == DefaultWriteType.BATCH_LOG)
        ? RetryDecision.RETRY_SAME
        : RetryDecision.RETHROW;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry, to the next node in the query plan. The
   * rationale is that the first coordinator might have been network-isolated from all other nodes
   * (thinking they're down), but still able to communicate with the client; in that case, retrying
   * on the same host has almost no chance of success, but moving to the next host might solve the
   * issue.
   *
   * <p>Otherwise, the exception is rethrown.
   */
  @Override
  public RetryDecision onUnavailable(
      Request request, ConsistencyLevel cl, int required, int alive, int retryCount) {

    return (retryCount == 0) ? RetryDecision.RETRY_NEXT : RetryDecision.RETHROW;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries on the next node if the connection was closed, and rethrows
   * (assuming a driver bug) in all other cases.
   */
  @Override
  public RetryDecision onRequestAborted(Request request, Throwable error, int retryCount) {
    return (error instanceof ClosedConnectionException || error instanceof HeartbeatException)
        ? RetryDecision.RETRY_NEXT
        : RetryDecision.RETHROW;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation rethrows read and write failures, and retries other errors on the next
   * node.
   */
  @Override
  public RetryDecision onErrorResponse(
      Request request, CoordinatorException error, int retryCount) {
    return (error instanceof ReadFailureException || error instanceof WriteFailureException)
        ? RetryDecision.RETHROW
        : RetryDecision.RETRY_NEXT;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
