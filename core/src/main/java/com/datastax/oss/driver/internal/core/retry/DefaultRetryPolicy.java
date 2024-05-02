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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default retry policy.
 *
 * <p>This is a very conservative implementation: it triggers a maximum of one retry per request,
 * and only in cases that have a high chance of success (see the method javadocs for detailed
 * explanations of each case).
 *
 * <p>To activate this policy, modify the {@code advanced.retry-policy} section in the driver
 * configuration, for example:
 *
 * <pre>
 * datastax-java-driver {
 *   advanced.retry-policy {
 *     class = DefaultRetryPolicy
 *   }
 * }
 * </pre>
 *
 * See {@code reference.conf} (in the manual or core driver JAR) for more details.
 */
@ThreadSafe
public class DefaultRetryPolicy implements RetryPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultRetryPolicy.class);

  @VisibleForTesting
  public static final String RETRYING_ON_READ_TIMEOUT =
      "[{}] Retrying on read timeout on same host (consistency: {}, required responses: {}, "
          + "received responses: {}, data retrieved: {}, retries: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_WRITE_TIMEOUT =
      "[{}] Retrying on write timeout on same host (consistency: {}, write type: {}, "
          + "required acknowledgments: {}, received acknowledgments: {}, retries: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_UNAVAILABLE =
      "[{}] Retrying on unavailable exception on next host (consistency: {}, "
          + "required replica: {}, alive replica: {}, retries: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_ABORTED =
      "[{}] Retrying on aborted request on next host (retries: {})";

  @VisibleForTesting
  public static final String RETRYING_ON_ERROR =
      "[{}] Retrying on node error on next host (retries: {})";

  private final String logPrefix;

  public DefaultRetryPolicy(DriverContext context, String profileName) {
    this.logPrefix = (context != null ? context.getSessionName() : null) + "|" + profileName;
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
  @Deprecated
  public RetryDecision onReadTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {

    RetryDecision decision =
        (retryCount == 0 && received >= blockFor && !dataPresent)
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY_SAME && LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_READ_TIMEOUT, logPrefix, cl, blockFor, received, false, retryCount);
    }

    return decision;
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
  @Deprecated
  public RetryDecision onWriteTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {

    RetryDecision decision =
        (retryCount == 0 && writeType == DefaultWriteType.BATCH_LOG)
            ? RetryDecision.RETRY_SAME
            : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY_SAME && LOG.isTraceEnabled()) {
      LOG.trace(
          RETRYING_ON_WRITE_TIMEOUT, logPrefix, cl, writeType, blockFor, received, retryCount);
    }
    return decision;
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
  @Deprecated
  public RetryDecision onUnavailable(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {

    RetryDecision decision = (retryCount == 0) ? RetryDecision.RETRY_NEXT : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY_NEXT && LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_UNAVAILABLE, logPrefix, cl, required, alive, retryCount);
    }

    return decision;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation retries on the next node if the connection was closed, and rethrows
   * (assuming a driver bug) in all other cases.
   */
  @Override
  @Deprecated
  public RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {

    RetryDecision decision =
        (error instanceof ClosedConnectionException || error instanceof HeartbeatException)
            ? RetryDecision.RETRY_NEXT
            : RetryDecision.RETHROW;

    if (decision == RetryDecision.RETRY_NEXT && LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_ABORTED, logPrefix, retryCount, error);
    }

    return decision;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation rethrows read and write failures, and retries other errors on the next
   * node.
   */
  @Override
  @Deprecated
  public RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {

    RetryDecision decision =
        (error instanceof ReadFailureException || error instanceof WriteFailureException)
            ? RetryDecision.RETHROW
            : RetryDecision.RETRY_NEXT;

    if (decision == RetryDecision.RETRY_NEXT && LOG.isTraceEnabled()) {
      LOG.trace(RETRYING_ON_ERROR, logPrefix, retryCount, error);
    }

    return decision;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
