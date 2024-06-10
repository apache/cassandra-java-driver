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

import static com.datastax.oss.driver.api.core.servererrors.WriteType.BATCH;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.BATCH_LOG;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.SIMPLE;
import static com.datastax.oss.driver.api.core.servererrors.WriteType.UNLOGGED_BATCH;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A retry policy that sometimes retries with a lower consistency level than the one initially
 * requested.
 *
 * <p><b>BEWARE</b>: this policy may retry queries using a lower consistency level than the one
 * initially requested. By doing so, it may break consistency guarantees. In other words, if you use
 * this retry policy, there are cases (documented below) where a read at {@code QUORUM} <b>may
 * not</b> see a preceding write at {@code QUORUM}. Furthermore, this policy doesn't always respect
 * datacenter locality; for example, it may downgrade {@code LOCAL_QUORUM} to {@code ONE}, and thus
 * could accidentally send a write that was intended for the local datacenter to another
 * datacenter.Do not use this policy unless you have understood the cases where this can happen and
 * are ok with that.
 *
 * <p>This policy implements the same retries than the {@link DefaultRetryPolicy} policy. But on top
 * of that, it also retries in the following cases:
 *
 * <ul>
 *   <li>On a read timeout: if the number of replicas that responded is greater than one, but lower
 *       than is required by the requested consistency level, the operation is retried at a lower
 *       consistency level.
 *   <li>On a write timeout: if the operation is a {@code WriteType.UNLOGGED_BATCH} and at least one
 *       replica acknowledged the write, the operation is retried at a lower consistency level.
 *       Furthermore, for other operations, if at least one replica acknowledged the write, the
 *       timeout is ignored.
 *   <li>On an unavailable exception: if at least one replica is alive, the operation is retried at
 *       a lower consistency level.
 * </ul>
 *
 * The lower consistency level to use for retries is determined by the following rules:
 *
 * <ul>
 *   <li>if more than 3 replicas responded, use {@code THREE}.
 *   <li>if 1, 2 or 3 replicas responded, use the corresponding level {@code ONE}, {@code TWO} or
 *       {@code THREE}.
 * </ul>
 *
 * Note that if the initial consistency level was {@code EACH_QUORUM}, Cassandra returns the number
 * of live replicas <em>in the datacenter that failed to reach consistency</em>, not the overall
 * number in the cluster. Therefore if this number is 0, we still retry at {@code ONE}, on the
 * assumption that a host may still be up in another datacenter.
 *
 * <p>The reasoning behind this retry policy is the following one. If, based on the information the
 * Cassandra coordinator node returns, retrying the operation with the initially requested
 * consistency has a chance to succeed, do it. Otherwise, if based on this information, we know that
 * <b>the initially requested consistency level cannot be achieved currently</b>, then:
 *
 * <ul>
 *   <li>For writes, ignore the exception (thus silently failing the consistency requirement) if we
 *       know the write has been persisted on at least one replica.
 *   <li>For reads, try reading at a lower consistency level (thus silently failing the consistency
 *       requirement).
 * </ul>
 *
 * In other words, this policy implements the idea that if the requested consistency level cannot be
 * achieved, the next best thing for writes is to make sure the data is persisted, and that reading
 * something is better than reading nothing, even if there is a risk of reading stale data.
 */
public class ConsistencyDowngradingRetryPolicy implements RetryPolicy {

  private static final Logger LOG =
      LoggerFactory.getLogger(ConsistencyDowngradingRetryPolicy.class);

  @VisibleForTesting
  public static final String VERDICT_ON_READ_TIMEOUT =
      "[{}] Verdict on read timeout (consistency: {}, required responses: {}, "
          + "received responses: {}, data retrieved: {}, retries: {}): {}";

  @VisibleForTesting
  public static final String VERDICT_ON_WRITE_TIMEOUT =
      "[{}] Verdict on write timeout (consistency: {}, write type: {}, "
          + "required acknowledgments: {}, received acknowledgments: {}, retries: {}): {}";

  @VisibleForTesting
  public static final String VERDICT_ON_UNAVAILABLE =
      "[{}] Verdict on unavailable exception (consistency: {}, "
          + "required replica: {}, alive replica: {}, retries: {}): {}";

  @VisibleForTesting
  public static final String VERDICT_ON_ABORTED =
      "[{}] Verdict on aborted request (type: {}, message: '{}', retries: {}): {}";

  @VisibleForTesting
  public static final String VERDICT_ON_ERROR =
      "[{}] Verdict on node error (type: {}, message: '{}', retries: {}): {}";

  private final String logPrefix;

  @SuppressWarnings("unused")
  public ConsistencyDowngradingRetryPolicy(
      @NonNull DriverContext context, @NonNull String profileName) {
    this(context.getSessionName() + "|" + profileName);
  }

  public ConsistencyDowngradingRetryPolicy(@NonNull String logPrefix) {
    this.logPrefix = logPrefix;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry. If less replicas responded than
   * required by the consistency level (but at least one replica did respond), the operation is
   * retried at a lower consistency level. If enough replicas responded but data was not retrieved,
   * the operation is retried with the initial consistency level. Otherwise, an exception is thrown.
   */
  @Override
  public RetryVerdict onReadTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    RetryVerdict verdict;
    if (retryCount != 0) {
      verdict = RetryVerdict.RETHROW;
    } else if (cl.isSerial()) {
      // CAS reads are not all that useful in terms of visibility of the writes since CAS write
      // supports the normal consistency levels on the committing phase. So the main use case for
      // CAS reads is probably for when you've timed out on a CAS write and want to make sure what
      // happened. Downgrading in that case would be always wrong so we just special-case to
      // rethrow.
      verdict = RetryVerdict.RETHROW;
    } else if (received < blockFor) {
      verdict = maybeDowngrade(received, cl);
    } else if (!dataPresent) {
      // Retry with same CL since this usually means that enough replica are alive to satisfy the
      // consistency but the coordinator picked a dead one for data retrieval, not having detected
      // that replica as dead yet.
      verdict = RetryVerdict.RETRY_SAME;
    } else {
      // This usually means a digest mismatch, in which case it's pointless to retry since
      // the inconsistency has to be repaired first.
      verdict = RetryVerdict.RETHROW;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          VERDICT_ON_READ_TIMEOUT,
          logPrefix,
          cl,
          blockFor,
          received,
          dataPresent,
          retryCount,
          verdict);
    }
    return verdict;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry. If {@code writeType ==
   * WriteType.BATCH_LOG}, the write is retried with the initial consistency level. If {@code
   * writeType == WriteType.UNLOGGED_BATCH} and at least one replica acknowledged, the write is
   * retried with a lower consistency level (with unlogged batch, a write timeout can <b>always</b>
   * mean that part of the batch haven't been persisted at all, even if {@code receivedAcks > 0}).
   * For other write types ({@code WriteType.SIMPLE} and {@code WriteType.BATCH}), if we know the
   * write has been persisted on at least one replica, we ignore the exception. Otherwise, an
   * exception is thrown.
   */
  @Override
  public RetryVerdict onWriteTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    RetryVerdict verdict;
    if (retryCount != 0) {
      verdict = RetryVerdict.RETHROW;
    } else if (SIMPLE.equals(writeType) || BATCH.equals(writeType)) {
      // Since we provide atomicity, if at least one replica acknowledged the write,
      // there is no point in retrying
      verdict = received > 0 ? RetryVerdict.IGNORE : RetryVerdict.RETHROW;
    } else if (UNLOGGED_BATCH.equals(writeType)) {
      // Since only part of the batch could have been persisted,
      // retry with whatever consistency should allow to persist all
      verdict = maybeDowngrade(received, cl);
    } else if (BATCH_LOG.equals(writeType)) {
      verdict = RetryVerdict.RETRY_SAME;
    } else {
      verdict = RetryVerdict.RETHROW;
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          VERDICT_ON_WRITE_TIMEOUT,
          logPrefix,
          cl,
          writeType,
          blockFor,
          received,
          retryCount,
          verdict);
    }
    return verdict;
  }

  /**
   * {@inheritDoc}
   *
   * <p>This implementation triggers a maximum of one retry. If at least one replica is known to be
   * alive, the operation is retried at a lower consistency level.
   */
  @Override
  public RetryVerdict onUnavailableVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    RetryVerdict verdict;
    if (retryCount != 0) {
      verdict = RetryVerdict.RETHROW;
    } else if (cl.isSerial()) {
      // JAVA-764: if the requested consistency level is serial, it means that the
      // operation failed at the paxos phase of a LWT.
      // Retry on the next host, on the assumption that the initial coordinator could be
      // network-isolated.
      verdict = RetryVerdict.RETRY_NEXT;
    } else {
      verdict = maybeDowngrade(alive, cl);
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace(VERDICT_ON_UNAVAILABLE, logPrefix, cl, required, alive, retryCount, verdict);
    }
    return verdict;
  }

  @Override
  public RetryVerdict onRequestAbortedVerdict(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    RetryVerdict verdict =
        error instanceof ClosedConnectionException || error instanceof HeartbeatException
            ? RetryVerdict.RETRY_NEXT
            : RetryVerdict.RETHROW;
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          VERDICT_ON_ABORTED,
          logPrefix,
          error.getClass().getSimpleName(),
          error.getMessage(),
          retryCount,
          verdict);
    }
    return verdict;
  }

  @Override
  public RetryVerdict onErrorResponseVerdict(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    RetryVerdict verdict =
        error instanceof WriteFailureException || error instanceof ReadFailureException
            ? RetryVerdict.RETHROW
            : RetryVerdict.RETRY_NEXT;
    if (LOG.isTraceEnabled()) {
      LOG.trace(
          VERDICT_ON_ERROR,
          logPrefix,
          error.getClass().getSimpleName(),
          error.getMessage(),
          retryCount,
          verdict);
    }
    return verdict;
  }

  @Override
  @Deprecated
  public RetryDecision onReadTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    throw new UnsupportedOperationException("onReadTimeout");
  }

  @Override
  @Deprecated
  public RetryDecision onWriteTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    throw new UnsupportedOperationException("onWriteTimeout");
  }

  @Override
  @Deprecated
  public RetryDecision onUnavailable(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    throw new UnsupportedOperationException("onUnavailable");
  }

  @Override
  @Deprecated
  public RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    throw new UnsupportedOperationException("onRequestAborted");
  }

  @Override
  @Deprecated
  public RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    throw new UnsupportedOperationException("onErrorResponse");
  }

  @Override
  public void close() {}

  private RetryVerdict maybeDowngrade(int alive, ConsistencyLevel current) {
    if (alive >= 3) {
      return new ConsistencyDowngradingRetryVerdict(ConsistencyLevel.THREE);
    }
    if (alive == 2) {
      return new ConsistencyDowngradingRetryVerdict(ConsistencyLevel.TWO);
    }
    // JAVA-1005: EACH_QUORUM does not report a global number of alive replicas
    // so even if we get 0 alive replicas, there might be a node up in some other datacenter
    if (alive == 1 || current.getProtocolCode() == ConsistencyLevel.EACH_QUORUM.getProtocolCode()) {
      return new ConsistencyDowngradingRetryVerdict(ConsistencyLevel.ONE);
    }
    return RetryVerdict.RETHROW;
  }
}
