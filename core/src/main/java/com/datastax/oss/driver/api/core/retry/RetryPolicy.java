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
package com.datastax.oss.driver.api.core.retry;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.OverloadedException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.ServerError;
import com.datastax.oss.driver.api.core.servererrors.TruncateException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Defines the behavior to adopt when a request fails.
 *
 * <p>For each request, the driver gets a "query plan" (a list of coordinators to try) from the
 * {@link LoadBalancingPolicy}, and tries each node in sequence. This policy is invoked if the
 * request to that node fails.
 *
 * <p>The methods of this interface are invoked on I/O threads, therefore <b>implementations should
 * never block</b>. In particular, don't call {@link Thread#sleep(long)} to retry after a delay:
 * this would prevent asynchronous processing of other requests, and very negatively impact
 * throughput. If the application needs to back off and retry later, this should be implemented in
 * client code, not in this policy.
 */
public interface RetryPolicy extends AutoCloseable {

  /**
   * Whether to retry when the server replied with a {@code READ_TIMEOUT} error; this indicates a
   * <b>server-side</b> timeout during a read query, i.e. some replicas did not reply to the
   * coordinator in time.
   *
   * @param request the request that timed out.
   * @param cl the requested consistency level.
   * @param blockFor the minimum number of replica acknowledgements/responses that were required to
   *     fulfill the operation.
   * @param received the number of replica that had acknowledged/responded to the operation before
   *     it failed.
   * @param dataPresent whether the actual data was amongst the received replica responses. See
   *     {@link ReadTimeoutException#wasDataPresent()}.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   * @deprecated As of version 4.10, use {@link #onReadTimeoutVerdict(Request, ConsistencyLevel,
   *     int, int, boolean, int)} instead.
   */
  @Deprecated
  RetryDecision onReadTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount);

  /**
   * Whether to retry when the server replied with a {@code READ_TIMEOUT} error; this indicates a
   * <b>server-side</b> timeout during a read query, i.e. some replicas did not reply to the
   * coordinator in time.
   *
   * @param request the request that timed out.
   * @param cl the requested consistency level.
   * @param blockFor the minimum number of replica acknowledgements/responses that were required to
   *     fulfill the operation.
   * @param received the number of replica that had acknowledged/responded to the operation before
   *     it failed.
   * @param dataPresent whether the actual data was amongst the received replica responses. See
   *     {@link ReadTimeoutException#wasDataPresent()}.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  default RetryVerdict onReadTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    RetryDecision decision =
        onReadTimeout(request, cl, blockFor, received, dataPresent, retryCount);
    return () -> decision;
  }

  /**
   * Whether to retry when the server replied with a {@code WRITE_TIMEOUT} error; this indicates a
   * <b>server-side</b> timeout during a write query, i.e. some replicas did not reply to the
   * coordinator in time.
   *
   * <p>Note that this method will only be invoked for {@link Request#isIdempotent()} idempotent}
   * requests: when a write times out, it is impossible to determine with 100% certainty whether the
   * mutation was applied or not, so the write is never safe to retry; the driver will rethrow the
   * error directly, without invoking the retry policy.
   *
   * @param request the request that timed out.
   * @param cl the requested consistency level.
   * @param writeType the type of the write for which the timeout was raised.
   * @param blockFor the minimum number of replica acknowledgements/responses that were required to
   *     fulfill the operation.
   * @param received the number of replica that had acknowledged/responded to the operation before
   *     it failed.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   * @deprecated As of version 4.10, use {@link #onWriteTimeoutVerdict(Request, ConsistencyLevel,
   *     WriteType, int, int, int)} instead.
   */
  @Deprecated
  RetryDecision onWriteTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount);

  /**
   * Whether to retry when the server replied with a {@code WRITE_TIMEOUT} error; this indicates a
   * <b>server-side</b> timeout during a write query, i.e. some replicas did not reply to the
   * coordinator in time.
   *
   * <p>Note that this method will only be invoked for {@link Request#isIdempotent()} idempotent}
   * requests: when a write times out, it is impossible to determine with 100% certainty whether the
   * mutation was applied or not, so the write is never safe to retry; the driver will rethrow the
   * error directly, without invoking the retry policy.
   *
   * @param request the request that timed out.
   * @param cl the requested consistency level.
   * @param writeType the type of the write for which the timeout was raised.
   * @param blockFor the minimum number of replica acknowledgements/responses that were required to
   *     fulfill the operation.
   * @param received the number of replica that had acknowledged/responded to the operation before
   *     it failed.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  default RetryVerdict onWriteTimeoutVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    RetryDecision decision = onWriteTimeout(request, cl, writeType, blockFor, received, retryCount);
    return () -> decision;
  }

  /**
   * Whether to retry when the server replied with an {@code UNAVAILABLE} error; this indicates that
   * the coordinator determined that there were not enough replicas alive to perform a query with
   * the requested consistency level.
   *
   * @param request the request that timed out.
   * @param cl the requested consistency level.
   * @param required the number of replica acknowledgements/responses required to perform the
   *     operation (with its required consistency level).
   * @param alive the number of replicas that were known to be alive by the coordinator node when it
   *     tried to execute the operation.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   * @deprecated As of version 4.10, use {@link #onUnavailableVerdict(Request, ConsistencyLevel,
   *     int, int, int)} instead.
   */
  @Deprecated
  RetryDecision onUnavailable(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount);

  /**
   * Whether to retry when the server replied with an {@code UNAVAILABLE} error; this indicates that
   * the coordinator determined that there were not enough replicas alive to perform a query with
   * the requested consistency level.
   *
   * @param request the request that timed out.
   * @param cl the requested consistency level.
   * @param required the number of replica acknowledgements/responses required to perform the
   *     operation (with its required consistency level).
   * @param alive the number of replicas that were known to be alive by the coordinator node when it
   *     tried to execute the operation.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  default RetryVerdict onUnavailableVerdict(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    RetryDecision decision = onUnavailable(request, cl, required, alive, retryCount);
    return () -> decision;
  }

  /**
   * Whether to retry when a request was aborted before we could get a response from the server.
   *
   * <p>This can happen in two cases: if the connection was closed due to an external event (this
   * will manifest as a {@link ClosedConnectionException}, or {@link HeartbeatException} for a
   * heartbeat failure); or if there was an unexpected error while decoding the response (this can
   * only be a driver bug).
   *
   * <p>Note that this method will only be invoked for {@linkplain Request#isIdempotent()
   * idempotent} requests: when execution was aborted before getting a response, it is impossible to
   * determine with 100% certainty whether a mutation was applied or not, so a write is never safe
   * to retry; the driver will rethrow the error directly, without invoking the retry policy.
   *
   * @param request the request that was aborted.
   * @param error the error.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   * @deprecated As of version 4.10, use {@link #onRequestAbortedVerdict(Request, Throwable, int)}
   *     instead.
   */
  @Deprecated
  RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount);

  /**
   * Whether to retry when a request was aborted before we could get a response from the server.
   *
   * <p>This can happen in two cases: if the connection was closed due to an external event (this
   * will manifest as a {@link ClosedConnectionException}, or {@link HeartbeatException} for a
   * heartbeat failure); or if there was an unexpected error while decoding the response (this can
   * only be a driver bug).
   *
   * <p>Note that this method will only be invoked for {@linkplain Request#isIdempotent()
   * idempotent} requests: when execution was aborted before getting a response, it is impossible to
   * determine with 100% certainty whether a mutation was applied or not, so a write is never safe
   * to retry; the driver will rethrow the error directly, without invoking the retry policy.
   *
   * @param request the request that was aborted.
   * @param error the error.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  default RetryVerdict onRequestAbortedVerdict(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    RetryDecision decision = onRequestAborted(request, error, retryCount);
    return () -> decision;
  }

  /**
   * Whether to retry when the server replied with a recoverable error (other than {@code
   * READ_TIMEOUT}, {@code WRITE_TIMEOUT} or {@code UNAVAILABLE}).
   *
   * <p>This can happen for the following errors: {@link OverloadedException}, {@link ServerError},
   * {@link TruncateException}, {@link ReadFailureException}, {@link WriteFailureException}.
   *
   * <p>The following errors are handled internally by the driver, and therefore will <b>never</b>
   * be encountered in this method:
   *
   * <ul>
   *   <li>{@link BootstrappingException}: always retried on the next node;
   *   <li>{@link QueryValidationException} (and its subclasses), {@link FunctionFailureException}
   *       and {@link ProtocolError}: always rethrown.
   * </ul>
   *
   * <p>Note that this method will only be invoked for {@link Request#isIdempotent()} idempotent}
   * requests: when execution was aborted before getting a response, it is impossible to determine
   * with 100% certainty whether a mutation was applied or not, so a write is never safe to retry;
   * the driver will rethrow the error directly, without invoking the retry policy.
   *
   * @param request the request that failed.
   * @param error the error.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   * @deprecated As of version 4.10, use {@link #onErrorResponseVerdict(Request,
   *     CoordinatorException, int)} instead.
   */
  @Deprecated
  RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount);

  /**
   * Whether to retry when the server replied with a recoverable error (other than {@code
   * READ_TIMEOUT}, {@code WRITE_TIMEOUT} or {@code UNAVAILABLE}).
   *
   * <p>This can happen for the following errors: {@link OverloadedException}, {@link ServerError},
   * {@link TruncateException}, {@link ReadFailureException}, {@link WriteFailureException}.
   *
   * <p>The following errors are handled internally by the driver, and therefore will <b>never</b>
   * be encountered in this method:
   *
   * <ul>
   *   <li>{@link BootstrappingException}: always retried on the next node;
   *   <li>{@link QueryValidationException} (and its subclasses), {@link FunctionFailureException}
   *       and {@link ProtocolError}: always rethrown.
   * </ul>
   *
   * <p>Note that this method will only be invoked for {@link Request#isIdempotent()} idempotent}
   * requests: when execution was aborted before getting a response, it is impossible to determine
   * with 100% certainty whether a mutation was applied or not, so a write is never safe to retry;
   * the driver will rethrow the error directly, without invoking the retry policy.
   *
   * @param request the request that failed.
   * @param error the error.
   * @param retryCount how many times the retry policy has been invoked already for this request
   *     (not counting the current invocation).
   */
  default RetryVerdict onErrorResponseVerdict(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    RetryDecision decision = onErrorResponse(request, error, retryCount);
    return () -> decision;
  }

  /** Called when the cluster that this policy is associated with closes. */
  @Override
  void close();
}
