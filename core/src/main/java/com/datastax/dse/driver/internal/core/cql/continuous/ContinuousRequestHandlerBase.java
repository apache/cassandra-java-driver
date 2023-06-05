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
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.internal.core.DseProtocolFeature;
import com.datastax.dse.driver.internal.core.cql.DseConversions;
import com.datastax.dse.protocol.internal.request.Revise;
import com.datastax.dse.protocol.internal.response.result.DseRowsMetadata;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.AsyncPagingIterable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NodeUnavailableException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.metrics.NodeMetric;
import com.datastax.oss.driver.api.core.metrics.SessionMetric;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import com.datastax.oss.driver.internal.core.cql.DefaultExecutionInfo;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.collection.SimpleQueryPlan;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.oss.protocol.internal.util.Bytes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.handler.codec.EncoderException;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a request that supports multiple response messages (a.k.a. continuous paging request).
 */
@ThreadSafe
public abstract class ContinuousRequestHandlerBase<StatementT extends Request, ResultSetT>
    implements Throttled {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousRequestHandlerBase.class);

  protected final String logPrefix;
  protected final StatementT initialStatement;
  protected final DefaultSession session;
  private final CqlIdentifier keyspace;
  protected final InternalDriverContext context;
  private final Queue<Node> queryPlan;
  protected final RequestThrottler throttler;
  private final boolean protocolBackpressureAvailable;
  private final Timer timer;
  private final SessionMetricUpdater sessionMetricUpdater;
  private final boolean specExecEnabled;
  private final SessionMetric clientTimeoutsMetric;
  private final SessionMetric continuousRequestsMetric;
  private final NodeMetric messagesMetric;
  private final List<Timeout> scheduledExecutions;

  // The errors on the nodes that were already tried.
  // We don't use a map because nodes can appear multiple times.
  protected final List<Map.Entry<Node, Throwable>> errors = new CopyOnWriteArrayList<>();

  /**
   * The list of in-flight executions, one per node. Executions may be triggered by speculative
   * executions or retries. An execution is added to this list when the write operation completes.
   * It is removed from this list when the callback has done reading responses.
   */
  private final List<NodeResponseCallback> inFlightCallbacks = new CopyOnWriteArrayList<>();

  /** The callback selected to stream results back to the client. */
  private final CompletableFuture<NodeResponseCallback> chosenCallback = new CompletableFuture<>();

  /**
   * How many speculative executions are currently running (including the initial execution). We
   * track this in order to know when to fail the request if all executions have reached the end of
   * the query plan.
   */
  private final AtomicInteger activeExecutionsCount = new AtomicInteger(0);

  /**
   * How many speculative executions have started (excluding the initial execution), whether they
   * have completed or not. We track this in order to fill execution info objects with this
   * information.
   */
  protected final AtomicInteger startedSpeculativeExecutionsCount = new AtomicInteger(0);

  // Set when the execution starts, and is never modified after.
  private final long startTimeNanos;
  private volatile Timeout globalTimeout;

  private final Class<ResultSetT> resultSetClass;

  public ContinuousRequestHandlerBase(
      @NonNull StatementT statement,
      @NonNull DefaultSession session,
      @NonNull InternalDriverContext context,
      @NonNull String sessionLogPrefix,
      @NonNull Class<ResultSetT> resultSetClass,
      boolean specExecEnabled,
      SessionMetric clientTimeoutsMetric,
      SessionMetric continuousRequestsMetric,
      NodeMetric messagesMetric) {
    this.resultSetClass = resultSetClass;

    ProtocolVersion protocolVersion = context.getProtocolVersion();
    if (!context
        .getProtocolVersionRegistry()
        .supports(protocolVersion, DseProtocolFeature.CONTINUOUS_PAGING)) {
      throw new IllegalStateException(
          "Cannot execute continuous paging requests with protocol version " + protocolVersion);
    }
    this.clientTimeoutsMetric = clientTimeoutsMetric;
    this.continuousRequestsMetric = continuousRequestsMetric;
    this.messagesMetric = messagesMetric;
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    LOG.trace("[{}] Creating new continuous handler for request {}", logPrefix, statement);
    this.initialStatement = statement;
    this.session = session;
    this.keyspace = session.getKeyspace().orElse(null);
    this.context = context;
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    this.queryPlan =
        statement.getNode() != null
            ? new SimpleQueryPlan(statement.getNode())
            : context
                .getLoadBalancingPolicyWrapper()
                .newQueryPlan(statement, executionProfile.getName(), session);
    this.timer = context.getNettyOptions().getTimer();

    this.protocolBackpressureAvailable =
        protocolVersion.getCode() >= DseProtocolVersion.DSE_V2.getCode();
    this.throttler = context.getRequestThrottler();
    this.sessionMetricUpdater = session.getMetricUpdater();
    this.startTimeNanos = System.nanoTime();
    this.specExecEnabled = specExecEnabled;
    this.scheduledExecutions = this.specExecEnabled ? new CopyOnWriteArrayList<>() : null;
  }

  @NonNull
  protected abstract Duration getGlobalTimeout();

  @NonNull
  protected abstract Duration getPageTimeout(@NonNull StatementT statement, int pageNumber);

  @NonNull
  protected abstract Duration getReviseRequestTimeout(@NonNull StatementT statement);

  protected abstract int getMaxEnqueuedPages(@NonNull StatementT statement);

  protected abstract int getMaxPages(@NonNull StatementT statement);

  @NonNull
  protected abstract Message getMessage(@NonNull StatementT statement);

  protected abstract boolean isTracingEnabled(@NonNull StatementT statement);

  @NonNull
  protected abstract Map<String, ByteBuffer> createPayload(@NonNull StatementT statement);

  @NonNull
  protected abstract ResultSetT createEmptyResultSet(@NonNull ExecutionInfo executionInfo);

  protected abstract int pageNumber(@NonNull ResultSetT resultSet);

  @NonNull
  protected abstract ResultSetT createResultSet(
      @NonNull StatementT statement,
      @NonNull Rows rows,
      @NonNull ExecutionInfo executionInfo,
      @NonNull ColumnDefinitions columnDefinitions)
      throws IOException;

  // MAIN LIFECYCLE

  @Override
  public void onThrottleReady(boolean wasDelayed) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(initialStatement, context);
    if (wasDelayed
        // avoid call to nanoTime() if metric is disabled:
        && sessionMetricUpdater.isEnabled(
            DefaultSessionMetric.THROTTLING_DELAY, executionProfile.getName())) {
      session
          .getMetricUpdater()
          .updateTimer(
              DefaultSessionMetric.THROTTLING_DELAY,
              executionProfile.getName(),
              System.nanoTime() - startTimeNanos,
              TimeUnit.NANOSECONDS);
    }
    activeExecutionsCount.incrementAndGet();
    sendRequest(initialStatement, null, 0, 0, specExecEnabled);
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(initialStatement, context);
    session
        .getMetricUpdater()
        .incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, executionProfile.getName());
    abortGlobalRequestOrChosenCallback(error);
  }

  private void abortGlobalRequestOrChosenCallback(@NonNull Throwable error) {
    if (!chosenCallback.completeExceptionally(error)) {
      chosenCallback.thenAccept(callback -> callback.abort(error, false));
    }
  }

  public CompletionStage<ResultSetT> handle() {
    globalTimeout = scheduleGlobalTimeout();
    return fetchNextPage();
  }

  /**
   * Builds the future that will get returned to the user from the initial execute call or a
   * fetchNextPage() on the async API.
   */
  public CompletionStage<ResultSetT> fetchNextPage() {
    CompletableFuture<ResultSetT> result = new CompletableFuture<>();

    // This is equivalent to
    // `chosenCallback.thenCompose(NodeResponseCallback::dequeueOrCreatePending)`, except
    // that we need to cancel `result` if `resultSetError` is a CancellationException.
    chosenCallback.whenComplete(
        (callback, callbackError) -> {
          if (callbackError != null) {
            result.completeExceptionally(callbackError);
          } else {
            callback
                .dequeueOrCreatePending()
                .whenComplete(
                    (resultSet, resultSetError) -> {
                      if (resultSetError != null) {
                        result.completeExceptionally(resultSetError);
                      } else {
                        result.complete(resultSet);
                      }
                    });
          }
        });

    // If the user cancels the future, propagate to our internal components
    result.whenComplete(
        (rs, t) -> {
          if (t instanceof CancellationException) {
            cancel();
          }
        });

    return result;
  }

  /**
   * Sends the initial request to the next available node.
   *
   * @param node if not null, it will be attempted first before the rest of the query plan. It
   *     happens only when we retry on the same host.
   * @param currentExecutionIndex 0 for the initial execution, 1 for the first speculative one, etc.
   * @param retryCount the number of times that the retry policy was invoked for this execution
   *     already (note that some internal retries don't go through the policy, and therefore don't
   *     increment this counter)
   * @param scheduleSpeculativeExecution whether to schedule the next speculative execution
   */
  private void sendRequest(
      StatementT statement,
      @Nullable Node node,
      int currentExecutionIndex,
      int retryCount,
      boolean scheduleSpeculativeExecution) {
    DriverChannel channel = null;
    if (node == null || (channel = session.getChannel(node, logPrefix)) == null) {
      while ((node = queryPlan.poll()) != null) {
        channel = session.getChannel(node, logPrefix);
        if (channel != null) {
          break;
        } else {
          recordError(node, new NodeUnavailableException(node));
        }
      }
    }
    if (channel == null) {
      // We've reached the end of the query plan without finding any node to write to; abort the
      // continuous paging session.
      if (activeExecutionsCount.decrementAndGet() == 0) {
        abortGlobalRequestOrChosenCallback(AllNodesFailedException.fromErrors(errors));
      }
    } else if (!chosenCallback.isDone()) {
      NodeResponseCallback nodeResponseCallback =
          new NodeResponseCallback(
              statement,
              node,
              channel,
              currentExecutionIndex,
              retryCount,
              scheduleSpeculativeExecution,
              logPrefix);
      inFlightCallbacks.add(nodeResponseCallback);
      channel
          .write(
              getMessage(statement),
              isTracingEnabled(statement),
              createPayload(statement),
              nodeResponseCallback)
          .addListener(nodeResponseCallback);
    }
  }

  private Timeout scheduleGlobalTimeout() {
    Duration globalTimeout = getGlobalTimeout();
    if (globalTimeout.toNanos() <= 0) {
      return null;
    }
    LOG.trace("[{}] Scheduling global timeout for pages in {}", logPrefix, globalTimeout);
    return timer.newTimeout(
        timeout ->
            abortGlobalRequestOrChosenCallback(
                new DriverTimeoutException("Query timed out after " + globalTimeout)),
        globalTimeout.toNanos(),
        TimeUnit.NANOSECONDS);
  }

  /**
   * Cancels the continuous paging request.
   *
   * <p>Called from user code, see {@link DefaultContinuousAsyncResultSet#cancel()}, or from a
   * driver I/O thread.
   */
  public void cancel() {
    // If chosenCallback is already set, this is a no-op and the chosen callback will be handled by
    // cancelScheduledTasks
    chosenCallback.cancel(true);

    cancelScheduledTasks(null);
    cancelGlobalTimeout();
  }

  private void cancelGlobalTimeout() {
    if (globalTimeout != null) {
      globalTimeout.cancel();
    }
  }

  /**
   * Cancel all pending and scheduled executions, except the one passed as an argument to the
   * method.
   *
   * @param toIgnore An optional execution to ignore (will not be cancelled).
   */
  private void cancelScheduledTasks(@Nullable NodeResponseCallback toIgnore) {
    if (scheduledExecutions != null) {
      for (Timeout scheduledExecution : scheduledExecutions) {
        scheduledExecution.cancel();
      }
    }
    for (NodeResponseCallback callback : inFlightCallbacks) {
      if (toIgnore == null || toIgnore != callback) {
        callback.cancel();
      }
    }
  }

  @VisibleForTesting
  int getState() {
    try {
      return chosenCallback.get().getState();
    } catch (CancellationException e) {
      // Happens if the test cancels before the callback was chosen
      return NodeResponseCallback.STATE_FAILED;
    } catch (InterruptedException | ExecutionException e) {
      // We never interrupt or fail chosenCallback (other than canceling)
      throw new AssertionError("Unexpected error", e);
    }
  }

  @VisibleForTesting
  CompletableFuture<ResultSetT> getPendingResult() {
    try {
      return chosenCallback.get().getPendingResult();
    } catch (Exception e) {
      // chosenCallback should always be complete by the time tests call this
      throw new AssertionError("Expected callback to be chosen at this point");
    }
  }

  private void recordError(@NonNull Node node, @NonNull Throwable error) {
    errors.add(new AbstractMap.SimpleEntry<>(node, error));
  }

  /**
   * Handles the interaction with a single node in the query plan.
   *
   * <p>An instance of this class is created each time we (re)try a node. The first callback that
   * has something ready to enqueue will be allowed to stream results back to the client; the others
   * will be cancelled.
   */
  private class NodeResponseCallback
      implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>> {

    private final long messageStartTimeNanos = System.nanoTime();
    private final StatementT statement;
    private final Node node;
    private final DriverChannel channel;
    // The identifier of the current execution (0 for the initial execution, 1 for the first
    // speculative execution, etc.)
    private final int executionIndex;
    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt of each execution).
    private final String logPrefix;
    private final boolean scheduleSpeculativeExecution;

    private final DriverExecutionProfile executionProfile;

    // Coordinates concurrent accesses between the client and I/O threads
    private final ReentrantLock lock = new ReentrantLock();

    // The page queue, storing responses that we have received and have not been consumed by the
    // client yet. We instantiate it lazily to avoid unnecessary allocation; this is also used to
    // check if the callback ever tried to enqueue something.
    @GuardedBy("lock")
    private Queue<Object> queue;

    // If the client requests a page and we can't serve it immediately (empty queue), then we create
    // this future and have the client wait on it. Otherwise this field is null.
    @GuardedBy("lock")
    private CompletableFuture<ResultSetT> pendingResult;

    // How many pages were requested. This is the total number of pages requested from the
    // beginning.
    // It will be zero if the protocol does not support numPagesRequested (DSE_V1)
    @GuardedBy("lock")
    private int numPagesRequested;

    // An integer that represents the state of the continuous paging request:
    // - if positive, it is the sequence number of the next expected page;
    // - if negative, it is a terminal state, identified by the constants below.
    @GuardedBy("lock")
    private int state = 1;

    // Whether isLastResponse has returned true already
    @GuardedBy("lock")
    private boolean sawLastResponse;

    @GuardedBy("lock")
    private boolean sentCancelRequest;

    private static final int STATE_FINISHED = -1;
    private static final int STATE_FAILED = -2;

    @GuardedBy("lock")
    private int streamId = -1;

    // These are set when the first page arrives, and are never modified after.
    private volatile ColumnDefinitions columnDefinitions;

    private volatile Timeout pageTimeout;

    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt, 1 for the first retry, etc.).
    private final int retryCount;

    // SpeculativeExecution node metrics should be executed only for the first page (first
    // invocation)
    private final AtomicBoolean stopNodeMessageTimerReported = new AtomicBoolean(false);
    private final AtomicBoolean nodeErrorReported = new AtomicBoolean(false);
    private final AtomicBoolean nodeSuccessReported = new AtomicBoolean(false);

    public NodeResponseCallback(
        StatementT statement,
        Node node,
        DriverChannel channel,
        int executionIndex,
        int retryCount,
        boolean scheduleSpeculativeExecution,
        String logPrefix) {
      this.statement = statement;
      this.node = node;
      this.channel = channel;
      this.executionIndex = executionIndex;
      this.retryCount = retryCount;
      this.scheduleSpeculativeExecution = scheduleSpeculativeExecution;
      this.logPrefix = logPrefix + "|" + executionIndex;
      this.executionProfile = Conversions.resolveExecutionProfile(statement, context);
    }

    @Override
    public void onStreamIdAssigned(int streamId) {
      LOG.trace("[{}] Assigned streamId {} on node {}", logPrefix, streamId, node);
      lock.lock();
      try {
        this.streamId = streamId;
        if (state < 0) {
          // This happens if we were cancelled before getting the stream id, we have a request in
          // flight that needs to be cancelled
          releaseStreamId();
        }
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean isLastResponse(@NonNull Frame responseFrame) {
      lock.lock();
      try {
        Message message = responseFrame.message;
        boolean isLastResponse;

        if (sentCancelRequest) {
          // The only response we accept is the SERVER_ERROR triggered by a successful cancellation.
          // Otherwise we risk releasing and reusing the stream id while the cancel request is still
          // in flight, and it might end up cancelling an unrelated request.
          // Note that there is a chance that the request ends normally right after we send the
          // cancel request. In that case this method never returns true and the stream id will
          // remain orphaned forever. This should be very rare so this is acceptable.
          if (message instanceof Error) {
            Error error = (Error) message;
            isLastResponse =
                (error.code == ProtocolConstants.ErrorCode.SERVER_ERROR)
                    && error.message.contains("Session cancelled by the user");
          } else {
            isLastResponse = false;
          }
        } else if (message instanceof Rows) {
          Rows rows = (Rows) message;
          DseRowsMetadata metadata = (DseRowsMetadata) rows.getMetadata();
          isLastResponse = metadata.isLastContinuousPage;
        } else {
          isLastResponse = message instanceof Error;
        }

        if (isLastResponse) {
          sawLastResponse = true;
        }
        return isLastResponse;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Invoked when the write from {@link #sendRequest} completes.
     *
     * @param future The future representing the outcome of the write operation.
     */
    @Override
    public void operationComplete(@NonNull Future<java.lang.Void> future) {
      if (!future.isSuccess()) {
        Throwable error = future.cause();
        if (error instanceof EncoderException
            && error.getCause() instanceof FrameTooLongException) {
          trackNodeError(node, error.getCause(), null);
          lock.lock();
          try {
            abort(error.getCause(), false);
          } finally {
            lock.unlock();
          }
        } else {
          LOG.trace(
              "[{}] Failed to send request on {}, trying next node (cause: {})",
              logPrefix,
              channel,
              error);
          ((DefaultNode) node)
              .getMetricUpdater()
              .incrementCounter(DefaultNodeMetric.UNSENT_REQUESTS, executionProfile.getName());
          recordError(node, error);
          trackNodeError(node, error.getCause(), null);
          sendRequest(statement, null, executionIndex, retryCount, scheduleSpeculativeExecution);
        }
      } else {
        LOG.trace("[{}] Request sent on {}", logPrefix, channel);
        if (scheduleSpeculativeExecution
            && Conversions.resolveIdempotence(statement, executionProfile)) {
          int nextExecution = executionIndex + 1;
          // Note that `node` is the first node of the execution, it might not be the "slow" one
          // if there were retries, but in practice retries are rare.
          long nextDelay =
              Conversions.resolveSpeculativeExecutionPolicy(context, executionProfile)
                  .nextExecution(node, keyspace, statement, nextExecution);
          if (nextDelay >= 0) {
            scheduleSpeculativeExecution(nextExecution, nextDelay);
          } else {
            LOG.trace(
                "[{}] Speculative execution policy returned {}, no next execution",
                logPrefix,
                nextDelay);
          }
        }
        pageTimeout = schedulePageTimeout(1);
      }
    }

    private void scheduleSpeculativeExecution(int nextExecutionIndex, long delay) {
      LOG.trace(
          "[{}] Scheduling speculative execution {} in {} ms",
          logPrefix,
          nextExecutionIndex,
          delay);
      try {
        scheduledExecutions.add(
            timer.newTimeout(
                (Timeout timeout) -> {
                  if (!chosenCallback.isDone()) {
                    LOG.trace(
                        "[{}] Starting speculative execution {}", logPrefix, nextExecutionIndex);
                    activeExecutionsCount.incrementAndGet();
                    startedSpeculativeExecutionsCount.incrementAndGet();
                    NodeMetricUpdater nodeMetricUpdater = ((DefaultNode) node).getMetricUpdater();
                    if (nodeMetricUpdater.isEnabled(
                        DefaultNodeMetric.SPECULATIVE_EXECUTIONS, executionProfile.getName())) {
                      nodeMetricUpdater.incrementCounter(
                          DefaultNodeMetric.SPECULATIVE_EXECUTIONS, executionProfile.getName());
                    }
                    sendRequest(statement, null, nextExecutionIndex, 0, true);
                  }
                },
                delay,
                TimeUnit.MILLISECONDS));
      } catch (IllegalStateException e) {
        logTimeoutSchedulingError(e);
      }
    }

    private Timeout schedulePageTimeout(int expectedPage) {
      if (expectedPage < 0) {
        return null;
      }
      Duration timeout = getPageTimeout(statement, expectedPage);
      if (timeout.toNanos() <= 0) {
        return null;
      }
      LOG.trace("[{}] Scheduling timeout for page {} in {}", logPrefix, expectedPage, timeout);
      return timer.newTimeout(
          t -> onPageTimeout(expectedPage), timeout.toNanos(), TimeUnit.NANOSECONDS);
    }

    private void onPageTimeout(int expectedPage) {
      lock.lock();
      try {
        if (state == expectedPage) {
          abort(
              new DriverTimeoutException(
                  String.format("Timed out waiting for page %d", expectedPage)),
              false);
        } else {
          // Ignore timeout if the request has moved on in the interim.
          LOG.trace(
              "[{}] Timeout fired for page {} but query already at state {}, skipping",
              logPrefix,
              expectedPage,
              state);
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Invoked when a continuous paging response is received, either a successful or failed one.
     *
     * <p>Delegates further processing to appropriate methods: {@link #processResultResponse(Result,
     * Frame)} if the response was successful, or {@link #processErrorResponse(Error, Frame)} if it
     * wasn't.
     *
     * @param response the received {@link Frame}.
     */
    @Override
    public void onResponse(@NonNull Frame response) {
      stopNodeMessageTimer();
      cancelTimeout(pageTimeout);
      lock.lock();
      try {
        if (state < 0) {
          LOG.trace("[{}] Got result but the request has been cancelled, ignoring", logPrefix);
          return;
        }
        try {
          Message responseMessage = response.message;
          if (responseMessage instanceof Result) {
            LOG.trace("[{}] Got result", logPrefix);
            processResultResponse((Result) responseMessage, response);
          } else if (responseMessage instanceof Error) {
            LOG.trace("[{}] Got error response", logPrefix);
            processErrorResponse((Error) responseMessage, response);
          } else {
            IllegalStateException error =
                new IllegalStateException("Unexpected response " + responseMessage);
            trackNodeError(node, error, response);
            abort(error, false);
          }
        } catch (Throwable t) {
          trackNodeError(node, t, response);
          abort(t, false);
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Invoked when a continuous paging request hits an unexpected error.
     *
     * <p>Delegates further processing to to the retry policy ({@link
     * #processRetryVerdict(RetryVerdict, Throwable)}.
     *
     * @param error the error encountered, usually a network problem.
     */
    @Override
    public void onFailure(@NonNull Throwable error) {
      cancelTimeout(pageTimeout);
      LOG.trace(String.format("[%s] Request failure", logPrefix), error);
      RetryVerdict verdict;
      if (!Conversions.resolveIdempotence(statement, executionProfile)
          || error instanceof FrameTooLongException) {
        verdict = RetryVerdict.RETHROW;
      } else {
        try {
          RetryPolicy retryPolicy = Conversions.resolveRetryPolicy(context, executionProfile);
          verdict = retryPolicy.onRequestAbortedVerdict(statement, error, retryCount);
        } catch (Throwable cause) {
          abort(
              new IllegalStateException("Unexpected error while invoking the retry policy", cause),
              false);
          return;
        }
      }
      updateErrorMetrics(
          ((DefaultNode) node).getMetricUpdater(),
          verdict,
          DefaultNodeMetric.ABORTED_REQUESTS,
          DefaultNodeMetric.RETRIES_ON_ABORTED,
          DefaultNodeMetric.IGNORES_ON_ABORTED);
      lock.lock();
      try {
        processRetryVerdict(verdict, error);
      } finally {
        lock.unlock();
      }
    }

    // PROCESSING METHODS

    /**
     * Processes a new result response, creating the corresponding {@link ResultSetT} object and
     * then enqueuing it or serving it directly to the user if he was waiting for it.
     *
     * @param result the result to process. It is normally a {@link Rows} object, but may be a
     *     {@link Void} object if the retry policy decided to ignore an error.
     * @param frame the {@link Frame} (used to create the {@link ExecutionInfo} the first time).
     */
    @SuppressWarnings("GuardedBy") // this method is only called with the lock held
    private void processResultResponse(@NonNull Result result, @Nullable Frame frame) {
      assert lock.isHeldByCurrentThread();
      try {
        ExecutionInfo executionInfo = createExecutionInfo(result, frame);
        if (result instanceof Rows) {
          DseRowsMetadata rowsMetadata = (DseRowsMetadata) ((Rows) result).getMetadata();
          if (columnDefinitions == null) {
            // Contrary to ROWS responses from regular queries,
            // the first page always includes metadata so we use this
            // regardless of whether or not the query was from a prepared statement.
            columnDefinitions = Conversions.toColumnDefinitions(rowsMetadata, context);
          }
          int pageNumber = rowsMetadata.continuousPageNumber;
          int currentPage = state;
          if (pageNumber != currentPage) {
            abort(
                new IllegalStateException(
                    String.format(
                        "Received page %d but was expecting %d", pageNumber, currentPage)),
                false);
          } else {
            int pageSize = ((Rows) result).getData().size();
            ResultSetT resultSet =
                createResultSet(statement, (Rows) result, executionInfo, columnDefinitions);
            if (rowsMetadata.isLastContinuousPage) {
              LOG.trace("[{}] Received last page ({} - {} rows)", logPrefix, pageNumber, pageSize);
              state = STATE_FINISHED;
              reenableAutoReadIfNeeded();
              enqueueOrCompletePending(resultSet);
              stopGlobalRequestTimer();
              cancelTimeout(globalTimeout);
            } else {
              LOG.trace("[{}] Received page {} ({} rows)", logPrefix, pageNumber, pageSize);
              if (currentPage > 0) {
                state = currentPage + 1;
              }
              enqueueOrCompletePending(resultSet);
            }
          }
        } else {
          // Void responses happen only when the retry decision is ignore.
          assert result instanceof Void;
          ResultSetT resultSet = createEmptyResultSet(executionInfo);
          LOG.trace(
              "[{}] Continuous paging interrupted by retry policy decision to ignore error",
              logPrefix);
          state = STATE_FINISHED;
          reenableAutoReadIfNeeded();
          enqueueOrCompletePending(resultSet);
          stopGlobalRequestTimer();
          cancelTimeout(globalTimeout);
        }
      } catch (Throwable error) {
        abort(error, false);
      }
    }

    /**
     * Processes an unsuccessful response.
     *
     * <p>Depending on the error, may trigger:
     *
     * <ol>
     *   <li>a re-prepare cycle, see {@link #processUnprepared(Unprepared)};
     *   <li>an immediate retry on the next host, bypassing the retry policy, if the host was
     *       bootstrapping;
     *   <li>an immediate abortion if the error is unrecoverable;
     *   <li>further processing if the error is recoverable, see {@link
     *       #processRecoverableError(CoordinatorException)}
     * </ol>
     *
     * @param errorMessage the error message received.
     */
    @SuppressWarnings("GuardedBy") // this method is only called with the lock held
    private void processErrorResponse(@NonNull Error errorMessage, @NonNull Frame frame) {
      assert lock.isHeldByCurrentThread();
      if (errorMessage instanceof Unprepared) {
        processUnprepared((Unprepared) errorMessage);
      } else {
        CoordinatorException error = DseConversions.toThrowable(node, errorMessage, context);
        if (error instanceof BootstrappingException) {
          LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
          recordError(node, error);
          trackNodeError(node, error, frame);
          sendRequest(statement, null, executionIndex, retryCount, false);
        } else if (error instanceof QueryValidationException
            || error instanceof FunctionFailureException
            || error instanceof ProtocolError
            || state > 1) {
          // we only process recoverable errors for the first page,
          // errors on subsequent pages will always trigger an immediate abortion
          LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
          NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
          metricUpdater.incrementCounter(
              DefaultNodeMetric.OTHER_ERRORS, executionProfile.getName());
          trackNodeError(node, error, frame);
          abort(error, true);
        } else {
          try {
            processRecoverableError(error);
          } catch (Throwable cause) {
            abort(cause, false);
          }
        }
      }
    }

    /**
     * Processes a recoverable error.
     *
     * <p>In most cases, delegates to the retry policy and its decision, see {@link
     * #processRetryVerdict(RetryVerdict, Throwable)}.
     *
     * @param error the recoverable error.
     */
    private void processRecoverableError(@NonNull CoordinatorException error) {
      assert lock.isHeldByCurrentThread();
      NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
      RetryVerdict verdict;
      RetryPolicy retryPolicy = Conversions.resolveRetryPolicy(context, executionProfile);
      if (error instanceof ReadTimeoutException) {
        ReadTimeoutException readTimeout = (ReadTimeoutException) error;
        verdict =
            retryPolicy.onReadTimeoutVerdict(
                statement,
                readTimeout.getConsistencyLevel(),
                readTimeout.getBlockFor(),
                readTimeout.getReceived(),
                readTimeout.wasDataPresent(),
                retryCount);
        updateErrorMetrics(
            metricUpdater,
            verdict,
            DefaultNodeMetric.READ_TIMEOUTS,
            DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT,
            DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT);
      } else if (error instanceof WriteTimeoutException) {
        WriteTimeoutException writeTimeout = (WriteTimeoutException) error;
        if (Conversions.resolveIdempotence(statement, executionProfile)) {
          verdict =
              retryPolicy.onWriteTimeoutVerdict(
                  statement,
                  writeTimeout.getConsistencyLevel(),
                  writeTimeout.getWriteType(),
                  writeTimeout.getBlockFor(),
                  writeTimeout.getReceived(),
                  retryCount);
        } else {
          verdict = RetryVerdict.RETHROW;
        }
        updateErrorMetrics(
            metricUpdater,
            verdict,
            DefaultNodeMetric.WRITE_TIMEOUTS,
            DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT,
            DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT);
      } else if (error instanceof UnavailableException) {
        UnavailableException unavailable = (UnavailableException) error;
        verdict =
            retryPolicy.onUnavailableVerdict(
                statement,
                unavailable.getConsistencyLevel(),
                unavailable.getRequired(),
                unavailable.getAlive(),
                retryCount);
        updateErrorMetrics(
            metricUpdater,
            verdict,
            DefaultNodeMetric.UNAVAILABLES,
            DefaultNodeMetric.RETRIES_ON_UNAVAILABLE,
            DefaultNodeMetric.IGNORES_ON_UNAVAILABLE);
      } else {
        verdict =
            Conversions.resolveIdempotence(statement, executionProfile)
                ? retryPolicy.onErrorResponseVerdict(statement, error, retryCount)
                : RetryVerdict.RETHROW;
        updateErrorMetrics(
            metricUpdater,
            verdict,
            DefaultNodeMetric.OTHER_ERRORS,
            DefaultNodeMetric.RETRIES_ON_OTHER_ERROR,
            DefaultNodeMetric.IGNORES_ON_OTHER_ERROR);
      }
      processRetryVerdict(verdict, error);
    }

    /**
     * Processes an {@link Unprepared} error by re-preparing then retrying on the same host.
     *
     * @param errorMessage the unprepared error message.
     */
    @SuppressWarnings("GuardedBy") // this method is only called with the lock held
    private void processUnprepared(@NonNull Unprepared errorMessage) {
      assert lock.isHeldByCurrentThread();
      ByteBuffer idToReprepare = ByteBuffer.wrap(errorMessage.id);
      LOG.trace(
          "[{}] Statement {} is not prepared on {}, re-preparing",
          logPrefix,
          Bytes.toHexString(idToReprepare),
          node);
      RepreparePayload repreparePayload = session.getRepreparePayloads().get(idToReprepare);
      if (repreparePayload == null) {
        throw new IllegalStateException(
            String.format(
                "Tried to execute unprepared query %s but we don't have the data to re-prepare it",
                Bytes.toHexString(idToReprepare)));
      }
      Prepare prepare = repreparePayload.toMessage();
      Duration timeout = executionProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT);
      ThrottledAdminRequestHandler.prepare(
              channel,
              true,
              prepare,
              repreparePayload.customPayload,
              timeout,
              throttler,
              sessionMetricUpdater,
              logPrefix)
          .start()
          .whenComplete(
              (repreparedId, exception) -> {
                // If we run into an unrecoverable error, surface it to the client instead of
                // retrying
                Throwable fatalError = null;
                if (exception == null) {
                  if (!repreparedId.equals(idToReprepare)) {
                    IllegalStateException illegalStateException =
                        new IllegalStateException(
                            String.format(
                                "ID mismatch while trying to reprepare (expected %s, got %s). "
                                    + "This prepared statement won't work anymore. "
                                    + "This usually happens when you run a 'USE...' query after "
                                    + "the statement was prepared.",
                                Bytes.toHexString(idToReprepare), Bytes.toHexString(repreparedId)));
                    trackNodeError(node, illegalStateException, null);
                    fatalError = illegalStateException;
                  } else {
                    LOG.trace(
                        "[{}] Re-prepare successful, retrying on the same node ({})",
                        logPrefix,
                        node);
                    sendRequest(statement, node, executionIndex, retryCount, false);
                  }
                } else {
                  if (exception instanceof UnexpectedResponseException) {
                    Message prepareErrorMessage = ((UnexpectedResponseException) exception).message;
                    if (prepareErrorMessage instanceof Error) {
                      CoordinatorException prepareError =
                          DseConversions.toThrowable(node, (Error) prepareErrorMessage, context);
                      if (prepareError instanceof QueryValidationException
                          || prepareError instanceof FunctionFailureException
                          || prepareError instanceof ProtocolError) {
                        LOG.trace("[{}] Unrecoverable error on re-prepare, rethrowing", logPrefix);
                        trackNodeError(node, prepareError, null);
                        fatalError = prepareError;
                      }
                    }
                  } else if (exception instanceof RequestThrottlingException) {
                    trackNodeError(node, exception, null);
                    fatalError = exception;
                  }
                  if (fatalError == null) {
                    LOG.trace("[{}] Re-prepare failed, trying next node", logPrefix);
                    recordError(node, exception);
                    trackNodeError(node, exception, null);
                    sendRequest(statement, null, executionIndex, retryCount, false);
                  }
                }
                if (fatalError != null) {
                  lock.lock();
                  try {
                    abort(fatalError, true);
                  } finally {
                    lock.unlock();
                  }
                }
              });
    }

    /**
     * Processes the retry decision by triggering a retry, aborting or ignoring; also records the
     * failures for further access.
     *
     * @param verdict the verdict to process.
     * @param error the original error.
     */
    private void processRetryVerdict(@NonNull RetryVerdict verdict, @NonNull Throwable error) {
      assert lock.isHeldByCurrentThread();
      LOG.trace("[{}] Processing retry decision {}", logPrefix, verdict);
      switch (verdict.getRetryDecision()) {
        case RETRY_SAME:
          recordError(node, error);
          trackNodeError(node, error, null);
          sendRequest(
              verdict.getRetryRequest(statement), node, executionIndex, retryCount + 1, false);
          break;
        case RETRY_NEXT:
          recordError(node, error);
          trackNodeError(node, error, null);
          sendRequest(
              verdict.getRetryRequest(statement), null, executionIndex, retryCount + 1, false);
          break;
        case RETHROW:
          trackNodeError(node, error, null);
          abort(error, true);
          break;
        case IGNORE:
          processResultResponse(Void.INSTANCE, null);
          break;
      }
    }

    // PAGE HANDLING

    /**
     * Enqueues a response or, if the client was already waiting for it, completes the pending
     * future.
     *
     * <p>Guarded by {@link #lock}.
     *
     * @param pageOrError the next page, or an error.
     */
    @SuppressWarnings("GuardedBy") // this method is only called with the lock held
    private void enqueueOrCompletePending(@NonNull Object pageOrError) {
      assert lock.isHeldByCurrentThread();

      if (queue == null) {
        // This is the first time this callback tries to stream something back to the client, check
        // if it can be selected
        if (!chosenCallback.complete(this)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace(
                "[{}] Trying to enqueue {} but another callback was already chosen, aborting",
                logPrefix,
                asTraceString(pageOrError));
          }
          // Discard the data, this callback will be canceled shortly since the chosen callback
          // invoked cancelScheduledTasks
          return;
        }

        queue = new ArrayDeque<>(getMaxEnqueuedPages(statement));
        numPagesRequested = protocolBackpressureAvailable ? getMaxEnqueuedPages(statement) : 0;
        cancelScheduledTasks(this);
      }

      if (pendingResult != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "[{}] Client was waiting on empty queue, completing with {}",
              logPrefix,
              asTraceString(pageOrError));
        }
        CompletableFuture<ResultSetT> tmp = pendingResult;
        // null out pendingResult before completing it because its completion
        // may trigger a call to fetchNextPage -> dequeueOrCreatePending,
        // which expects pendingResult to be null.
        pendingResult = null;
        completeResultSetFuture(tmp, pageOrError);
      } else {
        if (LOG.isTraceEnabled()) {
          LOG.trace("[{}] Enqueuing {}", logPrefix, asTraceString(pageOrError));
        }
        queue.add(pageOrError);
        // Backpressure without protocol support: if the queue grows too large,
        // disable auto-read so that the channel eventually becomes
        // non-writable on the server side (causing it to back off for a while)
        if (!protocolBackpressureAvailable
            && queue.size() == getMaxEnqueuedPages(statement)
            && state > 0) {
          LOG.trace(
              "[{}] Exceeded {} queued response pages, disabling auto-read",
              logPrefix,
              queue.size());
          channel.config().setAutoRead(false);
        }
      }
    }

    /**
     * Dequeue a response or, if the queue is empty, create the future that will get notified of the
     * next response, when it arrives.
     *
     * <p>Called from user code, see {@link ContinuousAsyncResultSet#fetchNextPage()}.
     *
     * @return the next page's future; never null.
     */
    @NonNull
    public CompletableFuture<ResultSetT> dequeueOrCreatePending() {
      lock.lock();
      try {
        // If the client was already waiting for a page, there's no way it can call this method
        // again
        // (this is guaranteed by our public API because in order to ask for the next page,
        // you need the reference to the previous page).
        assert pendingResult == null;

        Object head = null;
        if (queue != null) {
          head = queue.poll();
          if (!protocolBackpressureAvailable
              && head != null
              && queue.size() == getMaxEnqueuedPages(statement) - 1) {
            LOG.trace(
                "[{}] Back to {} queued response pages, re-enabling auto-read",
                logPrefix,
                queue.size());
            channel.config().setAutoRead(true);
          }
          maybeRequestMore();
        }

        if (head != null) {
          if (state == STATE_FAILED && !(head instanceof Throwable)) {
            LOG.trace(
                "[{}] Client requested next page on cancelled queue, discarding page and returning cancelled future",
                logPrefix);
            return cancelledResultSetFuture();
          } else {
            if (LOG.isTraceEnabled()) {
              LOG.trace(
                  "[{}] Client requested next page on non-empty queue, returning immediate future of {}",
                  logPrefix,
                  asTraceString(head));
            }
            return immediateResultSetFuture(head);
          }
        } else {
          if (state == STATE_FAILED) {
            LOG.trace(
                "[{}] Client requested next page on cancelled empty queue, returning cancelled future",
                logPrefix);
            return cancelledResultSetFuture();
          } else {
            LOG.trace(
                "[{}] Client requested next page but queue is empty, installing future", logPrefix);
            pendingResult = new CompletableFuture<>();
            // Only schedule a timeout if we're past the first page (the first page's timeout is
            // handled in sendRequest).
            if (state > 1) {
              pageTimeout = schedulePageTimeout(state);
              // Note: each new timeout is cancelled when the next response arrives, see
              // onResponse(Frame).
            }
            return pendingResult;
          }
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * If the total number of results in the queue and in-flight (requested - received) is less than
     * half the queue size, then request more pages, unless the {@link #state} is failed, we're
     * still waiting for the first page (so maybe still throttled or in the middle of a retry), or
     * we don't support backpressure at the protocol level.
     */
    @SuppressWarnings("GuardedBy")
    private void maybeRequestMore() {
      assert lock.isHeldByCurrentThread();
      if (state < 2 || streamId == -1 || !protocolBackpressureAvailable) {
        return;
      }
      // if we have already requested more than the client needs, then no need to request some more
      int maxPages = getMaxPages(statement);
      if (maxPages > 0 && numPagesRequested >= maxPages) {
        return;
      }
      // the pages received so far, which is the state minus one
      int received = state - 1;
      int requested = numPagesRequested;
      // the pages that fit in the queue, which is the queue free space minus the requests in flight
      int freeSpace = getMaxEnqueuedPages(statement) - queue.size();
      int inFlight = requested - received;
      int numPagesFittingInQueue = freeSpace - inFlight;
      if (numPagesFittingInQueue > 0
          && numPagesFittingInQueue >= getMaxEnqueuedPages(statement) / 2) {
        LOG.trace("[{}] Requesting more {} pages", logPrefix, numPagesFittingInQueue);
        numPagesRequested = requested + numPagesFittingInQueue;
        sendMorePagesRequest(numPagesFittingInQueue);
      }
    }

    /**
     * Sends a request for more pages (a.k.a. backpressure request).
     *
     * @param nextPages the number of extra pages to request.
     */
    @SuppressWarnings("GuardedBy")
    private void sendMorePagesRequest(int nextPages) {
      assert lock.isHeldByCurrentThread();
      assert channel != null : "expected valid connection in order to request more pages";
      assert protocolBackpressureAvailable;
      assert streamId != -1;

      LOG.trace("[{}] Sending request for more pages", logPrefix);
      ThrottledAdminRequestHandler.query(
              channel,
              true,
              Revise.requestMoreContinuousPages(streamId, nextPages),
              statement.getCustomPayload(),
              getReviseRequestTimeout(statement),
              throttler,
              session.getMetricUpdater(),
              logPrefix,
              "request " + nextPages + " more pages for id " + streamId)
          .start()
          .handle(
              (result, error) -> {
                if (error != null) {
                  Loggers.warnWithException(
                      LOG, "[{}] Error requesting more pages, aborting.", logPrefix, error);
                  lock.lock();
                  try {
                    // Set fromServer to false because we want the callback to still cancel the
                    // session if possible or else the server will wait on a timeout.
                    abort(error, false);
                  } finally {
                    lock.unlock();
                  }
                }
                return null;
              });
    }

    /** Cancels the given timeout, if non null. */
    private void cancelTimeout(Timeout timeout) {
      if (timeout != null) {
        LOG.trace("[{}] Cancelling timeout", logPrefix);
        timeout.cancel();
      }
    }

    // CANCELLATION

    public void cancel() {
      lock.lock();
      try {
        if (state < 0) {
          return;
        } else {
          LOG.trace(
              "[{}] Cancelling continuous paging session with state {} on node {}",
              logPrefix,
              state,
              node);
          state = STATE_FAILED;
          if (pendingResult != null) {
            pendingResult.cancel(true);
          }
          releaseStreamId();
        }
      } finally {
        lock.unlock();
      }
      reenableAutoReadIfNeeded();
    }

    @SuppressWarnings("GuardedBy")
    private void releaseStreamId() {
      assert lock.isHeldByCurrentThread();
      // If we saw the last response already, InFlightHandler will release the id so no need to
      // cancel explicitly
      if (streamId >= 0 && !sawLastResponse && !channel.closeFuture().isDone()) {
        // This orphans the stream id, but it will still be held until we see the last response:
        channel.cancel(this);
        // This tells the server to stop streaming, and send a terminal response:
        sendCancelRequest();
      }
    }

    @SuppressWarnings("GuardedBy")
    private void sendCancelRequest() {
      assert lock.isHeldByCurrentThread();
      LOG.trace("[{}] Sending cancel request", logPrefix);
      ThrottledAdminRequestHandler.query(
              channel,
              true,
              Revise.cancelContinuousPaging(streamId),
              statement.getCustomPayload(),
              getReviseRequestTimeout(statement),
              throttler,
              session.getMetricUpdater(),
              logPrefix,
              "cancel request")
          .start()
          .handle(
              (result, error) -> {
                if (error != null) {
                  Loggers.warnWithException(
                      LOG,
                      "[{}] Error sending cancel request. "
                          + "This is not critical (the request will eventually time out server-side).",
                      logPrefix,
                      error);
                } else {
                  LOG.trace("[{}] Continuous paging session cancelled successfully", logPrefix);
                }
                return null;
              });
      sentCancelRequest = true;
    }

    // TERMINATION

    private void reenableAutoReadIfNeeded() {
      // Make sure we don't leave the channel unreadable
      LOG.trace("[{}] Re-enabling auto-read", logPrefix);
      if (!protocolBackpressureAvailable) {
        channel.config().setAutoRead(true);
      }
    }

    // ERROR HANDLING

    private void trackNodeError(
        @NonNull Node node, @NonNull Throwable error, @Nullable Frame frame) {
      if (nodeErrorReported.compareAndSet(false, true)) {
        long latencyNanos = System.nanoTime() - this.messageStartTimeNanos;
        context
            .getRequestTracker()
            .onNodeError(
                this.statement,
                error,
                latencyNanos,
                executionProfile,
                node,
                createExecutionInfo(frame),
                logPrefix);
      }
    }

    /**
     * Aborts the continuous paging session due to an error that can be either from the server or
     * the client.
     *
     * @param error the error that causes the abortion.
     * @param fromServer whether the error was triggered by the coordinator or by the driver.
     */
    @SuppressWarnings("GuardedBy") // this method is only called with the lock held
    private void abort(@NonNull Throwable error, boolean fromServer) {
      assert lock.isHeldByCurrentThread();
      LOG.trace(
          "[{}] Aborting due to {} ({})",
          logPrefix,
          error.getClass().getSimpleName(),
          error.getMessage());
      if (channel == null) {
        // This only happens when sending the initial request, if no host was available
        // or if the iterator returned by the LBP threw an exception.
        // In either case the write was not even attempted, and
        // we set the state right now.
        enqueueOrCompletePending(error);
        state = STATE_FAILED;
      } else if (state > 0) {
        enqueueOrCompletePending(error);
        if (fromServer) {
          // We can safely assume the server won't send any more responses,
          // so set the state and call release() right now.
          state = STATE_FAILED;
          reenableAutoReadIfNeeded();
        } else {
          // attempt to cancel first, i.e. ask server to stop sending responses,
          // and only then release.
          cancel();
        }
      }
      stopGlobalRequestTimer();
      cancelTimeout(globalTimeout);
    }

    // METRICS

    private void stopNodeMessageTimer() {
      if (stopNodeMessageTimerReported.compareAndSet(false, true)) {
        ((DefaultNode) node)
            .getMetricUpdater()
            .updateTimer(
                messagesMetric,
                executionProfile.getName(),
                System.nanoTime() - messageStartTimeNanos,
                TimeUnit.NANOSECONDS);
      }
    }

    private void stopGlobalRequestTimer() {
      session
          .getMetricUpdater()
          .updateTimer(
              continuousRequestsMetric,
              null,
              System.nanoTime() - startTimeNanos,
              TimeUnit.NANOSECONDS);
    }

    private void updateErrorMetrics(
        @NonNull NodeMetricUpdater metricUpdater,
        @NonNull RetryVerdict verdict,
        @NonNull DefaultNodeMetric error,
        @NonNull DefaultNodeMetric retriesOnError,
        @NonNull DefaultNodeMetric ignoresOnError) {
      metricUpdater.incrementCounter(error, executionProfile.getName());
      switch (verdict.getRetryDecision()) {
        case RETRY_SAME:
        case RETRY_NEXT:
          metricUpdater.incrementCounter(DefaultNodeMetric.RETRIES, executionProfile.getName());
          metricUpdater.incrementCounter(retriesOnError, executionProfile.getName());
          break;
        case IGNORE:
          metricUpdater.incrementCounter(DefaultNodeMetric.IGNORES, executionProfile.getName());
          metricUpdater.incrementCounter(ignoresOnError, executionProfile.getName());
          break;
        case RETHROW:
          // nothing do do
      }
    }

    // UTILITY METHODS

    @NonNull
    private CompletableFuture<ResultSetT> immediateResultSetFuture(@NonNull Object pageOrError) {
      CompletableFuture<ResultSetT> future = new CompletableFuture<>();
      completeResultSetFuture(future, pageOrError);
      return future;
    }

    @NonNull
    private CompletableFuture<ResultSetT> cancelledResultSetFuture() {
      return immediateResultSetFuture(
          new CancellationException(
              "Can't get more results because the continuous query has failed already. "
                  + "Most likely this is because the query was cancelled"));
    }

    private void completeResultSetFuture(
        @NonNull CompletableFuture<ResultSetT> future, @NonNull Object pageOrError) {
      long now = System.nanoTime();
      long totalLatencyNanos = now - startTimeNanos;
      long nodeLatencyNanos = now - messageStartTimeNanos;
      if (resultSetClass.isInstance(pageOrError)) {
        if (future.complete(resultSetClass.cast(pageOrError))) {
          throttler.signalSuccess(ContinuousRequestHandlerBase.this);

          ExecutionInfo executionInfo = null;
          if (pageOrError instanceof AsyncPagingIterable) {
            executionInfo = ((AsyncPagingIterable) pageOrError).getExecutionInfo();
          } else if (pageOrError instanceof AsyncGraphResultSet) {
            executionInfo = ((AsyncGraphResultSet) pageOrError).getRequestExecutionInfo();
          }

          if (nodeSuccessReported.compareAndSet(false, true)) {
            context
                .getRequestTracker()
                .onNodeSuccess(
                    statement, nodeLatencyNanos, executionProfile, node, executionInfo, logPrefix);
          }
          context
              .getRequestTracker()
              .onSuccess(
                  statement, totalLatencyNanos, executionProfile, node, executionInfo, logPrefix);
        }
      } else {
        Throwable error = (Throwable) pageOrError;
        if (future.completeExceptionally(error)) {
          context
              .getRequestTracker()
              .onError(
                  statement, error, totalLatencyNanos, executionProfile, node, null, logPrefix);
          if (error instanceof DriverTimeoutException) {
            throttler.signalTimeout(ContinuousRequestHandlerBase.this);
            session
                .getMetricUpdater()
                .incrementCounter(clientTimeoutsMetric, executionProfile.getName());
          } else if (!(error instanceof RequestThrottlingException)) {
            throttler.signalError(ContinuousRequestHandlerBase.this, error);
          }
        }
      }
    }

    @NonNull
    private ExecutionInfo createExecutionInfo(@NonNull Result result, @Nullable Frame response) {
      ByteBuffer pagingState =
          result instanceof Rows ? ((Rows) result).getMetadata().pagingState : null;
      return new DefaultExecutionInfo(
          statement,
          node,
          startedSpeculativeExecutionsCount.get(),
          executionIndex,
          errors,
          pagingState,
          response,
          true,
          session,
          context,
          executionProfile);
    }

    @NonNull
    private ExecutionInfo createExecutionInfo(@Nullable Frame response) {
      return new DefaultExecutionInfo(
          statement,
          node,
          startedSpeculativeExecutionsCount.get(),
          executionIndex,
          errors,
          null,
          response,
          true,
          session,
          context,
          executionProfile);
    }

    private void logTimeoutSchedulingError(IllegalStateException timeoutError) {
      // If we're racing with session shutdown, the timer might be stopped already. We don't want
      // to schedule more executions anyway, so swallow the error.
      if (!"cannot be started once stopped".equals(timeoutError.getMessage())) {
        Loggers.warnWithException(
            LOG, "[{}] Error while scheduling timeout", logPrefix, timeoutError);
      }
    }

    @NonNull
    private String asTraceString(@NonNull Object pageOrError) {
      return resultSetClass.isInstance(pageOrError)
          ? "page " + pageNumber(resultSetClass.cast(pageOrError))
          : ((Exception) pageOrError).getClass().getSimpleName();
    }

    private int getState() {
      lock.lock();
      try {
        return state;
      } finally {
        lock.unlock();
      }
    }

    private CompletableFuture<ResultSetT> getPendingResult() {
      lock.lock();
      try {
        return pendingResult;
      } finally {
        lock.unlock();
      }
    }
  }
}
