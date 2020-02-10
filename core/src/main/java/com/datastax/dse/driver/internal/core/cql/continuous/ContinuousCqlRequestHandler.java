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
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.dse.driver.internal.core.DseProtocolFeature;
import com.datastax.dse.driver.internal.core.cql.DseConversions;
import com.datastax.dse.protocol.internal.request.Revise;
import com.datastax.dse.protocol.internal.response.result.DseRowsMetadata;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.ReadTimeoutException;
import com.datastax.oss.driver.api.core.servererrors.UnavailableException;
import com.datastax.oss.driver.api.core.servererrors.WriteTimeoutException;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import com.datastax.oss.driver.internal.core.cql.DefaultExecutionInfo;
import com.datastax.oss.driver.internal.core.cql.DefaultRow;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.util.CountingIterator;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.collection.QueryPlan;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a request that supports multiple response messages (a.k.a. continuous paging request).
 */
@ThreadSafe
public class ContinuousCqlRequestHandler
    implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>>, Throttled {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousCqlRequestHandler.class);

  private final String logPrefix;
  private final Statement<?> statement;
  private final DefaultSession session;
  private final InternalDriverContext context;
  private final DriverExecutionProfile executionProfile;
  private final Queue<Node> queryPlan;
  private final RetryPolicy retryPolicy;
  private final RequestThrottler throttler;
  private final int maxEnqueuedPages;
  private final int maxPages;
  private final boolean protocolBackpressureAvailable;
  private final boolean isIdempotent;
  private final Message message;
  private final Duration timeoutFirstPage;
  private final Duration timeoutOtherPages;
  private final Timer timer;
  private final SessionMetricUpdater sessionMetricUpdater;

  // The errors on the nodes that were already tried.
  // We don't use a map because nodes can appear multiple times.
  private final List<Map.Entry<Node, Throwable>> errors = new CopyOnWriteArrayList<>();

  // Coordinates concurrent accesses between the client and I/O threads
  private final ReentrantLock lock = new ReentrantLock();

  // The page queue, storing responses that we have received and have not been consumed by the
  // client yet.
  @GuardedBy("lock")
  private final Queue<Object> queue;

  // If the client requests a page and we can't serve it immediately (empty queue), then we create
  // this future and have the client wait on it. Otherwise this field is null.
  @GuardedBy("lock")
  @VisibleForTesting
  CompletableFuture<ContinuousAsyncResultSet> pendingResult;

  // How many pages were requested. This is the total number of pages requested from the beginning.
  // It will be zero if the protocol does not support numPagesRequested (DSE_V1)
  @GuardedBy("lock")
  private int numPagesRequested;

  // An integer that represents the state of the continuous paging request:
  // - if positive, it is the sequence number of the next expected page;
  // - if negative, it is a terminal state, identified by the constants below.
  @GuardedBy("lock")
  @VisibleForTesting
  int state = 1;

  private static final int STATE_FINISHED = -1;
  private static final int STATE_FAILED = -2;

  // Set when the execution starts, and is never modified after.
  private volatile long startTimeNanos;

  // These are set when the first page arrives, and are never modified after.
  private volatile ColumnDefinitions columnDefinitions;

  // These change over time as different nodes are tried;
  // they can only be null before the first request is sent.
  private volatile Node node;
  private volatile DriverChannel channel;
  private volatile int streamId;
  // Set each time a new request/response cycle starts.
  private volatile long messageStartTimeNanos;
  private volatile Timeout timeout;

  // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
  // the first attempt, 1 for the first retry, etc.).
  private volatile int retryCount;

  public ContinuousCqlRequestHandler(
      @NonNull Statement<?> statement,
      @NonNull DefaultSession session,
      @NonNull InternalDriverContext context,
      @NonNull String sessionLogPrefix) {
    ProtocolVersion protocolVersion = context.getProtocolVersion();
    if (!context
        .getProtocolVersionRegistry()
        .supports(protocolVersion, DseProtocolFeature.CONTINUOUS_PAGING)) {
      throw new IllegalStateException(
          "Cannot execute continuous paging requests with protocol version " + protocolVersion);
    }
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    LOG.trace("[{}] Creating new continuous handler for request {}", logPrefix, statement);
    this.statement = statement;
    this.session = session;
    this.context = context;
    if (statement.getExecutionProfile() != null) {
      this.executionProfile = statement.getExecutionProfile();
    } else {
      DriverConfig config = context.getConfig();
      String profileName = statement.getExecutionProfileName();
      this.executionProfile =
          (profileName == null || profileName.isEmpty())
              ? config.getDefaultProfile()
              : config.getProfile(profileName);
    }
    this.queryPlan =
        statement.getNode() != null
            ? new QueryPlan(statement.getNode())
            : context
                .getLoadBalancingPolicyWrapper()
                .newQueryPlan(statement, executionProfile.getName(), session);
    this.retryPolicy = context.getRetryPolicy(executionProfile.getName());
    Boolean idempotent = statement.isIdempotent();
    this.isIdempotent =
        (idempotent == null)
            ? executionProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE)
            : idempotent;
    this.timeoutFirstPage =
        executionProfile.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_FIRST_PAGE);
    this.timeoutOtherPages =
        executionProfile.getDuration(DseDriverOption.CONTINUOUS_PAGING_TIMEOUT_OTHER_PAGES);
    this.timer = context.getNettyOptions().getTimer();
    this.maxEnqueuedPages =
        executionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES);
    this.queue = new ArrayDeque<>(maxEnqueuedPages);
    this.maxPages = executionProfile.getInt(DseDriverOption.CONTINUOUS_PAGING_MAX_PAGES);
    this.protocolBackpressureAvailable =
        protocolVersion.getCode() >= DseProtocolVersion.DSE_V2.getCode();
    this.numPagesRequested = protocolBackpressureAvailable ? maxEnqueuedPages : 0;
    this.message = DseConversions.toContinuousPagingMessage(statement, executionProfile, context);
    this.throttler = context.getRequestThrottler();
    this.throttler.register(this);
    this.sessionMetricUpdater = session.getMetricUpdater();
    this.startTimeNanos = System.nanoTime();
  }

  // MAIN LIFECYCLE

  @Override
  public void onStreamIdAssigned(int streamId) {
    LOG.trace("[{}] Assigned streamId {} on node {}", logPrefix, streamId, node);
    this.streamId = streamId;
  }

  @Override
  public boolean isLastResponse(@NonNull Frame responseFrame) {
    Message message = responseFrame.message;
    if (message instanceof Rows) {
      Rows rows = (Rows) message;
      DseRowsMetadata metadata = (DseRowsMetadata) rows.getMetadata();
      return metadata.isLastContinuousPage;
    } else {
      return message instanceof Error;
    }
  }

  @Override
  public void onThrottleReady(boolean wasDelayed) {
    if (wasDelayed) {
      session
          .getMetricUpdater()
          .updateTimer(
              DefaultSessionMetric.THROTTLING_DELAY,
              executionProfile.getName(),
              System.nanoTime() - startTimeNanos,
              TimeUnit.NANOSECONDS);
    }
    sendRequest(null);
  }

  public CompletionStage<ContinuousAsyncResultSet> handle() {
    return dequeueOrCreatePending();
  }

  /**
   * Sends the initial request to the next available node.
   *
   * @param node if not null, it will be attempted first before the rest of the query plan. It
   *     happens only when we retry on the same host.
   */
  private void sendRequest(@Nullable Node node) {
    channel = null;
    if (node == null || (channel = session.getChannel(node, logPrefix)) == null) {
      while ((node = queryPlan.poll()) != null) {
        channel = session.getChannel(node, logPrefix);
        if (channel != null) {
          break;
        }
      }
    }
    if (channel == null || node == null) {
      // We've reached the end of the query plan without finding any node to write to; abort the
      // continuous paging session.
      lock.lock();
      try {
        abort(AllNodesFailedException.fromErrors(errors), false);
      } finally {
        lock.unlock();
      }
    } else {
      this.node = node;
      streamId = -1;
      messageStartTimeNanos = System.nanoTime();
      channel.write(message, false, statement.getCustomPayload(), this).addListener(this);
    }
  }

  /**
   * Invoked when the write from {@link #sendRequest(Node)} completes.
   *
   * @param future The future representing the outcome of the write operation.
   */
  @Override
  public void operationComplete(@NonNull Future<java.lang.Void> future) {
    if (!future.isSuccess()) {
      Throwable error = future.cause();
      if (error instanceof EncoderException && error.getCause() instanceof FrameTooLongException) {
        trackNodeError(node, error.getCause());
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
        trackNodeError(node, error.getCause());
        sendRequest(null);
      }
    } else {
      LOG.trace("[{}] Request sent on {}", logPrefix, channel);
      timeout = scheduleTimeout(1);
    }
  }

  /**
   * Invoked when a continuous paging response is received, either a successful or failed one.
   *
   * <p>Delegates further processing to appropriate methods: {@link #processResultResponse(Result,
   * Frame)} if the response was successful, or {@link #processErrorResponse(Error)} if it wasn't.
   *
   * @param response the received {@link Frame}.
   */
  @Override
  public void onResponse(@NonNull Frame response) {
    stopNodeMessageTimer();
    cancelTimeout();
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
          processErrorResponse((Error) responseMessage);
        } else {
          IllegalStateException error =
              new IllegalStateException("Unexpected response " + responseMessage);
          trackNodeError(node, error);
          abort(error, false);
        }
      } catch (Throwable t) {
        trackNodeError(node, t);
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
   * #processRetryDecision(RetryDecision, Throwable)}.
   *
   * @param error the error encountered, usually a network problem.
   */
  @Override
  public void onFailure(@NonNull Throwable error) {
    cancelTimeout();
    LOG.trace(String.format("[%s] Request failure", logPrefix), error);
    RetryDecision decision;
    if (!isIdempotent || error instanceof FrameTooLongException) {
      decision = RetryDecision.RETHROW;
    } else {
      decision = retryPolicy.onRequestAborted(statement, error, retryCount);
    }
    updateErrorMetrics(
        ((DefaultNode) node).getMetricUpdater(),
        decision,
        DefaultNodeMetric.ABORTED_REQUESTS,
        DefaultNodeMetric.RETRIES_ON_ABORTED,
        DefaultNodeMetric.IGNORES_ON_ABORTED);
    lock.lock();
    try {
      processRetryDecision(decision, error);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    session
        .getMetricUpdater()
        .incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, executionProfile.getName());
    lock.lock();
    try {
      abort(error, false);
    } finally {
      lock.unlock();
    }
  }

  // PROCESSING METHODS

  /**
   * Processes a new result response, creating the corresponding {@link ContinuousAsyncResultSet}
   * object and then enqueuing it or serving it directly to the user if he was waiting for it.
   *
   * @param result the result to process. It is normally a {@link Rows} object, but may be a {@link
   *     Void} object if the retry policy decided to ignore an error.
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
                  String.format("Received page %d but was expecting %d", pageNumber, currentPage)),
              false);
        } else {
          DefaultContinuousAsyncResultSet resultSet = createResultSet((Rows) result, executionInfo);
          if (rowsMetadata.isLastContinuousPage) {
            LOG.trace(
                "[{}] Received last page ({} - {} rows)",
                logPrefix,
                pageNumber,
                resultSet.remaining());
            state = STATE_FINISHED;
            reenableAutoReadIfNeeded();
            enqueueOrCompletePending(resultSet);
            stopGlobalRequestTimer();
          } else {
            LOG.trace(
                "[{}] Received page {} ({} rows)", logPrefix, pageNumber, resultSet.remaining());
            if (currentPage > 0) {
              state = currentPage + 1;
            }
            enqueueOrCompletePending(resultSet);
          }
        }
      } else {
        // Void responses happen only when the retry decision is ignore.
        assert result instanceof Void;
        ContinuousAsyncResultSet resultSet = DefaultContinuousAsyncResultSet.empty(executionInfo);
        LOG.trace(
            "[{}] Continuous paging interrupted by retry policy decision to ignore error",
            logPrefix);
        state = STATE_FINISHED;
        reenableAutoReadIfNeeded();
        enqueueOrCompletePending(resultSet);
        stopGlobalRequestTimer();
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
  private void processErrorResponse(@NonNull Error errorMessage) {
    assert lock.isHeldByCurrentThread();
    if (errorMessage instanceof Unprepared) {
      processUnprepared((Unprepared) errorMessage);
    } else {
      CoordinatorException error = DseConversions.toThrowable(node, errorMessage, context);
      if (error instanceof BootstrappingException) {
        LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
        recordError(node, error);
        trackNodeError(node, error);
        sendRequest(null);
      } else if (error instanceof QueryValidationException
          || error instanceof FunctionFailureException
          || error instanceof ProtocolError
          || state > 1) {
        // we only process recoverable errors for the first page,
        // errors on subsequent pages will always trigger an immediate abortion
        LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
        NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
        metricUpdater.incrementCounter(DefaultNodeMetric.OTHER_ERRORS, executionProfile.getName());
        trackNodeError(node, error);
        abort(error, true);
      } else {
        processRecoverableError(error);
      }
    }
  }

  /**
   * Processes a recoverable error.
   *
   * <p>In most cases, delegates to the retry policy and its decision, see {@link
   * #processRetryDecision(RetryDecision, Throwable)}.
   *
   * @param error the recoverable error.
   */
  private void processRecoverableError(@NonNull CoordinatorException error) {
    assert lock.isHeldByCurrentThread();
    NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
    RetryDecision decision;
    if (error instanceof ReadTimeoutException) {
      ReadTimeoutException readTimeout = (ReadTimeoutException) error;
      decision =
          retryPolicy.onReadTimeout(
              statement,
              readTimeout.getConsistencyLevel(),
              readTimeout.getBlockFor(),
              readTimeout.getReceived(),
              readTimeout.wasDataPresent(),
              retryCount);
      updateErrorMetrics(
          metricUpdater,
          decision,
          DefaultNodeMetric.READ_TIMEOUTS,
          DefaultNodeMetric.RETRIES_ON_READ_TIMEOUT,
          DefaultNodeMetric.IGNORES_ON_READ_TIMEOUT);
    } else if (error instanceof WriteTimeoutException) {
      WriteTimeoutException writeTimeout = (WriteTimeoutException) error;
      if (isIdempotent) {
        decision =
            retryPolicy.onWriteTimeout(
                statement,
                writeTimeout.getConsistencyLevel(),
                writeTimeout.getWriteType(),
                writeTimeout.getBlockFor(),
                writeTimeout.getReceived(),
                retryCount);
      } else {
        decision = RetryDecision.RETHROW;
      }
      updateErrorMetrics(
          metricUpdater,
          decision,
          DefaultNodeMetric.WRITE_TIMEOUTS,
          DefaultNodeMetric.RETRIES_ON_WRITE_TIMEOUT,
          DefaultNodeMetric.IGNORES_ON_WRITE_TIMEOUT);
    } else if (error instanceof UnavailableException) {
      UnavailableException unavailable = (UnavailableException) error;
      decision =
          retryPolicy.onUnavailable(
              statement,
              unavailable.getConsistencyLevel(),
              unavailable.getRequired(),
              unavailable.getAlive(),
              retryCount);
      updateErrorMetrics(
          metricUpdater,
          decision,
          DefaultNodeMetric.UNAVAILABLES,
          DefaultNodeMetric.RETRIES_ON_UNAVAILABLE,
          DefaultNodeMetric.IGNORES_ON_UNAVAILABLE);
    } else {
      decision =
          isIdempotent
              ? retryPolicy.onErrorResponse(statement, error, retryCount)
              : RetryDecision.RETHROW;
      updateErrorMetrics(
          metricUpdater,
          decision,
          DefaultNodeMetric.OTHER_ERRORS,
          DefaultNodeMetric.RETRIES_ON_OTHER_ERROR,
          DefaultNodeMetric.IGNORES_ON_OTHER_ERROR);
    }
    processRetryDecision(decision, error);
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
              // If we run into an unrecoverable error, surface it to the client instead of retrying
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
                  trackNodeError(node, illegalStateException);
                  fatalError = illegalStateException;
                } else {
                  LOG.trace(
                      "[{}] Re-prepare successful, retrying on the same node ({})",
                      logPrefix,
                      node);
                  sendRequest(node);
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
                      trackNodeError(node, prepareError);
                      fatalError = prepareError;
                    }
                  }
                } else if (exception instanceof RequestThrottlingException) {
                  trackNodeError(node, exception);
                  fatalError = exception;
                }
                if (fatalError == null) {
                  LOG.trace("[{}] Re-prepare failed, trying next node", logPrefix);
                  recordError(node, exception);
                  trackNodeError(node, exception);
                  sendRequest(null);
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
   * @param decision the decision to process.
   * @param error the original error.
   */
  @SuppressWarnings({"NonAtomicOperationOnVolatileField", "NonAtomicVolatileUpdate"})
  private void processRetryDecision(@NonNull RetryDecision decision, @NonNull Throwable error) {
    assert lock.isHeldByCurrentThread();
    LOG.trace("[{}] Processing retry decision {}", logPrefix, decision);
    switch (decision) {
      case RETRY_SAME:
        recordError(node, error);
        trackNodeError(node, error);
        retryCount++;
        sendRequest(node);
        break;
      case RETRY_NEXT:
        recordError(node, error);
        trackNodeError(node, error);
        retryCount++;
        sendRequest(null);
        break;
      case RETHROW:
        trackNodeError(node, error);
        abort(error, true);
        break;
      case IGNORE:
        processResultResponse(Void.INSTANCE, null);
        break;
    }
  }

  // PAGE HANDLING

  /**
   * Enqueues a response or, if the client was already waiting for it, completes the pending future.
   *
   * <p>Guarded by {@link #lock}.
   *
   * @param pageOrError the next page, or an error.
   */
  @SuppressWarnings("GuardedBy") // this method is only called with the lock held
  private void enqueueOrCompletePending(@NonNull Object pageOrError) {
    assert lock.isHeldByCurrentThread();
    if (pendingResult != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(
            "[{}] Client was waiting on empty queue, completing with {}",
            logPrefix,
            asTraceString(pageOrError));
      }
      CompletableFuture<ContinuousAsyncResultSet> tmp = pendingResult;
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
      if (!protocolBackpressureAvailable && queue.size() == maxEnqueuedPages && state > 0) {
        LOG.trace(
            "[{}] Exceeded {} queued response pages, disabling auto-read", logPrefix, queue.size());
        channel.config().setAutoRead(false);
      }
    }
  }

  /**
   * Dequeue a response or, if the queue is empty, create the future that will get notified of the
   * next response, when it arrives.
   *
   * <p>Called from user code, see {@link DefaultContinuousAsyncResultSet#fetchNextPage()}.
   *
   * @return the next page's future; never null.
   */
  @NonNull
  protected CompletableFuture<ContinuousAsyncResultSet> dequeueOrCreatePending() {
    lock.lock();
    try {
      // If the client was already waiting for a page, there's no way it can call this method again
      // (this is guaranteed by our public API because in order to ask for the next page,
      // you need the reference to the previous page).
      assert pendingResult == null;

      Object head = queue.poll();
      if (!protocolBackpressureAvailable && head != null && queue.size() == maxEnqueuedPages - 1) {
        LOG.trace(
            "[{}] Back to {} queued response pages, re-enabling auto-read",
            logPrefix,
            queue.size());
        channel.config().setAutoRead(true);
      }
      maybeRequestMore();
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
          pendingResult = createResultSetFuture();
          // Only schedule a timeout if we're past the first page (the first page's timeout is
          // handled in sendRequest).
          if (state > 1) {
            timeout = scheduleTimeout(state);
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
   * half the queue size, then request more pages, unless the {@link #state} is failed, we're still
   * waiting for the first page (so maybe still throttled or in the middle of a retry), or we don't
   * support backpressure at the protocol level.
   */
  @SuppressWarnings("GuardedBy")
  private void maybeRequestMore() {
    assert lock.isHeldByCurrentThread();
    if (state < 2 || streamId == -1 || !protocolBackpressureAvailable) {
      return;
    }
    // if we have already requested more than the client needs, then no need to request some more
    if (maxPages > 0 && numPagesRequested >= maxPages) {
      return;
    }
    // the pages received so far, which is the state minus one
    int received = state - 1;
    int requested = numPagesRequested;
    // the pages that fit in the queue, which is the queue free space minus the requests in flight
    int freeSpace = maxEnqueuedPages - queue.size();
    int inFlight = requested - received;
    int numPagesFittingInQueue = freeSpace - inFlight;
    if (numPagesFittingInQueue >= maxEnqueuedPages / 2) {
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
            timeoutOtherPages,
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

  // TIMEOUT HANDLING

  private Timeout scheduleTimeout(int expectedPage) {
    if (expectedPage < 0) {
      return null;
    }
    Duration timeout = (expectedPage == 1) ? timeoutFirstPage : timeoutOtherPages;
    if (timeout.toNanos() <= 0) {
      return null;
    }
    LOG.trace("[{}] Scheduling timeout for page {} in {}", logPrefix, expectedPage, timeout);
    return timer.newTimeout(
        (timeout1) -> {
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
        },
        timeout.toNanos(),
        TimeUnit.NANOSECONDS);
  }

  /** Cancels the current timeout, if non null. */
  private void cancelTimeout() {
    Timeout timeout = this.timeout;
    if (timeout != null) {
      LOG.trace("[{}] Cancelling timeout", logPrefix);
      timeout.cancel();
    }
  }

  // CANCELLATION

  /**
   * Cancels the continuous paging request.
   *
   * <p>Called from user code, see {@link DefaultContinuousAsyncResultSet#cancel()}, or from a
   * driver I/O thread.
   */
  void cancel() {
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
        // the rest can be done without holding the lock, see below
      }
    } finally {
      lock.unlock();
    }
    if (channel != null) {
      if (!channel.closeFuture().isDone()) {
        this.channel.cancel(this);
      }
      sendCancelRequest();
    }
    reenableAutoReadIfNeeded();
  }

  private void sendCancelRequest() {
    LOG.trace("[{}] Sending cancel request", logPrefix);
    ThrottledAdminRequestHandler.query(
            channel,
            true,
            Revise.cancelContinuousPaging(streamId),
            statement.getCustomPayload(),
            timeoutOtherPages,
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

  private void recordError(@NonNull Node node, @NonNull Throwable error) {
    errors.add(new AbstractMap.SimpleEntry<>(node, error));
  }

  private void trackNodeError(@NonNull Node node, @NonNull Throwable error) {
    long latencyNanos = System.nanoTime() - this.messageStartTimeNanos;
    context
        .getRequestTracker()
        .onNodeError(statement, error, latencyNanos, executionProfile, node, logPrefix);
  }

  /**
   * Aborts the continuous paging session due to an error that can be either from the server or the
   * client.
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
  }

  // METRICS

  private void stopNodeMessageTimer() {
    ((DefaultNode) node)
        .getMetricUpdater()
        .updateTimer(
            DefaultNodeMetric.CQL_MESSAGES,
            executionProfile.getName(),
            System.nanoTime() - messageStartTimeNanos,
            TimeUnit.NANOSECONDS);
  }

  private void stopGlobalRequestTimer() {
    session
        .getMetricUpdater()
        .updateTimer(
            DseSessionMetric.CONTINUOUS_CQL_REQUESTS,
            executionProfile.getName(),
            System.nanoTime() - startTimeNanos,
            TimeUnit.NANOSECONDS);
  }

  private void updateErrorMetrics(
      @NonNull NodeMetricUpdater metricUpdater,
      @NonNull RetryDecision decision,
      @NonNull DefaultNodeMetric error,
      @NonNull DefaultNodeMetric retriesOnError,
      @NonNull DefaultNodeMetric ignoresOnError) {
    metricUpdater.incrementCounter(error, executionProfile.getName());
    switch (decision) {
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
  private DefaultExecutionInfo createExecutionInfo(
      @NonNull Result result, @Nullable Frame response) {
    ByteBuffer pagingState =
        result instanceof Rows ? ((Rows) result).getMetadata().pagingState : null;
    return new DefaultExecutionInfo(
        statement,
        node,
        0,
        0,
        errors,
        pagingState,
        response,
        true,
        session,
        context,
        executionProfile);
  }

  @NonNull
  private DefaultContinuousAsyncResultSet createResultSet(
      @NonNull Rows rows, @NonNull ExecutionInfo executionInfo) {
    Queue<List<ByteBuffer>> data = rows.getData();
    CountingIterator<Row> iterator =
        new CountingIterator<Row>(data.size()) {
          @Override
          protected Row computeNext() {
            List<ByteBuffer> rowData = data.poll();
            return (rowData == null)
                ? endOfData()
                : new DefaultRow(columnDefinitions, rowData, context);
          }
        };
    DseRowsMetadata metadata = (DseRowsMetadata) rows.getMetadata();
    return new DefaultContinuousAsyncResultSet(
        iterator,
        columnDefinitions,
        metadata.continuousPageNumber,
        !metadata.isLastContinuousPage,
        executionInfo,
        this);
  }

  @NonNull
  private CompletableFuture<ContinuousAsyncResultSet> createResultSetFuture() {
    CompletableFuture<ContinuousAsyncResultSet> future = new CompletableFuture<>();
    future.whenComplete(
        (rs, t) -> {
          if (t instanceof CancellationException) {
            // if the future has been canceled by the user, propagate the cancellation
            cancel();
          }
        });
    return future;
  }

  @NonNull
  private CompletableFuture<ContinuousAsyncResultSet> immediateResultSetFuture(
      @NonNull Object pageOrError) {
    CompletableFuture<ContinuousAsyncResultSet> future = createResultSetFuture();
    completeResultSetFuture(future, pageOrError);
    return future;
  }

  @NonNull
  private CompletableFuture<ContinuousAsyncResultSet> cancelledResultSetFuture() {
    return immediateResultSetFuture(
        new CancellationException(
            "Can't get more results because the continuous query has failed already. "
                + "Most likely this is because the query was cancelled"));
  }

  private void completeResultSetFuture(
      @NonNull CompletableFuture<ContinuousAsyncResultSet> future, @NonNull Object pageOrError) {
    long now = System.nanoTime();
    long totalLatencyNanos = now - startTimeNanos;
    long nodeLatencyNanos = now - messageStartTimeNanos;
    if (pageOrError instanceof ContinuousAsyncResultSet) {
      if (future.complete((ContinuousAsyncResultSet) pageOrError)) {
        throttler.signalSuccess(this);
        context
            .getRequestTracker()
            .onNodeSuccess(statement, nodeLatencyNanos, executionProfile, node, logPrefix);
        context
            .getRequestTracker()
            .onSuccess(statement, totalLatencyNanos, executionProfile, node, logPrefix);
      }
    } else {
      Throwable error = (Throwable) pageOrError;
      if (future.completeExceptionally(error)) {
        context
            .getRequestTracker()
            .onError(statement, error, totalLatencyNanos, executionProfile, node, logPrefix);
        if (error instanceof DriverTimeoutException) {
          throttler.signalTimeout(this);
          session
              .getMetricUpdater()
              .incrementCounter(
                  DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, executionProfile.getName());
        } else if (!(error instanceof RequestThrottlingException)) {
          throttler.signalError(this, error);
        }
      }
    }
  }

  @NonNull
  private static String asTraceString(@NonNull Object pageOrError) {
    return (pageOrError instanceof ContinuousAsyncResultSet)
        ? "page " + ((ContinuousAsyncResultSet) pageOrError).pageNumber()
        : ((Exception) pageOrError).getClass().getSimpleName();
  }
}
