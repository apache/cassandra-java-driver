/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.dse.driver.internal.core.DseProtocolFeature;
import com.datastax.dse.driver.internal.core.cql.DseConversions;
import com.datastax.dse.protocol.internal.request.Revise;
import com.datastax.dse.protocol.internal.response.result.DseRowsMetadata;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
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
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
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
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.driver.internal.core.tracker.RequestLogger;
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

  private final String logPrefix;
  protected final StatementT statement;
  protected final DefaultSession session;
  private final CqlIdentifier keyspace;
  protected final InternalDriverContext context;
  protected final DriverExecutionProfile executionProfile;
  private final Queue<Node> queryPlan;
  private final RetryPolicy retryPolicy;
  private final RequestThrottler throttler;
  private final boolean protocolBackpressureAvailable;
  private final boolean isIdempotent;
  private final Timer timer;
  private final SessionMetricUpdater sessionMetricUpdater;
  private final boolean specExecEnabled;
  private final SpeculativeExecutionPolicy speculativeExecutionPolicy;
  private final List<Timeout> scheduledExecutions;

  /**
   * The errors on the nodes that were already tried. We don't use a map because nodes can appear
   * multiple times.
   */
  protected final List<Map.Entry<Node, Throwable>> errors = new CopyOnWriteArrayList<>();

  /**
   * Represents the global state of the continuous paging request. This future is not exposed to
   * clients, it is used internally to track completion and cancellation.
   */
  private final CompletableFuture<Void> doneFuture = new CompletableFuture<>();

  /**
   * The list of in-flight executions, one per node. Executions may be triggered by speculative
   * executions or retries. An execution is added to this list when the write operation completes.
   * It is removed from this list when the callback has done reading responses.
   */
  private final List<NodeResponseCallback> inFlightCallbacks = new CopyOnWriteArrayList<>();

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

  /**
   * Coordinates concurrent accesses between the client and I/O threads that wish to enqueue and
   * dequeue items from the page queue.
   */
  private final ReentrantLock lock = new ReentrantLock();

  private final SessionMetric clientTimeoutsMetric;
  private final SessionMetric continuousRequestsMetric;
  private final NodeMetric messagesMetric;
  /**
   * The page queue, storing pages that we have received and have not been consumed by the client
   * yet. It can also store errors, when the operation completed exceptionally.
   */
  @GuardedBy("lock")
  private Queue<Object> queue;

  /**
   * If the client requests a page and we can't serve it immediately (empty queue), then we create
   * this future and have the client wait on it. Otherwise this field is null.
   */
  @GuardedBy("lock")
  private CompletableFuture<ResultSetT> pendingResult;

  /**
   * How many pages were requested. This is the total number of pages requested from the beginning.
   * It will be zero if the protocol does not support numPagesRequested (DSE_V1)
   */
  @GuardedBy("lock")
  private int numPagesRequested;

  /**
   * The node that has been chosen to deliver results. In case of speculative executions, this is
   * the first node that replies with either a result or an error.
   */
  @GuardedBy("lock")
  private NodeResponseCallback chosenExecution;

  // Set when the execution starts, and is never modified after.
  private volatile long startTimeNanos;

  private volatile Timeout globalTimeout;

  public ContinuousRequestHandlerBase(
      @NonNull StatementT statement,
      @NonNull DefaultSession session,
      @NonNull InternalDriverContext context,
      @NonNull String sessionLogPrefix,
      boolean specExecEnabled,
      SessionMetric clientTimeoutsMetric,
      SessionMetric continuousRequestsMetric,
      NodeMetric messagesMetric) {
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
    this.statement = statement;
    this.session = session;
    this.keyspace = session.getKeyspace().orElse(null);
    this.context = context;
    this.executionProfile = Conversions.resolveExecutionProfile(this.statement, this.context);
    this.queryPlan =
        statement.getNode() != null
            ? new QueryPlan(statement.getNode())
            : context
                .getLoadBalancingPolicyWrapper()
                .newQueryPlan(statement, executionProfile.getName(), session);
    this.retryPolicy = context.getRetryPolicy(executionProfile.getName());
    Boolean idempotent = statement.isIdempotent();
    this.isIdempotent =
        idempotent == null
            ? executionProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE)
            : idempotent;
    this.timer = context.getNettyOptions().getTimer();
    this.protocolBackpressureAvailable =
        protocolVersion.getCode() >= DseProtocolVersion.DSE_V2.getCode();
    this.throttler = context.getRequestThrottler();
    this.sessionMetricUpdater = session.getMetricUpdater();
    this.specExecEnabled = specExecEnabled && isIdempotent;
    this.speculativeExecutionPolicy =
        this.specExecEnabled
            ? context.getSpeculativeExecutionPolicy(executionProfile.getName())
            : null;
    this.scheduledExecutions = this.specExecEnabled ? new CopyOnWriteArrayList<>() : null;
  }

  /** @return The global timeout, or {@link Duration#ZERO} to disable it. */
  @NonNull
  protected abstract Duration getGlobalTimeoutDuration();

  /**
   * @return The timeout for page <code>pageNumber</code>, or {@link Duration#ZERO} to disable it.
   */
  @NonNull
  protected abstract Duration getPageTimeoutDuration(int pageNumber);

  /**
   * @return The timeout for REVISE requests (cancellation and backpressure), or {@link
   *     Duration#ZERO} to disable it.
   */
  @NonNull
  protected abstract Duration getReviseRequestTimeoutDuration();

  protected abstract int getMaxEnqueuedPages();

  protected abstract int getMaxPages();

  @NonNull
  protected abstract Message getMessage();

  protected abstract boolean isTracingEnabled();

  @NonNull
  protected abstract Map<String, ByteBuffer> createPayload();

  @NonNull
  protected abstract ResultSetT createResultSet(
      @NonNull Rows rows,
      @NonNull ExecutionInfo executionInfo,
      @NonNull ColumnDefinitions columnDefinitions)
      throws IOException;

  /** @return An empty result set; used only when the retry policy decides to ignore the error. */
  @NonNull
  protected abstract ResultSetT createEmptyResultSet(@NonNull ExecutionInfo executionInfo);

  protected abstract int pageNumber(@NonNull ResultSetT resultSet);

  public CompletionStage<ResultSetT> handle() {
    startTimeNanos = System.nanoTime();
    lock.lock();
    try {
      this.queue = new ArrayDeque<>(getMaxEnqueuedPages());
      this.numPagesRequested = protocolBackpressureAvailable ? getMaxEnqueuedPages() : 0;
    } finally {
      lock.unlock();
    }
    scheduleGlobalTimeout();
    // must be done last since it may trigger an immediate call to #onThrottleReady
    throttler.register(this);
    return dequeueOrCreatePending();
  }

  @Override
  public void onThrottleReady(boolean wasDelayed) {
    if (wasDelayed
        // avoid call to nanoTime() if metric is disabled:
        && sessionMetricUpdater.isEnabled(
            DefaultSessionMetric.THROTTLING_DELAY, executionProfile.getName())) {
      sessionMetricUpdater.updateTimer(
          DefaultSessionMetric.THROTTLING_DELAY,
          executionProfile.getName(),
          System.nanoTime() - startTimeNanos,
          TimeUnit.NANOSECONDS);
    }
    activeExecutionsCount.incrementAndGet();
    sendRequest(null, 0, 0, specExecEnabled);
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    if (sessionMetricUpdater.isEnabled(
        DefaultSessionMetric.THROTTLING_ERRORS, executionProfile.getName())) {
      session
          .getMetricUpdater()
          .incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, executionProfile.getName());
    }
    setFailed(null, error);
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
        }
      }
    }
    if (channel == null) {
      // We've reached the end of the query plan without finding any node to write to; abort the
      // continuous paging session.
      if (activeExecutionsCount.decrementAndGet() == 0) {
        AllNodesFailedException error = AllNodesFailedException.fromErrors(errors);
        setFailed(null, error);
      }
    } else {
      NodeResponseCallback nodeResponseCallback =
          new NodeResponseCallback(
              node,
              channel,
              currentExecutionIndex,
              retryCount,
              scheduleSpeculativeExecution,
              logPrefix);
      channel
          .write(getMessage(), isTracingEnabled(), createPayload(), nodeResponseCallback)
          .addListener(nodeResponseCallback);
    }
  }

  /**
   * Handles the interaction with a single node in the query plan.
   *
   * <p>An instance of this class is created each time we (re)try a node.
   */
  private class NodeResponseCallback
      implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>> {

    private final long nodeStartTimeNanos = System.nanoTime();
    private final Node node;
    private final DriverChannel channel;
    // The identifier of the current execution (0 for the initial execution, 1 for the first
    // speculative execution, etc.)
    private final int executionIndex;
    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt of each execution).
    private final int retryCount;
    private final boolean scheduleSpeculativeExecution;
    private final String logPrefix;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    // These are volatile because they can be accessed outside the event loop, mostly inside
    // dequeueOrCreatePending and maybeRequestMore, to check whether we need to set a new page
    // timeout or to determine if we need to send a new backpressure request.
    private volatile int streamId = -1;
    private volatile int currentPage = 1;
    private volatile Timeout pageTimeout;

    private ColumnDefinitions columnDefinitions;

    // SpeculativeExecution node metrics should be executed only for the first page (first
    // invocation)
    private final AtomicBoolean stopNodeMessageTimerReported = new AtomicBoolean(false);
    private final AtomicBoolean nodeErrorReported = new AtomicBoolean(false);
    private final AtomicBoolean nodeSuccessReported = new AtomicBoolean(false);

    NodeResponseCallback(
        Node node,
        DriverChannel channel,
        int executionIndex,
        int retryCount,
        boolean scheduleSpeculativeExecution,
        String logPrefix) {
      this.node = node;
      this.channel = channel;
      this.executionIndex = executionIndex;
      this.retryCount = retryCount;
      this.scheduleSpeculativeExecution = scheduleSpeculativeExecution;
      this.logPrefix = logPrefix + "|" + executionIndex;
    }

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

    /**
     * Invoked when the write of a request completes.
     *
     * @param future The future representing the outcome of the write operation.
     */
    @Override
    public void operationComplete(@NonNull Future<java.lang.Void> future) {
      if (!future.isSuccess()) {
        Throwable error = future.cause();
        if (error instanceof EncoderException
            && error.getCause() instanceof FrameTooLongException) {
          trackNodeError(error.getCause());
          handleNodeFailure(error.getCause());
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
          trackNodeError(error);
          sendRequest(null, executionIndex, retryCount, scheduleSpeculativeExecution);
        }
      } else {
        LOG.trace("[{}] Request sent on {}", logPrefix, channel);
        if (doneFuture.isDone()) {
          cancelExecution();
        } else {
          inFlightCallbacks.add(this);
          if (scheduleSpeculativeExecution && currentPage == 1) {
            int nextExecution = executionIndex + 1;
            // Note that `node` is the first node of the execution, it might not be the "slow" one
            // if there were retries, but in practice retries are rare.
            long nextDelay =
                speculativeExecutionPolicy.nextExecution(node, keyspace, statement, nextExecution);
            if (nextDelay >= 0) {
              scheduleSpeculativeExecution(nextExecution, nextDelay);
            } else {
              LOG.trace(
                  "[{}] Speculative execution policy returned {}, no next execution",
                  logPrefix,
                  nextDelay);
            }
          }
          schedulePageTimeout();
        }
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
                  if (!doneFuture.isDone()) {
                    LOG.trace(
                        "[{}] Starting speculative execution {}", logPrefix, nextExecutionIndex);
                    activeExecutionsCount.incrementAndGet();
                    startedSpeculativeExecutionsCount.incrementAndGet();
                    incrementNodeSpecExecMetric();
                    sendRequest(null, nextExecutionIndex, 0, true);
                  }
                },
                delay,
                TimeUnit.MILLISECONDS));
      } catch (IllegalStateException e) {
        logTimeoutSchedulingError(e);
      }
    }

    private void schedulePageTimeout() {
      int expectedPage = currentPage;
      if (expectedPage < 0) {
        pageTimeout = null;
        return;
      }
      Duration timeout = getPageTimeoutDuration(expectedPage);
      if (timeout.toNanos() <= 0) {
        pageTimeout = null;
        return;
      }
      LOG.trace("[{}] Scheduling timeout for page {} in {}", logPrefix, expectedPage, timeout);
      try {
        pageTimeout =
            timer.newTimeout(
                timeout1 -> {
                  if (currentPage == expectedPage) {
                    LOG.trace(
                        "[{}] Timeout fired for page {}, cancelling execution",
                        logPrefix,
                        currentPage);
                    handleNodeFailure(
                        new DriverTimeoutException(
                            String.format("Timed out waiting for page %d", expectedPage)));
                  } else {
                    // Ignore timeout if the request has moved on in the interim.
                    LOG.trace(
                        "[{}] Timeout fired for page {} but query already at state {}, skipping",
                        logPrefix,
                        expectedPage,
                        currentPage);
                  }
                },
                timeout.toNanos(),
                TimeUnit.NANOSECONDS);
      } catch (IllegalStateException e) {
        logTimeoutSchedulingError(e);
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
      cancelPageTimeout();
      if (doneFuture.isDone()) {
        LOG.trace(
            "[{}] Got result but the request has been cancelled or completed by another execution, ignoring",
            logPrefix);
        // cancel to make sure the server will stop sending pages
        cancelExecution();
        return;
      }
      try {
        logServerWarnings(response.warnings);
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
          trackNodeError(error);
          handleNodeFailure(error);
        }
      } catch (Throwable t) {
        trackNodeError(t);
        handleNodeFailure(t);
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
      // do not update node metrics
      cancelPageTimeout();
      if (doneFuture.isDone()) {
        cancelExecution();
        return;
      }
      LOG.trace("[{}] Request failure, processing: {}", logPrefix, error);
      RetryDecision decision;
      if (!isIdempotent || error instanceof FrameTooLongException) {
        decision = RetryDecision.RETHROW;
      } else {
        decision = retryPolicy.onRequestAborted(statement, error, retryCount);
      }
      updateNodeErrorMetrics(
          decision,
          DefaultNodeMetric.ABORTED_REQUESTS,
          DefaultNodeMetric.RETRIES_ON_ABORTED,
          DefaultNodeMetric.IGNORES_ON_ABORTED);
      processRetryDecision(decision, error);
    }

    /**
     * Processes a new result response, creating the corresponding {@link ResultSetT} object and
     * then enqueuing it or serving it directly to the user if he was waiting for it.
     *
     * @param result the result to process. It is normally a {@link Rows} object, but may be a
     *     {@link Void} object if the retry policy decided to ignore an error.
     * @param frame the {@link Frame} (used to create the {@link ExecutionInfo} the first time).
     */
    private void processResultResponse(@NonNull Result result, @Nullable Frame frame) {
      try {
        if (setChosenExecution(this)) {
          ExecutionInfo executionInfo = createExecutionInfo(node, frame, executionIndex);
          if (result instanceof Rows) {
            DseRowsMetadata rowsMetadata = (DseRowsMetadata) ((Rows) result).getMetadata();
            int pageNumber = rowsMetadata.continuousPageNumber;
            int currentPage = this.currentPage;
            if (pageNumber != currentPage) {
              IllegalStateException error =
                  new IllegalStateException(
                      String.format(
                          "Received page %d but was expecting %d", pageNumber, currentPage));
              handleNodeFailure(error);
            } else {
              if (columnDefinitions == null) {
                // Contrary to ROWS responses from regular queries,
                // the first page always includes metadata so we use this
                // regardless of whether or not the query was from a prepared statement.
                columnDefinitions = Conversions.toColumnDefinitions(rowsMetadata, context);
              }
              ResultSetT resultSet =
                  createResultSet((Rows) result, executionInfo, columnDefinitions);
              if (rowsMetadata.isLastContinuousPage) {
                int pageSize = ((Rows) result).getData().size();
                LOG.trace(
                    "[{}] Received last page ({} - {} rows)", logPrefix, pageNumber, pageSize);
                stopExecution();
                setCompleted(this);
              } else {
                int pageSize = ((Rows) result).getData().size();
                LOG.trace("[{}] Received page {} ({} rows)", logPrefix, pageNumber, pageSize);
                this.currentPage = currentPage + 1;
              }
              enqueueOrCompletePending(resultSet);
              trackNodeSuccess();
            }
          } else {
            // Void responses happen only when the retry decision is ignore.
            assert result instanceof Void;
            LOG.trace(
                "[{}] Continuous paging interrupted by retry policy decision to ignore error",
                logPrefix);
            ResultSetT resultSet = createEmptyResultSet(executionInfo);
            stopExecution();
            setCompleted(this);
            enqueueOrCompletePending(resultSet);
            trackNodeSuccess();
          }
        } else {
          LOG.trace(
              "[{}] Discarding response from execution {} because another execution was chosen",
              logPrefix,
              executionIndex);
          cancelExecution();
        }
      } catch (Throwable error) {
        trackNodeError(error);
        handleNodeFailure(error);
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
    private void processErrorResponse(@NonNull Error errorMessage) {
      // graph does not use prepared statements
      if (errorMessage instanceof Unprepared) {
        processUnprepared((Unprepared) errorMessage);
      } else {
        CoordinatorException error = DseConversions.toThrowable(node, errorMessage, context);
        if (error instanceof BootstrappingException) {
          LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
          recordError(node, error);
          trackNodeError(error);
          sendRequest(null, executionIndex, retryCount, false);
        } else if (error instanceof QueryValidationException
            || error instanceof FunctionFailureException
            || error instanceof ProtocolError
            || currentPage > 1) {
          // we only process recoverable errors for the first page,
          // errors on subsequent pages will always trigger an immediate abortion
          LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
          NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
          metricUpdater.incrementCounter(
              DefaultNodeMetric.OTHER_ERRORS, executionProfile.getName());
          trackNodeError(error);
          handleNodeFailure(error);
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
        updateNodeErrorMetrics(
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
        updateNodeErrorMetrics(
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
        updateNodeErrorMetrics(
            decision,
            DefaultNodeMetric.UNAVAILABLES,
            DefaultNodeMetric.RETRIES_ON_UNAVAILABLE,
            DefaultNodeMetric.IGNORES_ON_UNAVAILABLE);
      } else {
        decision =
            isIdempotent
                ? retryPolicy.onErrorResponse(statement, error, retryCount)
                : RetryDecision.RETHROW;
        updateNodeErrorMetrics(
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
    private void processUnprepared(@NonNull Unprepared errorMessage) {
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
                    fatalError =
                        new IllegalStateException(
                            String.format(
                                "ID mismatch while trying to reprepare (expected %s, got %s). "
                                    + "This prepared statement won't work anymore. "
                                    + "This usually happens when you run a 'USE...' query after "
                                    + "the statement was prepared.",
                                Bytes.toHexString(idToReprepare), Bytes.toHexString(repreparedId)));
                    trackNodeError(fatalError);
                  } else {
                    LOG.trace(
                        "[{}] Re-prepare successful, retrying on the same node ({})",
                        logPrefix,
                        node);
                    stopExecution();
                    sendRequest(node, executionIndex, retryCount, false);
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
                        trackNodeError(prepareError);
                        fatalError = prepareError;
                      }
                    }
                  } else if (exception instanceof RequestThrottlingException) {
                    trackNodeError(exception);
                    fatalError = exception;
                  }
                  if (fatalError == null) {
                    LOG.trace("[{}] Re-prepare failed, trying next node", logPrefix);
                    recordError(node, exception);
                    trackNodeError(exception);
                    stopExecution();
                    sendRequest(null, executionIndex, retryCount, false);
                  } else {
                    handleNodeFailure(fatalError);
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
    private void processRetryDecision(@NonNull RetryDecision decision, @NonNull Throwable error) {
      LOG.trace("[{}] Processing retry decision {}", logPrefix, decision);
      switch (decision) {
        case RETRY_SAME:
          recordError(node, error);
          trackNodeError(error);
          stopExecution();
          sendRequest(node, executionIndex, retryCount + 1, false);
          break;
        case RETRY_NEXT:
          recordError(node, error);
          trackNodeError(error);
          stopExecution();
          sendRequest(null, executionIndex, retryCount + 1, false);
          break;
        case RETHROW:
          trackNodeError(error);
          handleNodeFailure(error);
          break;
        case IGNORE:
          processResultResponse(Void.INSTANCE, null);
          break;
      }
    }

    private void logServerWarnings(List<String> warnings) {
      // log the warnings if they have NOT been disabled
      if (warnings != null
          && !warnings.isEmpty()
          && executionProfile.getBoolean(DefaultDriverOption.REQUEST_LOG_WARNINGS)
          && LOG.isWarnEnabled()) {
        // use the RequestLogFormatter to format the query
        StringBuilder statementString = new StringBuilder();
        context
            .getRequestLogFormatter()
            .appendRequest(
                statement,
                executionProfile.getInt(
                    DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
                    RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_QUERY_LENGTH),
                executionProfile.getBoolean(
                    DefaultDriverOption.REQUEST_LOGGER_VALUES,
                    RequestLogger.DEFAULT_REQUEST_LOGGER_SHOW_VALUES),
                executionProfile.getInt(
                    DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES,
                    RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUES),
                executionProfile.getInt(
                    DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
                    RequestLogger.DEFAULT_REQUEST_LOGGER_MAX_VALUE_LENGTH),
                statementString);
        // log each warning separately
        warnings.forEach(
            (warning) ->
                LOG.warn(
                    "Query '{}' generated server side warning(s): {}", statementString, warning));
      }
    }

    private void stopNodeMessageTimer() {
      NodeMetricUpdater nodeMetricUpdater = ((DefaultNode) node).getMetricUpdater();
      if (nodeMetricUpdater.isEnabled(messagesMetric, executionProfile.getName())
          && stopNodeMessageTimerReported.compareAndSet(false, true)) {
        nodeMetricUpdater.updateTimer(
            messagesMetric,
            executionProfile.getName(),
            System.nanoTime() - nodeStartTimeNanos,
            TimeUnit.NANOSECONDS);
      }
    }

    private void incrementNodeSpecExecMetric() {
      NodeMetricUpdater nodeMetricUpdater = ((DefaultNode) node).getMetricUpdater();
      if (nodeMetricUpdater.isEnabled(
          DefaultNodeMetric.SPECULATIVE_EXECUTIONS, executionProfile.getName())) {
        nodeMetricUpdater.incrementCounter(
            DefaultNodeMetric.SPECULATIVE_EXECUTIONS, executionProfile.getName());
      }
    }

    private void updateNodeErrorMetrics(
        @NonNull RetryDecision decision,
        @NonNull DefaultNodeMetric error,
        @NonNull DefaultNodeMetric retriesOnError,
        @NonNull DefaultNodeMetric ignoresOnError) {
      NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
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

    private void trackNodeSuccess() {
      RequestTracker requestTracker = context.getRequestTracker();
      if (!(requestTracker instanceof NoopRequestTracker)
          && nodeSuccessReported.compareAndSet(false, true)) {
        long latencyNanos = System.nanoTime() - nodeStartTimeNanos;
        requestTracker.onNodeSuccess(statement, latencyNanos, executionProfile, node, logPrefix);
      }
    }

    private void trackNodeError(@NonNull Throwable error) {
      RequestTracker requestTracker = context.getRequestTracker();
      if (!(requestTracker instanceof NoopRequestTracker)
          && nodeErrorReported.compareAndSet(false, true)) {
        long latencyNanos = System.nanoTime() - nodeStartTimeNanos;
        requestTracker.onNodeError(
            statement, error, latencyNanos, executionProfile, node, logPrefix);
      }
    }

    private void cancelPageTimeout() {
      if (pageTimeout != null) {
        LOG.trace("[{}] Cancelling page timeout", logPrefix);
        pageTimeout.cancel();
      }
    }

    /**
     * Cancels this execution (see below) and tries to define itself as the chosen execution, in
     * which case, it will also fail the entire operation.
     */
    private void handleNodeFailure(Throwable error) {
      cancelExecution();
      if (setChosenExecution(this)) {
        setFailed(this, error);
      } else {
        LOG.trace(
            "[{}] Discarding error from execution {} because another execution was chosen",
            logPrefix,
            executionIndex);
      }
    }

    /**
     * Cancels this execution. It will only actually cancel once, other invocations are no-ops. A
     * cancellation consists of: cancelling this callback, sending a cancel request to the server,
     * and stopping the execution (final cleanup).
     *
     * <p>Cancellation can happen when the execution fails, when the users cancels a future, or when
     * a global timeout is fired.
     */
    private void cancelExecution() {
      if (cancelled.compareAndSet(false, true)) {
        try {
          LOG.trace("[{}] Cancelling execution", logPrefix);
          if (!channel.closeFuture().isDone()) {
            channel.cancel(this);
          }
          sendCancelRequest();
          stopExecution();
        } catch (Throwable t) {
          Loggers.warnWithException(
              LOG, "[{}] Error cancelling execution {}", logPrefix, executionIndex, t);
        }
      }
    }

    /**
     * Stops the execution, that is, removes this callback from the in-flight list and re-enables
     * autoread. In general one should call {@link #cancelExecution()}, unless it is certain that
     * the server has stopped sending responses and the channel is in autoread.
     */
    private void stopExecution() {
      inFlightCallbacks.remove(this);
      enableAutoReadIfNeeded();
    }

    private void enableAutoReadIfNeeded() {
      // Make sure we don't leave the channel unreadable
      if (!protocolBackpressureAvailable) {
        LOG.trace("[{}] Re-enabling auto-read", logPrefix);
        channel.config().setAutoRead(true);
      }
    }

    private void disableAutoReadIfNeeded() {
      if (!protocolBackpressureAvailable) {
        LOG.trace("[{}] Disabling auto-read", logPrefix);
        channel.config().setAutoRead(false);
      }
    }

    private void sendCancelRequest() {
      LOG.trace("[{}] Sending cancel request", logPrefix);
      // Note: In DSE_V1, the cancellation message is called CANCEL, and in DSE_V2, it's
      // called REVISE_REQUEST, but their structure is identical which is why we don't need to
      // distinguish which protocol is being used.
      ThrottledAdminRequestHandler.query(
              channel,
              true,
              Revise.cancelContinuousPaging(streamId),
              statement.getCustomPayload(),
              getReviseRequestTimeoutDuration(),
              throttler,
              session.getMetricUpdater(),
              logPrefix,
              "cancel request")
          .start()
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  LOG.debug(
                      "[{}] Error sending cancel request: {}. "
                          + "This is not critical (the request will eventually time out server-side).",
                      logPrefix,
                      error);
                } else {
                  LOG.trace("[{}] Cancel request sent successfully", logPrefix);
                }
              });
    }

    /**
     * Sends a request for more pages (a.k.a. backpressure request). This method should only be
     * invoked if protocol backpressure is available.
     *
     * @param nextPages the number of extra pages to request.
     */
    private void sendMorePagesRequest(int nextPages) {
      assert protocolBackpressureAvailable
          : "REVISE_REQUEST messages with revision type 2 require DSE_V2 or higher";
      LOG.trace("[{}] Sending request for more pages", logPrefix);
      ThrottledAdminRequestHandler.query(
              channel,
              true,
              Revise.requestMoreContinuousPages(streamId, nextPages),
              statement.getCustomPayload(),
              getReviseRequestTimeoutDuration(),
              throttler,
              session.getMetricUpdater(),
              logPrefix,
              "request " + nextPages + " more pages for id " + streamId)
          .start()
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  Loggers.warnWithException(
                      LOG, "[{}] Error requesting more pages, aborting.", logPrefix, error);
                  handleNodeFailure(error);
                }
              });
    }
  }

  /**
   * Sets the given node callback as the callback that has been chosen to deliver results.
   *
   * <p>Note that this method can be called many times, but only the first invocation will actually
   * set the chosen callback. All subsequent invocations will return true if the given callback is
   * the same as the previously chosen one. This means that once the chosen callback is set, it
   * cannot change anymore.
   *
   * @param execution The node callback to set.
   * @return true if the given node callback is now (or was already) the chosen one.
   */
  private boolean setChosenExecution(@NonNull NodeResponseCallback execution) {
    boolean isChosen;
    boolean wasSet = false;
    lock.lock();
    try {
      if (chosenExecution == null) {
        chosenExecution = execution;
        wasSet = true;
      }
      isChosen = chosenExecution == execution;
    } finally {
      lock.unlock();
    }
    if (wasSet) {
      // cancel all other pending executions, except the chosen one
      cancelScheduledTasks(execution);
    }
    return isChosen;
  }

  /**
   * Enqueues a response or, if the client was already waiting for it, completes the pending future.
   *
   * <p>Guarded by {@link #lock}.
   *
   * @param pageOrError the next page, or an error.
   */
  private void enqueueOrCompletePending(@NonNull Object pageOrError) {
    lock.lock();
    try {
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
            && chosenExecution != null
            && queue.size() == getMaxEnqueuedPages()
            && !doneFuture.isDone()) {
          LOG.trace(
              "[{}] Exceeded {} queued response pages, disabling auto-read",
              logPrefix,
              queue.size());
          chosenExecution.disableAutoReadIfNeeded();
        }
      }
    } finally {
      lock.unlock();
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
      // If the client was already waiting for a page, there's no way it can call this method again
      // (this is guaranteed by our public API because in order to ask for the next page,
      // you need the reference to the previous page).
      assert pendingResult == null;
      if (doneFuture.isCancelled()) {
        LOG.trace(
            "[{}] Client requested next page on cancelled operation, returning cancelled future",
            logPrefix);
        return newCancelledResultSetFuture();
      }
      Object head = queue.poll();
      maybeRequestMore();
      if (head == null) {
        LOG.trace(
            "[{}] Client requested next page but queue is empty, installing future", logPrefix);
        pendingResult = newPendingResultSetFuture();
        // Only schedule a timeout if we're past the first page (the first page's timeout is
        // handled in NodeResponseCallback.operationComplete).
        if (chosenExecution != null && chosenExecution.currentPage > 1) {
          chosenExecution.schedulePageTimeout();
          // Note: each new page timeout is cancelled when the next response arrives, see
          // onResponse(Frame).
        }
        return pendingResult;
      } else {
        if (!protocolBackpressureAvailable
            && chosenExecution != null
            && queue.size() == getMaxEnqueuedPages() - 1) {
          LOG.trace(
              "[{}] Back to {} queued response pages, re-enabling auto-read",
              logPrefix,
              queue.size());
          chosenExecution.enableAutoReadIfNeeded();
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace(
              "[{}] Client requested next page on non-empty queue, returning immediate future of {}",
              logPrefix,
              asTraceString(head));
        }
        return newCompletedResultSetFuture(head);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * If the total number of results in the queue and in-flight (requested - received) is less than
   * half the queue size, then request more pages, unless the {@link #doneFuture} is failed, we're
   * still waiting for the first page (so maybe still throttled or in the middle of a retry), or we
   * don't support backpressure at the protocol level.
   */
  @SuppressWarnings("GuardedBy")
  private void maybeRequestMore() {
    assert lock.isHeldByCurrentThread();
    if (doneFuture.isDone()
        || chosenExecution == null
        || chosenExecution.streamId == -1
        || chosenExecution.currentPage == 1
        || !protocolBackpressureAvailable) {
      return;
    }
    // if we have already requested more than the client needs, then no need to request some more
    if (getMaxPages() > 0 && numPagesRequested >= getMaxPages()) {
      return;
    }
    // the pages received so far, which is the current page minus one
    int received = chosenExecution.currentPage - 1;
    int requested = numPagesRequested;
    // the pages that fit in the queue, which is the queue free space minus the requests in flight
    int freeSpace = getMaxEnqueuedPages() - queue.size();
    int inFlight = requested - received;
    int numPagesFittingInQueue = freeSpace - inFlight;
    if (numPagesFittingInQueue > 0 && numPagesFittingInQueue >= getMaxEnqueuedPages() / 2) {
      LOG.trace("[{}] Requesting more {} pages", logPrefix, numPagesFittingInQueue);
      numPagesRequested = requested + numPagesFittingInQueue;
      chosenExecution.sendMorePagesRequest(numPagesFittingInQueue);
    }
  }

  private void scheduleGlobalTimeout() {
    Duration globalTimeout = getGlobalTimeoutDuration();
    if (globalTimeout.toNanos() <= 0) {
      return;
    }
    LOG.trace("[{}] Scheduling global timeout for pages in {}", logPrefix, globalTimeout);
    try {
      this.globalTimeout =
          timer.newTimeout(
              timeout -> {
                DriverTimeoutException error =
                    new DriverTimeoutException("Query timed out after " + globalTimeout);
                setFailed(null, error);
              },
              globalTimeout.toNanos(),
              TimeUnit.NANOSECONDS);
    } catch (IllegalStateException e) {
      logTimeoutSchedulingError(e);
    }
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
        callback.cancelExecution();
      }
    }
  }

  /**
   * Cancels the continuous paging request.
   *
   * <p>Called from user code, see {@link DefaultContinuousAsyncResultSet#cancel()}.
   */
  public void cancel() {
    if (doneFuture.cancel(true)) {
      lock.lock();
      try {
        LOG.trace("[{}] Cancelling continuous paging session", logPrefix);
        if (pendingResult != null) {
          pendingResult.cancel(true);
        }
      } finally {
        lock.unlock();
      }
      cancelGlobalTimeout();
      cancelScheduledTasks(null);
    }
  }

  @NonNull
  private CompletableFuture<ResultSetT> newPendingResultSetFuture() {
    CompletableFuture<ResultSetT> future = new CompletableFuture<>();
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
  private CompletableFuture<ResultSetT> newCompletedResultSetFuture(@NonNull Object pageOrError) {
    CompletableFuture<ResultSetT> future = newPendingResultSetFuture();
    completeResultSetFuture(future, pageOrError);
    return future;
  }

  @NonNull
  private CompletableFuture<ResultSetT> newCancelledResultSetFuture() {
    return newCompletedResultSetFuture(
        new CancellationException(
            "Can't get more results because the continuous query has failed already. "
                + "Most likely this is because the query was cancelled"));
  }

  @SuppressWarnings("unchecked")
  private void completeResultSetFuture(
      @NonNull CompletableFuture<ResultSetT> future, @NonNull Object pageOrError) {
    if (pageOrError instanceof Throwable) {
      Throwable error = (Throwable) pageOrError;
      future.completeExceptionally(error);
    } else {
      future.complete((ResultSetT) pageOrError);
    }
  }

  @NonNull
  private ExecutionInfo createExecutionInfo(
      @NonNull Node node, @Nullable Frame response, int successfulExecutionIndex) {
    return new DefaultExecutionInfo(
        statement,
        node,
        startedSpeculativeExecutionsCount.get(),
        successfulExecutionIndex,
        errors,
        null,
        response,
        true,
        session,
        context,
        executionProfile);
  }

  /**
   * Called from the chosen execution when it completes successfully.
   *
   * @param callback The callback that completed the operation, that is, the chosen execution.
   */
  private void setCompleted(@NonNull NodeResponseCallback callback) {
    if (doneFuture.complete(null)) {
      cancelGlobalTimeout();
      throttler.signalSuccess(this);
      RequestTracker requestTracker = context.getRequestTracker();
      boolean requestTrackerEnabled = !(requestTracker instanceof NoopRequestTracker);
      boolean metricEnabled =
          sessionMetricUpdater.isEnabled(continuousRequestsMetric, executionProfile.getName());
      if (requestTrackerEnabled || metricEnabled) {
        long now = System.nanoTime();
        long totalLatencyNanos = now - startTimeNanos;
        if (requestTrackerEnabled) {
          requestTracker.onSuccess(
              statement, totalLatencyNanos, executionProfile, callback.node, logPrefix);
        }
        if (metricEnabled) {
          sessionMetricUpdater.updateTimer(
              continuousRequestsMetric,
              executionProfile.getName(),
              totalLatencyNanos,
              TimeUnit.NANOSECONDS);
        }
      }
    }
  }

  /**
   * Called when the operation encounters an error and must abort. Can happen in the following
   * cases:
   *
   * <ol>
   *   <li>The throttler failed;
   *   <li>All nodes tried failed;
   *   <li>The global timeout was fired;
   *   <li>The chosen node callback failed.
   * </ol>
   *
   * @param callback The callback that signals the error, if present (in which case the method is
   *     being called from the chosen execution), or null if none (in which case the method is being
   *     called because of cancellation or timeout).
   */
  private void setFailed(@Nullable NodeResponseCallback callback, @NonNull Throwable error) {
    if (doneFuture.completeExceptionally(error)) {
      cancelGlobalTimeout();
      // Must be called here in case we are failing because the global timeout fired
      cancelScheduledTasks(null);
      if (callback != null && error instanceof DriverException) {
        ExecutionInfo executionInfo =
            createExecutionInfo(callback.node, null, callback.executionIndex);
        ((DriverException) error).setExecutionInfo(executionInfo);
      }
      enqueueOrCompletePending(error);
      RequestTracker requestTracker = context.getRequestTracker();
      if (!(requestTracker instanceof NoopRequestTracker)) {
        long now = System.nanoTime();
        long totalLatencyNanos = now - startTimeNanos;
        requestTracker.onError(
            statement,
            error,
            totalLatencyNanos,
            executionProfile,
            callback == null ? null : callback.node,
            logPrefix);
      }
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
        if (sessionMetricUpdater.isEnabled(clientTimeoutsMetric, executionProfile.getName())) {
          sessionMetricUpdater.incrementCounter(clientTimeoutsMetric, executionProfile.getName());
        }
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
  }

  private void recordError(@NonNull Node node, @NonNull Throwable error) {
    errors.add(new AbstractMap.SimpleEntry<>(node, error));
  }

  private void logTimeoutSchedulingError(IllegalStateException timeoutError) {
    // If we're racing with session shutdown, the timer might be stopped already. We don't want
    // to schedule more executions anyway, so swallow the error.
    if (!"cannot be started once stopped".equals(timeoutError.getMessage())) {
      Loggers.warnWithException(
          LOG, "[{}] Error while scheduling timeout", logPrefix, timeoutError);
    }
  }

  @SuppressWarnings("unchecked")
  @NonNull
  private String asTraceString(@NonNull Object pageOrError) {
    return pageOrError instanceof Throwable
        ? ((Exception) pageOrError).getClass().getSimpleName()
        : "page " + pageNumber((ResultSetT) pageOrError);
  }

  @VisibleForTesting
  @NonNull
  CompletableFuture<Void> getDoneFuture() {
    return doneFuture;
  }

  @VisibleForTesting
  @Nullable
  CompletableFuture<ResultSetT> getPendingResult() {
    lock.lock();
    try {
      return pendingResult;
    } finally {
      lock.unlock();
    }
  }

  @VisibleForTesting
  @Nullable
  public Timeout getGlobalTimeout() {
    return globalTimeout;
  }
}
