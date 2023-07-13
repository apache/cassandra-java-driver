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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
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
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.DefaultExecutionInfo;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.Void;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.handler.codec.EncoderException;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ScheduledFuture;
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
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class GraphRequestHandler implements Throttled {

  private static final Logger LOG = LoggerFactory.getLogger(GraphRequestHandler.class);

  private final long startTimeNanos;
  private final String logPrefix;

  private final DefaultSession session;

  private final InternalDriverContext context;
  private Queue<Node> queryPlan;
  private final DriverExecutionProfile executionProfile;

  private final GraphStatement<?> graphStatement;

  private final boolean isIdempotent;
  protected final CompletableFuture<AsyncGraphResultSet> result;
  private final Message message;
  private final String subProtocol;
  private final EventExecutor scheduler;

  /**
   * How many speculative executions are currently running (including the initial execution). We
   * track this in order to know when to fail the request if all executions have reached the end of
   * the query plan.
   */
  private final AtomicInteger activeExecutionsCount;

  /**
   * How many speculative executions have started (excluding the initial execution), whether they
   * have completed or not. We track this in order to fill {@link
   * ExecutionInfo#getSpeculativeExecutionCount()}.
   */
  private final AtomicInteger startedSpeculativeExecutionsCount;

  private final SpeculativeExecutionPolicy speculativeExecutionPolicy;

  private final ScheduledFuture<?> timeoutFuture;
  private final List<ScheduledFuture<?>> scheduledExecutions;
  private final List<PerRequestCallback> inFlightCallbacks;
  private final RetryPolicy retryPolicy;
  private final RequestThrottler throttler;

  // The errors on the nodes that were already tried (lazily initialized on the first error).
  // We don'traversals use a map because nodes can appear multiple times.
  private volatile List<Map.Entry<Node, Throwable>> errors;

  public GraphRequestHandler(
      @NonNull GraphStatement<?> graphStatement,
      @NonNull DefaultSession dseSession,
      @NonNull InternalDriverContext context,
      @NonNull String sessionLogPrefix) {
    this.startTimeNanos = System.nanoTime();
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    Preconditions.checkArgument(
        graphStatement instanceof ScriptGraphStatement
            || graphStatement instanceof FluentGraphStatement
            || graphStatement instanceof BatchGraphStatement
            || graphStatement instanceof BytecodeGraphStatement,
        "Unknown graph statement type: " + graphStatement.getClass());

    LOG.trace("[{}] Creating new Graph request handler for request {}", logPrefix, graphStatement);
    this.graphStatement = graphStatement;
    this.session = dseSession;
    this.context = context;

    this.executionProfile =
        GraphConversions.resolveExecutionProfile(this.graphStatement, this.context);
    Boolean statementIsIdempotent = graphStatement.isIdempotent();
    this.isIdempotent =
        (statementIsIdempotent == null)
            ? executionProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE)
            : statementIsIdempotent;
    this.result = new CompletableFuture<>();
    this.result.exceptionally(
        t -> {
          try {
            if (t instanceof CancellationException) {
              cancelScheduledTasks();
            }
          } catch (Throwable t2) {
            Loggers.warnWithException(LOG, "[{}] Uncaught exception", logPrefix, t2);
          }
          return null;
        });

    this.scheduler = context.getNettyOptions().ioEventLoopGroup().next();

    Duration timeout = graphStatement.getTimeout();
    if (timeout == null) {
      timeout = executionProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, null);
    }
    this.timeoutFuture = scheduleTimeout(timeout);

    this.retryPolicy = context.getRetryPolicy(executionProfile.getName());
    this.speculativeExecutionPolicy =
        context.getSpeculativeExecutionPolicy(executionProfile.getName());
    this.activeExecutionsCount = new AtomicInteger(1);
    this.startedSpeculativeExecutionsCount = new AtomicInteger(0);
    this.scheduledExecutions = isIdempotent ? new CopyOnWriteArrayList<>() : null;

    this.inFlightCallbacks = new CopyOnWriteArrayList<>();

    this.subProtocol =
        GraphConversions.inferSubProtocol(this.graphStatement, executionProfile, session);
    this.message =
        GraphConversions.createMessageFromGraphStatement(
            this.graphStatement, subProtocol, executionProfile, context);

    this.throttler = context.getRequestThrottler();
    this.throttler.register(this);
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
    // compute query plan only when the throttling is done.
    // TODO thread safety?
    this.queryPlan =
        context
            .getLoadBalancingPolicyWrapper()
            .newQueryPlan(graphStatement, executionProfile.getName(), session);
    sendRequest(null, 0, 0, true);
  }

  public CompletionStage<AsyncGraphResultSet> handle() {
    return result;
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    session
        .getMetricUpdater()
        .incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, executionProfile.getName());
    setFinalError(error, null);
  }

  private ScheduledFuture<?> scheduleTimeout(Duration timeout) {
    if (timeout != null && timeout.toNanos() > 0) {
      return scheduler.schedule(
          () -> setFinalError(new DriverTimeoutException("Query timed out after " + timeout), null),
          timeout.toNanos(),
          TimeUnit.NANOSECONDS);
    } else {
      return null;
    }
  }

  /**
   * Sends the request to the next available node.
   *
   * @param node if not null, it will be attempted first before the rest of the query plan.
   * @param currentExecutionIndex 0 for the initial execution, 1 for the first speculative one, etc.
   * @param retryCount the number of times that the retry policy was invoked for this execution
   *     already (note that some internal retries don'traversals go through the policy, and
   *     therefore don'traversals increment this counter)
   * @param scheduleNextExecution whether to schedule the next speculative execution
   */
  private void sendRequest(
      Node node, int currentExecutionIndex, int retryCount, boolean scheduleNextExecution) {
    if (result.isDone()) {
      return;
    }
    DriverChannel channel = null;
    if (node == null || (channel = session.getChannel(node, logPrefix)) == null) {
      while (!result.isDone() && (node = queryPlan.poll()) != null) {
        channel = session.getChannel(node, logPrefix);
        if (channel != null) {
          break;
        }
      }
    }
    if (channel == null) {
      // We've reached the end of the query plan without finding any node to write to
      if (!result.isDone() && activeExecutionsCount.decrementAndGet() == 0) {
        // We're the last execution so fail the result
        setFinalError(AllNodesFailedException.fromErrors(this.errors), null);
      }
    } else {
      PerRequestCallback perRequestCallback =
          new PerRequestCallback(
              node, channel, currentExecutionIndex, retryCount, scheduleNextExecution, logPrefix);

      channel
          .write(
              message,
              graphStatement.isTracing(),
              GraphConversions.createCustomPayload(
                  graphStatement, subProtocol, executionProfile, context),
              perRequestCallback)
          .addListener(perRequestCallback);
    }
  }

  private void cancelScheduledTasks() {
    if (this.timeoutFuture != null) {
      this.timeoutFuture.cancel(false);
    }
    if (scheduledExecutions != null) {
      for (ScheduledFuture<?> future : scheduledExecutions) {
        future.cancel(false);
      }
    }
    for (PerRequestCallback callback : inFlightCallbacks) {
      callback.cancel();
    }
  }

  private void setFinalError(Throwable error, Node node) {
    if (result.completeExceptionally(error)) {
      cancelScheduledTasks();
      long latencyNanos = System.nanoTime() - startTimeNanos;
      context
          .getRequestTracker()
          .onError(graphStatement, error, latencyNanos, executionProfile, node, logPrefix);
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
        session
            .getMetricUpdater()
            .incrementCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, executionProfile.getName());
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
  }

  private void recordError(Node node, Throwable error) {
    // Use a local variable to do only a single single volatile read in the nominal case
    List<Map.Entry<Node, Throwable>> errorsSnapshot = this.errors;
    if (errorsSnapshot == null) {
      synchronized (GraphRequestHandler.this) {
        errorsSnapshot = this.errors;
        if (errorsSnapshot == null) {
          this.errors = errorsSnapshot = new CopyOnWriteArrayList<>();
        }
      }
    }
    errorsSnapshot.add(new AbstractMap.SimpleEntry<>(node, error));
  }

  /**
   * Handles the interaction with a single node in the query plan.
   *
   * <p>An instance of this class is created each time we (re)try a node.
   */
  private class PerRequestCallback
      implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>> {
    private final long start = System.nanoTime();
    private final Node node;
    private final DriverChannel channel;
    // The identifier of the current execution (0 for the initial execution, 1 for the first
    // speculative execution, etc.)
    private final int execution;
    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt of each execution).
    private final int retryCount;
    private final boolean scheduleNextExecution;
    private final String logPrefix;

    PerRequestCallback(
        Node node,
        DriverChannel channel,
        int execution,
        int retryCount,
        boolean scheduleNextExecution,
        String logPrefix) {
      this.node = node;
      this.channel = channel;
      this.execution = execution;
      this.retryCount = retryCount;
      this.scheduleNextExecution = scheduleNextExecution;
      this.logPrefix = logPrefix + "|" + execution;
    }

    @Override
    public void onFailure(Throwable error) {
      inFlightCallbacks.remove(this);
      if (result.isDone()) {
        return;
      }
      LOG.trace("[{}] Request failure, processing: {}", logPrefix, error.toString());
      RetryDecision decision;
      if (!isIdempotent || error instanceof FrameTooLongException) {
        decision = RetryDecision.RETHROW;
      } else {
        decision = retryPolicy.onRequestAborted(graphStatement, error, retryCount);
      }
      processRetryDecision(decision, error);
      updateErrorMetrics(
          ((DefaultNode) node).getMetricUpdater(),
          decision,
          DefaultNodeMetric.ABORTED_REQUESTS,
          DefaultNodeMetric.RETRIES_ON_ABORTED,
          DefaultNodeMetric.IGNORES_ON_ABORTED);
    }

    // this gets invoked once the write completes.
    @Override
    public void operationComplete(Future<java.lang.Void> voidFuture) {
      if (!voidFuture.isSuccess()) {
        Throwable error = voidFuture.cause();
        if (error instanceof EncoderException
            && error.getCause() instanceof FrameTooLongException) {
          setFinalError(error.getCause(), node);
        } else {
          LOG.trace(
              "[{}] Failed to send request on {}, trying next node (cause: {})",
              logPrefix,
              channel,
              error);
          recordError(node, error);
          ((DefaultNode) node)
              .getMetricUpdater()
              .incrementCounter(DefaultNodeMetric.UNSENT_REQUESTS, executionProfile.getName());
          sendRequest(null, execution, retryCount, scheduleNextExecution); // try next node
        }
      } else {
        LOG.trace("[{}] Request sent on {}", logPrefix, channel);
        if (result.isDone()) {
          // If the handler completed since the last time we checked, cancel directly because we
          // don'traversals know if cancelScheduledTasks() has run yet
          cancel();
        } else {
          inFlightCallbacks.add(this);
          if (scheduleNextExecution && isIdempotent) {
            int nextExecution = execution + 1;
            // Note that `node` is the first node of the execution, it might not be the "slow" one
            // if there were retries, but in practice retries are rare.
            long nextDelay =
                speculativeExecutionPolicy.nextExecution(node, null, graphStatement, nextExecution);
            if (nextDelay >= 0) {
              LOG.trace(
                  "[{}] Scheduling speculative execution {} in {} ms",
                  logPrefix,
                  nextExecution,
                  nextDelay);
              scheduledExecutions.add(
                  scheduler.schedule(
                      () -> {
                        if (!result.isDone()) {
                          LOG.trace(
                              "[{}] Starting speculative execution {}",
                              GraphRequestHandler.this.logPrefix,
                              nextExecution);
                          activeExecutionsCount.incrementAndGet();
                          startedSpeculativeExecutionsCount.incrementAndGet();
                          ((DefaultNode) node)
                              .getMetricUpdater()
                              .incrementCounter(
                                  DefaultNodeMetric.SPECULATIVE_EXECUTIONS,
                                  executionProfile.getName());
                          sendRequest(null, nextExecution, 0, true);
                        }
                      },
                      nextDelay,
                      TimeUnit.MILLISECONDS));
            } else {
              LOG.trace(
                  "[{}] Speculative execution policy returned {}, no next execution",
                  logPrefix,
                  nextDelay);
            }
          }
        }
      }
    }

    @Override
    public void onResponse(Frame responseFrame) {
      ((DefaultNode) node)
          .getMetricUpdater()
          .updateTimer(
              DefaultNodeMetric.CQL_MESSAGES,
              executionProfile.getName(),
              System.nanoTime() - start,
              TimeUnit.NANOSECONDS);
      inFlightCallbacks.remove(this);
      if (result.isDone()) {
        return;
      }
      try {
        Message responseMessage = responseFrame.message;
        if (responseMessage instanceof Result) {
          LOG.trace("[{}] Got result, completing", logPrefix);
          setFinalResult((Result) responseMessage, responseFrame, this);
        } else if (responseMessage instanceof Error) {
          LOG.trace("[{}] Got error response, processing", logPrefix);
          processErrorResponse((Error) responseMessage);
        } else {
          setFinalError(new IllegalStateException("Unexpected response " + responseMessage), node);
        }
      } catch (Throwable t) {
        setFinalError(t, node);
      }
    }

    private void setFinalResult(
        Result resultMessage, Frame responseFrame, PerRequestCallback callback) {
      try {
        ExecutionInfo executionInfo = buildExecutionInfo(callback, responseFrame);

        Queue<GraphNode> graphNodes = new ArrayDeque<>();
        for (List<ByteBuffer> row : ((Rows) resultMessage).getData()) {
          graphNodes.offer(GraphSONUtils.createGraphNode(row, subProtocol));
        }

        DefaultAsyncGraphResultSet resultSet =
            new DefaultAsyncGraphResultSet(executionInfo, graphNodes);
        if (result.complete(resultSet)) {
          cancelScheduledTasks();
          throttler.signalSuccess(GraphRequestHandler.this);
          long latencyNanos = System.nanoTime() - startTimeNanos;
          context
              .getRequestTracker()
              .onSuccess(graphStatement, latencyNanos, executionProfile, callback.node, logPrefix);
          session
              .getMetricUpdater()
              .updateTimer(
                  DefaultSessionMetric.CQL_REQUESTS,
                  executionProfile.getName(),
                  latencyNanos,
                  TimeUnit.NANOSECONDS);
        }
      } catch (Throwable error) {
        setFinalError(error, callback.node);
      }
    }

    private ExecutionInfo buildExecutionInfo(PerRequestCallback callback, Frame responseFrame) {
      return new DefaultExecutionInfo(
          graphStatement,
          callback.node,
          startedSpeculativeExecutionsCount.get(),
          callback.execution,
          errors,
          null,
          responseFrame,
          true,
          session,
          context,
          executionProfile);
    }

    private void processErrorResponse(Error errorMessage) {
      CoordinatorException error = GraphConversions.toThrowable(node, errorMessage, context);
      NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
      if (error instanceof BootstrappingException) {
        LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
        recordError(node, error);
        sendRequest(null, execution, retryCount, false);
      } else if (error instanceof QueryValidationException
          || error instanceof FunctionFailureException
          || error instanceof ProtocolError) {
        LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
        metricUpdater.incrementCounter(DefaultNodeMetric.OTHER_ERRORS, executionProfile.getName());
        setFinalError(error, node);
      } else {
        RetryDecision decision;
        if (error instanceof ReadTimeoutException) {
          ReadTimeoutException readTimeout = (ReadTimeoutException) error;
          decision =
              retryPolicy.onReadTimeout(
                  graphStatement,
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
          decision =
              isIdempotent
                  ? retryPolicy.onWriteTimeout(
                      graphStatement,
                      writeTimeout.getConsistencyLevel(),
                      writeTimeout.getWriteType(),
                      writeTimeout.getBlockFor(),
                      writeTimeout.getReceived(),
                      retryCount)
                  : RetryDecision.RETHROW;
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
                  graphStatement,
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
                  ? retryPolicy.onErrorResponse(graphStatement, error, retryCount)
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
    }

    private void processRetryDecision(RetryDecision decision, Throwable error) {
      LOG.trace("[{}] Processing retry decision {}", logPrefix, decision);
      switch (decision) {
        case RETRY_SAME:
          recordError(node, error);
          sendRequest(node, execution, retryCount + 1, false);
          break;
        case RETRY_NEXT:
          recordError(node, error);
          sendRequest(null, execution, retryCount + 1, false);
          break;
        case RETHROW:
          setFinalError(error, node);
          break;
        case IGNORE:
          setFinalResult(Void.INSTANCE, null, this);
          break;
      }
    }

    private void updateErrorMetrics(
        NodeMetricUpdater metricUpdater,
        RetryDecision decision,
        DefaultNodeMetric error,
        DefaultNodeMetric retriesOnError,
        DefaultNodeMetric ignoresOnError) {
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

    void cancel() {
      try {
        if (!channel.closeFuture().isDone()) {
          this.channel.cancel(this);
        }
      } catch (Throwable t) {
        Loggers.warnWithException(LOG, "[{}] Error cancelling", logPrefix, t);
      }
    }

    @Override
    public String toString() {
      return logPrefix;
    }
  }
}
