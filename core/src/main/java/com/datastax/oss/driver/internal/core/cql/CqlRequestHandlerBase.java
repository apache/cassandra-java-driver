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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
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
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.UnexpectedResponseException;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.SchemaChange;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.handler.codec.EncoderException;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ScheduledFuture;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public abstract class CqlRequestHandlerBase implements Throttled {

  private static final Logger LOG = LoggerFactory.getLogger(CqlRequestHandlerBase.class);

  private final long startTimeNanos;
  private final String logPrefix;
  private final Statement<?> statement;
  private final DefaultSession session;
  private final CqlIdentifier keyspace;
  private final InternalDriverContext context;
  private final Queue<Node> queryPlan;
  private final DriverConfigProfile configProfile;
  private final boolean isIdempotent;
  protected final CompletableFuture<AsyncResultSet> result;
  private final Message message;
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

  private final Duration timeout;
  private final ScheduledFuture<?> timeoutFuture;
  private final List<ScheduledFuture<?>> scheduledExecutions;
  private final List<NodeResponseCallback> inFlightCallbacks;
  private final RetryPolicy retryPolicy;
  private final SpeculativeExecutionPolicy speculativeExecutionPolicy;
  private final RequestThrottler throttler;

  // The errors on the nodes that were already tried (lazily initialized on the first error).
  // We don't use a map because nodes can appear multiple times.
  private volatile List<Map.Entry<Node, Throwable>> errors;

  protected CqlRequestHandlerBase(
      Statement<?> statement,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    this.startTimeNanos = System.nanoTime();
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    LOG.trace("[{}] Creating new handler for request {}", logPrefix, statement);

    this.statement = statement;
    this.session = session;
    this.keyspace = session.getKeyspace();
    this.context = context;

    if (statement.getConfigProfile() != null) {
      this.configProfile = statement.getConfigProfile();
    } else {
      DriverConfig config = context.config();
      String profileName = statement.getConfigProfileName();
      this.configProfile =
          (profileName == null || profileName.isEmpty())
              ? config.getDefaultProfile()
              : config.getProfile(profileName);
    }
    this.queryPlan =
        context
            .loadBalancingPolicyWrapper()
            .newQueryPlan(statement, configProfile.getName(), session);
    this.retryPolicy = context.retryPolicy(configProfile.getName());
    this.speculativeExecutionPolicy = context.speculativeExecutionPolicy(configProfile.getName());
    this.isIdempotent =
        (statement.isIdempotent() == null)
            ? configProfile.getBoolean(DefaultDriverOption.REQUEST_DEFAULT_IDEMPOTENCE)
            : statement.isIdempotent();
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
    this.message = Conversions.toMessage(statement, configProfile, context);
    this.scheduler = context.nettyOptions().ioEventLoopGroup().next();

    this.timeout = configProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT);
    this.timeoutFuture = scheduleTimeout(timeout);

    this.activeExecutionsCount = new AtomicInteger(1);
    this.startedSpeculativeExecutionsCount = new AtomicInteger(0);
    this.scheduledExecutions = isIdempotent ? new CopyOnWriteArrayList<>() : null;
    this.inFlightCallbacks = new CopyOnWriteArrayList<>();

    this.throttler = context.requestThrottler();
    this.throttler.register(this);
  }

  @Override
  public void onThrottleReady(boolean wasDelayed) {
    if (wasDelayed) {
      session
          .getMetricUpdater()
          .updateTimer(
              DefaultSessionMetric.THROTTLING_DELAY,
              configProfile.getName(),
              System.nanoTime() - startTimeNanos,
              TimeUnit.NANOSECONDS);
    }
    sendRequest(null, 0, 0, true);
  }

  private ScheduledFuture<?> scheduleTimeout(Duration timeout) {
    if (timeout.toNanos() > 0) {
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
   *     already (note that some internal retries don't go through the policy, and therefore don't
   *     increment this counter)
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
      NodeResponseCallback nodeResponseCallback =
          new NodeResponseCallback(
              node, channel, currentExecutionIndex, retryCount, scheduleNextExecution, logPrefix);
      channel
          .write(message, statement.isTracing(), statement.getCustomPayload(), nodeResponseCallback)
          .addListener(nodeResponseCallback);
    }
  }

  private void recordError(Node node, Throwable error) {
    // Use a local variable to do only a single single volatile read in the nominal case
    List<Map.Entry<Node, Throwable>> errorsSnapshot = this.errors;
    if (errorsSnapshot == null) {
      synchronized (CqlRequestHandlerBase.this) {
        errorsSnapshot = this.errors;
        if (errorsSnapshot == null) {
          this.errors = errorsSnapshot = new CopyOnWriteArrayList<>();
        }
      }
    }
    errorsSnapshot.add(new AbstractMap.SimpleEntry<>(node, error));
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
    for (NodeResponseCallback callback : inFlightCallbacks) {
      callback.cancel();
    }
  }

  private void setFinalResult(
      Result resultMessage,
      Frame responseFrame,
      boolean schemaInAgreement,
      NodeResponseCallback callback) {
    try {
      ExecutionInfo executionInfo =
          buildExecutionInfo(callback, resultMessage, responseFrame, schemaInAgreement);
      AsyncResultSet resultSet =
          Conversions.toResultSet(resultMessage, executionInfo, session, context);
      if (result.complete(resultSet)) {
        cancelScheduledTasks();
        throttler.signalSuccess(this);
        long latencyNanos = System.nanoTime() - startTimeNanos;
        context.requestTracker().onSuccess(statement, latencyNanos, configProfile, callback.node);
        session
            .getMetricUpdater()
            .updateTimer(
                DefaultSessionMetric.CQL_REQUESTS,
                configProfile.getName(),
                latencyNanos,
                TimeUnit.NANOSECONDS);
      }
    } catch (Throwable error) {
      setFinalError(error, callback.node);
    }
  }

  private ExecutionInfo buildExecutionInfo(
      NodeResponseCallback callback,
      Result resultMessage,
      Frame responseFrame,
      boolean schemaInAgreement) {
    ByteBuffer pagingState =
        (resultMessage instanceof Rows) ? ((Rows) resultMessage).getMetadata().pagingState : null;
    return new DefaultExecutionInfo(
        statement,
        callback.node,
        startedSpeculativeExecutionsCount.get(),
        callback.execution,
        errors,
        pagingState,
        responseFrame,
        schemaInAgreement,
        session,
        context,
        configProfile);
  }

  @Override
  public void onThrottleFailure(RequestThrottlingException error) {
    session
        .getMetricUpdater()
        .incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, configProfile.getName());
    setFinalError(error, null);
  }

  private void setFinalError(Throwable error, Node node) {
    if (result.completeExceptionally(error)) {
      cancelScheduledTasks();
      long latencyNanos = System.nanoTime() - startTimeNanos;
      context.requestTracker().onError(statement, error, latencyNanos, configProfile, node);
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
        session
            .getMetricUpdater()
            .incrementCounter(DefaultSessionMetric.CQL_CLIENT_TIMEOUTS, configProfile.getName());
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
  }

  /**
   * Handles the interaction with a single node in the query plan.
   *
   * <p>An instance of this class is created each time we (re)try a node.
   */
  private class NodeResponseCallback
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

    private NodeResponseCallback(
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

    // this gets invoked once the write completes.
    @Override
    public void operationComplete(Future<java.lang.Void> future) throws Exception {
      if (!future.isSuccess()) {
        Throwable error = future.cause();
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
              .incrementCounter(DefaultNodeMetric.UNSENT_REQUESTS, configProfile.getName());
          sendRequest(null, execution, retryCount, scheduleNextExecution); // try next node
        }
      } else {
        LOG.trace("[{}] Request sent on {}", logPrefix, channel);
        if (result.isDone()) {
          // If the handler completed since the last time we checked, cancel directly because we
          // don't know if cancelScheduledTasks() has run yet
          cancel();
        } else {
          inFlightCallbacks.add(this);
          if (scheduleNextExecution && isIdempotent) {
            int nextExecution = execution + 1;
            // Note that `node` is the first node of the execution, it might not be the "slow" one
            // if there were retries, but in practice retries are rare.
            long nextDelay =
                speculativeExecutionPolicy.nextExecution(node, keyspace, statement, nextExecution);
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
                              CqlRequestHandlerBase.this.logPrefix,
                              nextExecution);
                          activeExecutionsCount.incrementAndGet();
                          startedSpeculativeExecutionsCount.incrementAndGet();
                          ((DefaultNode) node)
                              .getMetricUpdater()
                              .incrementCounter(
                                  DefaultNodeMetric.SPECULATIVE_EXECUTIONS,
                                  configProfile.getName());
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
              configProfile.getName(),
              System.nanoTime() - start,
              TimeUnit.NANOSECONDS);
      inFlightCallbacks.remove(this);
      if (result.isDone()) {
        return;
      }
      try {
        Message responseMessage = responseFrame.message;
        if (responseMessage instanceof SchemaChange) {
          SchemaChange schemaChange = (SchemaChange) responseMessage;
          context
              .topologyMonitor()
              .checkSchemaAgreement()
              .thenCombine(
                  context.metadataManager().refreshSchema(schemaChange.keyspace, false, false),
                  (schemaInAgreement, metadata) -> schemaInAgreement)
              .whenComplete(
                  ((schemaInAgreement, error) ->
                      setFinalResult(schemaChange, responseFrame, schemaInAgreement, this)));
        } else if (responseMessage instanceof SetKeyspace) {
          SetKeyspace setKeyspace = (SetKeyspace) responseMessage;
          session
              .setKeyspace(CqlIdentifier.fromInternal(setKeyspace.keyspace))
              .whenComplete((v, error) -> setFinalResult(setKeyspace, responseFrame, true, this));
        } else if (responseMessage instanceof Result) {
          LOG.trace("[{}] Got result, completing", logPrefix);
          setFinalResult((Result) responseMessage, responseFrame, true, this);
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

    private void processErrorResponse(Error errorMessage) {
      if (errorMessage.code == ProtocolConstants.ErrorCode.UNPREPARED) {
        LOG.trace("[{}] Statement is not prepared on {}, repreparing", logPrefix, node);
        ByteBuffer id = ByteBuffer.wrap(((Unprepared) errorMessage).id);
        RepreparePayload repreparePayload = session.getRepreparePayloads().get(id);
        if (repreparePayload == null) {
          throw new IllegalStateException(
              String.format(
                  "Tried to execute unprepared query %s but we don't have the data to reprepare it",
                  Bytes.toHexString(id)));
        }
        Prepare reprepareMessage = new Prepare(repreparePayload.query);
        ThrottledAdminRequestHandler reprepareHandler =
            new ThrottledAdminRequestHandler(
                channel,
                reprepareMessage,
                repreparePayload.customPayload,
                timeout,
                throttler,
                session.getMetricUpdater(),
                logPrefix,
                "Reprepare " + reprepareMessage.toString());
        reprepareHandler
            .start()
            .handle(
                (result, exception) -> {
                  if (exception != null) {
                    // If the error is not recoverable, surface it to the client instead of retrying
                    if (exception instanceof UnexpectedResponseException) {
                      Message prepareErrorMessage =
                          ((UnexpectedResponseException) exception).message;
                      if (prepareErrorMessage instanceof Error) {
                        CoordinatorException prepareError =
                            Conversions.toThrowable(node, (Error) prepareErrorMessage, context);
                        if (prepareError instanceof QueryValidationException
                            || prepareError instanceof FunctionFailureException
                            || prepareError instanceof ProtocolError) {
                          LOG.trace("[{}] Unrecoverable error on reprepare, rethrowing", logPrefix);
                          setFinalError(prepareError, node);
                          return null;
                        }
                      }
                    } else if (exception instanceof RequestThrottlingException) {
                      setFinalError(exception, node);
                      return null;
                    }
                    recordError(node, exception);
                    LOG.trace("[{}] Reprepare failed, trying next node", logPrefix);
                    sendRequest(null, execution, retryCount, false);
                  } else {
                    LOG.trace("[{}] Reprepare sucessful, retrying", logPrefix);
                    sendRequest(node, execution, retryCount, false);
                  }
                  return null;
                });
        return;
      }
      CoordinatorException error = Conversions.toThrowable(node, errorMessage, context);
      NodeMetricUpdater metricUpdater = ((DefaultNode) node).getMetricUpdater();
      if (error instanceof BootstrappingException) {
        LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
        recordError(node, error);
        sendRequest(null, execution, retryCount, false);
      } else if (error instanceof QueryValidationException
          || error instanceof FunctionFailureException
          || error instanceof ProtocolError) {
        LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
        metricUpdater.incrementCounter(DefaultNodeMetric.OTHER_ERRORS, configProfile.getName());
        setFinalError(error, node);
      } else {
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
          decision =
              isIdempotent
                  ? retryPolicy.onWriteTimeout(
                      statement,
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
          setFinalResult(Void.INSTANCE, null, true, this);
          break;
      }
    }

    private void updateErrorMetrics(
        NodeMetricUpdater metricUpdater,
        RetryDecision decision,
        DefaultNodeMetric error,
        DefaultNodeMetric retriesOnError,
        DefaultNodeMetric ignoresOnError) {
      metricUpdater.incrementCounter(error, configProfile.getName());
      switch (decision) {
        case RETRY_SAME:
        case RETRY_NEXT:
          metricUpdater.incrementCounter(DefaultNodeMetric.RETRIES, configProfile.getName());
          metricUpdater.incrementCounter(retriesOnError, configProfile.getName());
          break;
        case IGNORE:
          metricUpdater.incrementCounter(DefaultNodeMetric.IGNORES, configProfile.getName());
          metricUpdater.incrementCounter(ignoresOnError, configProfile.getName());
          break;
        case RETHROW:
          // nothing do do
      }
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
        decision = retryPolicy.onRequestAborted(statement, error, retryCount);
      }
      processRetryDecision(decision, error);
      updateErrorMetrics(
          ((DefaultNode) node).getMetricUpdater(),
          decision,
          DefaultNodeMetric.ABORTED_REQUESTS,
          DefaultNodeMetric.RETRIES_ON_ABORTED,
          DefaultNodeMetric.IGNORES_ON_ABORTED);
    }

    public void cancel() {
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
