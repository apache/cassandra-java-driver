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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.NodeUnavailableException;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.retry.RetryVerdict;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.DefaultProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the lifecycle of the preparation of a CQL statement. */
@ThreadSafe
public class CqlPrepareHandler implements Throttled {

  private static final Logger LOG = LoggerFactory.getLogger(CqlPrepareHandler.class);

  private final long startTimeNanos;
  private final String logPrefix;
  private final PrepareRequest initialRequest;
  private final DefaultSession session;
  private final InternalDriverContext context;
  private final Queue<Node> queryPlan;
  protected final CompletableFuture<PreparedStatement> result;
  private final Timer timer;
  private final Timeout scheduledTimeout;
  private final RequestTracker requestTracker;
  private final RequestThrottler throttler;
  private final Boolean prepareOnAllNodes;
  private final DriverExecutionProfile executionProfile;
  private volatile InitialPrepareCallback initialCallback;

  // The errors on the nodes that were already tried (lazily initialized on the first error).
  // We don't use a map because nodes can appear multiple times.
  private volatile List<Map.Entry<Node, Throwable>> errors;

  protected CqlPrepareHandler(
      PrepareRequest request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    this.startTimeNanos = System.nanoTime();
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    LOG.trace("[{}] Creating new handler for prepare request {}", logPrefix, request);

    this.initialRequest = request;
    this.session = session;
    this.context = context;
    this.executionProfile = Conversions.resolveExecutionProfile(request, context);
    this.queryPlan =
        context
            .getLoadBalancingPolicyWrapper()
            .newQueryPlan(request, executionProfile.getName(), session);

    this.result = new CompletableFuture<>();
    this.result.exceptionally(
        t -> {
          try {
            if (t instanceof CancellationException) {
              cancelTimeout();
              context.getRequestThrottler().signalCancel(this);
            }
          } catch (Throwable t2) {
            Loggers.warnWithException(LOG, "[{}] Uncaught exception", logPrefix, t2);
          }
          return null;
        });
    this.timer = context.getNettyOptions().getTimer();

    Duration timeout = Conversions.resolveRequestTimeout(request, executionProfile);
    this.scheduledTimeout = scheduleTimeout(timeout);
    this.prepareOnAllNodes = executionProfile.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES);

    this.requestTracker = context.getRequestTracker();
    trackStart();

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
    sendRequest(initialRequest, null, 0);
  }

  public CompletableFuture<PreparedStatement> handle() {
    return result;
  }

  private Timeout scheduleTimeout(Duration timeoutDuration) {
    if (timeoutDuration.toNanos() > 0) {
      return this.timer.newTimeout(
          (Timeout timeout1) -> {
            setFinalError(
                new DriverTimeoutException("Query timed out after " + timeoutDuration), null);
            if (initialCallback != null) {
              initialCallback.cancel();
            }
          },
          timeoutDuration.toNanos(),
          TimeUnit.NANOSECONDS);
    } else {
      return null;
    }
  }

  private void cancelTimeout() {
    if (this.scheduledTimeout != null) {
      this.scheduledTimeout.cancel();
    }
  }

  private void sendRequest(PrepareRequest request, Node node, int retryCount) {
    if (result.isDone()) {
      return;
    }
    DriverChannel channel = null;
    if (node == null || (channel = session.getChannel(node, logPrefix)) == null) {
      while (!result.isDone() && (node = queryPlan.poll()) != null) {
        channel = session.getChannel(node, logPrefix);
        if (channel != null) {
          break;
        } else {
          recordError(node, new NodeUnavailableException(node));
        }
      }
    }
    if (channel == null) {
      setFinalError(AllNodesFailedException.fromErrors(this.errors), node);
    } else {
      InitialPrepareCallback initialPrepareCallback =
          new InitialPrepareCallback(request, node, channel, retryCount);
      Prepare message = toPrepareMessage(request);
      trackNodeStart(request, node);
      channel
          .write(message, false, request.getCustomPayload(), initialPrepareCallback)
          .addListener(initialPrepareCallback);
    }
  }

  @NonNull
  private Prepare toPrepareMessage(PrepareRequest request) {
    ProtocolVersion protocolVersion = context.getProtocolVersion();
    ProtocolVersionRegistry registry = context.getProtocolVersionRegistry();
    CqlIdentifier keyspace = request.getKeyspace();
    if (keyspace != null
        && !registry.supports(protocolVersion, DefaultProtocolFeature.PER_REQUEST_KEYSPACE)) {
      throw new IllegalArgumentException(
          "Can't use per-request keyspace with protocol " + protocolVersion);
    }
    return new Prepare(request.getQuery(), (keyspace == null) ? null : keyspace.asInternal());
  }

  private void recordError(Node node, Throwable error) {
    // Use a local variable to do only a single volatile read in the nominal case
    List<Map.Entry<Node, Throwable>> errorsSnapshot = this.errors;
    if (errorsSnapshot == null) {
      synchronized (CqlPrepareHandler.this) {
        errorsSnapshot = this.errors;
        if (errorsSnapshot == null) {
          this.errors = errorsSnapshot = new CopyOnWriteArrayList<>();
        }
      }
    }
    errorsSnapshot.add(new AbstractMap.SimpleEntry<>(node, error));
  }

  private void setFinalResult(
      PrepareRequest request, Prepared response, InitialPrepareCallback callback) {

    // Whatever happens below, we're done with this stream id
    throttler.signalSuccess(this);

    DefaultPreparedStatement preparedStatement =
        Conversions.toPreparedStatement(response, request, context);

    trackNodeEnd(request, callback.node, null, callback.nodeStartTimeNanos);

    session
        .getRepreparePayloads()
        .put(preparedStatement.getId(), preparedStatement.getRepreparePayload());
    if (prepareOnAllNodes) {
      prepareOnOtherNodes(request)
          .thenRun(
              () -> {
                LOG.trace(
                    "[{}] Done repreparing on other nodes, completing the request", logPrefix);
                result.complete(preparedStatement);
                trackEnd(callback.node, null);
              })
          .exceptionally(
              error -> {
                result.completeExceptionally(error);
                trackEnd(callback.node, error);
                return null;
              });
    } else {
      LOG.trace("[{}] Prepare on all nodes is disabled, completing the request", logPrefix);
      result.complete(preparedStatement);
      trackEnd(callback.node, null);
    }
  }

  private CompletionStage<Void> prepareOnOtherNodes(PrepareRequest request) {
    List<CompletionStage<Void>> otherNodesFutures = new ArrayList<>();
    // Only process the rest of the query plan. Any node before that is either the coordinator, or
    // a node that failed (we assume that retrying right now has little chance of success).
    for (Node node : queryPlan) {
      otherNodesFutures.add(prepareOnOtherNode(request, node));
    }
    return CompletableFutures.allDone(otherNodesFutures);
  }

  // Try to reprepare on another node, after the initial query has succeeded. Errors are not
  // blocking, the preparation will be retried later on that node. Simply warn and move on.
  private CompletionStage<Void> prepareOnOtherNode(PrepareRequest request, Node node) {
    LOG.trace("[{}] Repreparing on {}", logPrefix, node);
    DriverChannel channel = session.getChannel(node, logPrefix);
    if (channel == null) {
      LOG.trace("[{}] Could not get a channel to reprepare on {}, skipping", logPrefix, node);
      return CompletableFuture.completedFuture(null);
    } else {
      ThrottledAdminRequestHandler<ByteBuffer> handler =
          ThrottledAdminRequestHandler.prepare(
              channel,
              false,
              toPrepareMessage(request),
              request.getCustomPayload(),
              Conversions.resolveRequestTimeout(request, executionProfile),
              throttler,
              session.getMetricUpdater(),
              logPrefix);
      long nodeStartTimeNanos = System.nanoTime();
      trackNodeStart(request, node);
      return handler
          .start()
          .handle(
              (result, error) -> {
                if (error == null) {
                  LOG.trace("[{}] Successfully reprepared on {}", logPrefix, node);
                  trackNodeEnd(request, node, null, nodeStartTimeNanos);
                } else {
                  Loggers.warnWithException(
                      LOG, "[{}] Error while repreparing on {}", node, logPrefix, error);
                  trackNodeEnd(request, node, error, nodeStartTimeNanos);
                }
                return null;
              });
    }
  }

  @Override
  public void onThrottleFailure(@NonNull RequestThrottlingException error) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(initialRequest, context);
    session
        .getMetricUpdater()
        .incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, executionProfile.getName());
    setFinalError(error, null);
  }

  private void setFinalError(Throwable error, Node node) {
    if (result.completeExceptionally(error)) {
      cancelTimeout();
      trackEnd(node, error);
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
  }

  private class InitialPrepareCallback
      implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>> {
    private final long nodeStartTimeNanos = System.nanoTime();
    private final PrepareRequest request;
    private final Node node;
    private final DriverChannel channel;
    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt of each execution).
    private final int retryCount;

    private InitialPrepareCallback(
        PrepareRequest request, Node node, DriverChannel channel, int retryCount) {
      this.request = request;
      this.node = node;
      this.channel = channel;
      this.retryCount = retryCount;
    }

    // this gets invoked once the write completes.
    @Override
    public void operationComplete(Future<java.lang.Void> future) {
      if (!future.isSuccess()) {
        LOG.trace(
            "[{}] Failed to send request on {}, trying next node (cause: {})",
            logPrefix,
            node,
            future.cause().toString());
        recordError(node, future.cause());
        trackNodeEnd(request, node, future.cause(), nodeStartTimeNanos);
        sendRequest(request, null, retryCount); // try next host
      } else {
        if (result.isDone()) {
          // Might happen if the timeout just fired
          cancel();
        } else {
          LOG.trace("[{}] Request sent to {}", logPrefix, node);
          initialCallback = this;
        }
      }
    }

    @Override
    public void onResponse(Frame responseFrame) {
      if (result.isDone()) {
        return;
      }
      try {
        Message responseMessage = responseFrame.message;
        if (responseMessage instanceof Prepared) {
          LOG.trace("[{}] Got result, completing", logPrefix);
          setFinalResult(request, (Prepared) responseMessage, this);
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
      if (errorMessage.code == ProtocolConstants.ErrorCode.UNPREPARED
          || errorMessage.code == ProtocolConstants.ErrorCode.ALREADY_EXISTS
          || errorMessage.code == ProtocolConstants.ErrorCode.READ_FAILURE
          || errorMessage.code == ProtocolConstants.ErrorCode.READ_TIMEOUT
          || errorMessage.code == ProtocolConstants.ErrorCode.WRITE_FAILURE
          || errorMessage.code == ProtocolConstants.ErrorCode.WRITE_TIMEOUT
          || errorMessage.code == ProtocolConstants.ErrorCode.UNAVAILABLE
          || errorMessage.code == ProtocolConstants.ErrorCode.TRUNCATE_ERROR) {
        setFinalError(
            new IllegalStateException("Unexpected server error for a PREPARE query" + errorMessage),
            node);
        return;
      }
      CoordinatorException error = Conversions.toThrowable(node, errorMessage, context);
      if (error instanceof BootstrappingException) {
        LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
        recordError(node, error);
        trackNodeEnd(request, node, error, nodeStartTimeNanos);
        sendRequest(request, null, retryCount);
      } else if (error instanceof QueryValidationException
          || error instanceof FunctionFailureException
          || error instanceof ProtocolError) {
        LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
        setFinalError(error, node);
      } else {
        // Because prepare requests are known to always be idempotent, we call the retry policy
        // directly, without checking the flag.
        RetryPolicy retryPolicy = Conversions.resolveRetryPolicy(context, executionProfile);
        RetryVerdict verdict = retryPolicy.onErrorResponseVerdict(request, error, retryCount);
        processRetryVerdict(verdict, error);
      }
    }

    private void processRetryVerdict(RetryVerdict verdict, Throwable error) {
      RetryDecision decision = verdict.getRetryDecision();
      LOG.trace("[{}] Processing retry decision {}", logPrefix, decision);
      switch (decision) {
        case RETRY_SAME:
          recordError(node, error);
          trackNodeEnd(request, node, error, nodeStartTimeNanos);
          sendRequest(verdict.getRetryRequest(request), node, retryCount + 1);
          break;
        case RETRY_NEXT:
          recordError(node, error);
          trackNodeEnd(request, node, error, nodeStartTimeNanos);
          sendRequest(verdict.getRetryRequest(request), null, retryCount + 1);
          break;
        case RETHROW:
          trackNodeEnd(request, node, error, nodeStartTimeNanos);
          setFinalError(error, node);
          break;
        case IGNORE:
          setFinalError(
              new IllegalArgumentException(
                  "IGNORE decisions are not allowed for prepare requests, "
                      + "please fix your retry policy."),
              node);
          break;
      }
    }

    @Override
    public void onFailure(Throwable error) {
      if (result.isDone()) {
        return;
      }
      LOG.trace("[{}] Request failure, processing: {}", logPrefix, error.toString());
      RetryVerdict verdict;
      try {
        RetryPolicy retryPolicy = Conversions.resolveRetryPolicy(context, executionProfile);
        verdict = retryPolicy.onRequestAbortedVerdict(request, error, retryCount);
      } catch (Throwable cause) {
        setFinalError(
            new IllegalStateException("Unexpected error while invoking the retry policy", cause),
            node);
        return;
      }
      processRetryVerdict(verdict, error);
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

  /** Notify request tracker that processing of initial statement starts. */
  private void trackStart() {
    if (requestTracker instanceof NoopRequestTracker) {
      return;
    }
    requestTracker.onRequestCreated(initialRequest, executionProfile, logPrefix);
  }

  /**
   * Notify request tracker that processing of statement starts at a given node. Statement is passed
   * as a separate parameter, because it might have been changed by custom retry policy.
   */
  private void trackNodeStart(Request request, Node node) {
    if (requestTracker instanceof NoopRequestTracker) {
      return;
    }
    requestTracker.onRequestCreatedForNode(request, executionProfile, node, logPrefix);
  }

  /** Notify request tracker that processing of statement has been completed by a given node. */
  private void trackNodeEnd(Request request, Node node, Throwable error, long startTimeNanos) {
    if (requestTracker instanceof NoopRequestTracker) {
      return;
    }
    long latencyNanos = System.nanoTime() - startTimeNanos;
    ExecutionInfo executionInfo = defaultExecutionInfo(request, node, error).build();
    if (error == null) {
      requestTracker.onNodeSuccess(latencyNanos, executionInfo, logPrefix);
    } else {
      requestTracker.onNodeError(latencyNanos, executionInfo, logPrefix);
    }
  }

  /**
   * Notify request tracker that processing of initial statement has been completed (successfully or
   * with error).
   */
  private void trackEnd(Node node, Throwable error) {
    if (requestTracker instanceof NoopRequestTracker) {
      return;
    }
    long latencyNanos = System.nanoTime() - this.startTimeNanos;
    ExecutionInfo executionInfo = defaultExecutionInfo(initialRequest, node, error).build();
    if (error == null) {
      requestTracker.onSuccess(latencyNanos, executionInfo, logPrefix);
    } else {
      requestTracker.onError(latencyNanos, executionInfo, logPrefix);
    }
  }

  private DefaultExecutionInfo.Builder defaultExecutionInfo(
      Request statement, Node node, Throwable error) {
    return new DefaultExecutionInfo.Builder(
        statement, node, -1, 0, error, null, session, context, executionProfile);
  }
}
