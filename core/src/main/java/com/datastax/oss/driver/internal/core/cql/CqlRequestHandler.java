/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
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
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RepreparePayload;
import com.datastax.oss.driver.internal.core.session.RequestHandlerBase;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
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
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ScheduledFuture;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles execution of a {@link Statement}. */
public class CqlRequestHandler
    extends RequestHandlerBase<ResultSet, CompletionStage<AsyncResultSet>> {

  private static final Logger LOG = LoggerFactory.getLogger(CqlRequestHandler.class);

  private final String logPrefix;
  private final CompletableFuture<AsyncResultSet> result;
  private final Message message;
  private final EventExecutor scheduler;
  // How many speculative executions are currently running (not counting the initial execution).
  // All executions share the same query plan, they stop either when the request completes or the
  // query plan is empty.
  private final AtomicInteger executions;
  private final Duration timeout;
  private final ScheduledFuture<?> timeoutFuture;
  private final List<ScheduledFuture<?>> pendingExecutions;
  private final List<NodeResponseCallback> inFlightCallbacks;
  private final RetryPolicy retryPolicy;
  private final SpeculativeExecutionPolicy speculativeExecutionPolicy;

  // The errors on the nodes that were already tried (lazily initialized on the first error).
  // We don't use a map because nodes can appear multiple times.
  private volatile List<Map.Entry<Node, Throwable>> errors;

  CqlRequestHandler(
      Statement statement,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    super(statement, session, context);
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    LOG.debug("[{}] Creating new handler for request {}", logPrefix, statement);
    this.result = new CompletableFuture<>();
    this.result.exceptionally(
        t -> {
          try {
            if (t instanceof CancellationException) {
              cancelScheduledTasks();
            }
          } catch (Throwable t2) {
            LOG.warn("[{}] Uncaught exception", logPrefix, t2);
          }
          return null;
        });
    this.message = Conversions.toMessage(statement, configProfile, context);
    this.scheduler = context.nettyOptions().ioEventLoopGroup().next();

    this.timeout = configProfile.getDuration(CoreDriverOption.REQUEST_TIMEOUT);
    this.timeoutFuture = scheduleTimeout(timeout);

    this.retryPolicy = context.retryPolicy();
    this.speculativeExecutionPolicy = context.speculativeExecutionPolicy();
    this.executions = new AtomicInteger(0);

    if (isIdempotent) {
      // Start the initial execution
      long nextExecution = context.speculativeExecutionPolicy().nextExecution(keyspace, request, 1);
      if (nextExecution > 0) {
        LOG.debug("[{}] Scheduling first speculative execution in {} ms", logPrefix, nextExecution);
        this.pendingExecutions = new CopyOnWriteArrayList<>();
        this.pendingExecutions.add(
            scheduler.schedule(this::startExecution, nextExecution, TimeUnit.MILLISECONDS));
      } else {
        LOG.debug(
            "[{}] Speculative execution policy returned {}, no next execution",
            logPrefix,
            nextExecution);
        this.pendingExecutions = null; // we'll never need this so avoid allocation
      }
    } else {
      LOG.debug("[{}] Request is not idempotent, no speculative executions", logPrefix);
      this.pendingExecutions = null;
    }
    this.inFlightCallbacks = new CopyOnWriteArrayList<>();
    sendRequest(null, 0, 0);
  }

  @Override
  public CompletionStage<AsyncResultSet> asyncResult() {
    return result;
  }

  @Override
  public ResultSet syncResult() {
    BlockingOperation.checkNotDriverThread();
    AsyncResultSet firstPage = CompletableFutures.getUninterruptibly(asyncResult());
    return ResultSets.newInstance(firstPage);
  }

  private ScheduledFuture<?> scheduleTimeout(Duration timeout) {
    if (timeout.toNanos() > 0) {
      return scheduler.schedule(
          () -> setFinalError(new DriverTimeoutException("Query timed out after " + timeout)),
          timeout.toNanos(),
          TimeUnit.NANOSECONDS);
    } else {
      return null;
    }
  }

  private void startExecution() {
    if (!result.isDone()) {
      int execution = executions.incrementAndGet();
      LOG.trace("[{}] Starting speculative execution {}", logPrefix, execution);
      long nextDelay = speculativeExecutionPolicy.nextExecution(keyspace, request, execution + 1);
      if (nextDelay > 0) {
        LOG.trace(
            "[{}] Scheduling {}th speculative execution in {} ms",
            logPrefix,
            execution + 1,
            nextDelay);
        this.pendingExecutions.add(
            scheduler.schedule(this::startExecution, nextDelay, TimeUnit.MILLISECONDS));
      } else {
        LOG.trace(
            "[{}] Speculative execution policy returned {}, no next execution",
            logPrefix,
            nextDelay);
      }
      sendRequest(null, execution, 0);
    }
  }

  /**
   * Sends the request to the next available node.
   *
   * @param node if not null, it will be attempted first before the rest of the query plan.
   */
  private void sendRequest(Node node, int execution, int retryCount) {
    if (result.isDone()) {
      return;
    }
    DriverChannel channel = null;
    if (node == null || (channel = getChannel(node, logPrefix)) == null) {
      while (!result.isDone() && (node = queryPlan.poll()) != null) {
        channel = getChannel(node, logPrefix);
        if (channel != null) {
          break;
        }
      }
    }
    if (channel == null) {
      // We've reached the end of the query plan without finding any node to write to
      if (!result.isDone() && executions.decrementAndGet() == -1) {
        // We're the last execution so fail the result
        setFinalError(AllNodesFailedException.fromErrors(this.errors));
      }
    } else {
      NodeResponseCallback nodeResponseCallback =
          new NodeResponseCallback(node, channel, execution, retryCount, logPrefix);
      channel
          .write(message, request.isTracing(), request.getCustomPayload(), nodeResponseCallback)
          .addListener(nodeResponseCallback);
    }
  }

  private void recordError(Node node, Throwable error) {
    // Use a local variable to do only a single single volatile read in the nominal case
    List<Map.Entry<Node, Throwable>> errorsSnapshot = this.errors;
    if (errorsSnapshot == null) {
      synchronized (CqlRequestHandler.this) {
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
    List<ScheduledFuture<?>> pendingExecutionsSnapshot = this.pendingExecutions;
    if (pendingExecutionsSnapshot != null) {
      for (ScheduledFuture<?> future : pendingExecutionsSnapshot) {
        future.cancel(false);
      }
    }
    for (NodeResponseCallback callback : inFlightCallbacks) {
      callback.cancel();
    }
  }

  private void setFinalResult(
      Result resultMessage, Frame responseFrame, NodeResponseCallback callback) {
    try {
      ExecutionInfo executionInfo = buildExecutionInfo(callback, resultMessage, responseFrame);
      AsyncResultSet resultSet =
          Conversions.toResultSet(resultMessage, executionInfo, session, context);
      if (result.complete(resultSet)) {
        cancelScheduledTasks();
        if (resultMessage instanceof SetKeyspace) {
          CqlIdentifier newKeyspace =
              CqlIdentifier.fromInternal(((SetKeyspace) resultMessage).keyspace);
          session.setKeyspace(newKeyspace);
        }
      }
    } catch (Throwable error) {
      setFinalError(error);
    }
  }

  private ExecutionInfo buildExecutionInfo(
      NodeResponseCallback callback, Result resultMessage, Frame responseFrame) {
    ByteBuffer pagingState =
        (resultMessage instanceof Rows) ? ((Rows) resultMessage).getMetadata().pagingState : null;
    return new DefaultExecutionInfo(
        (Statement) request,
        callback.node,
        callback.execution,
        executions.get(),
        errors,
        pagingState,
        responseFrame);
  }

  private void setFinalError(Throwable error) {
    if (result.completeExceptionally(error)) {
      cancelScheduledTasks();
    }
  }

  /**
   * Handles the interaction with a single node in the query plan.
   *
   * <p>An instance of this class is created each time we (re)try a node.
   */
  private class NodeResponseCallback
      implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>> {

    private final Node node;
    private final DriverChannel channel;
    // The identifier of the current execution (0 for the initial execution, 1 for the first
    // speculative execution, etc.)
    private final int execution;
    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt of each execution).
    private final int retryCount;
    private final String logPrefix;

    private NodeResponseCallback(
        Node node, DriverChannel channel, int execution, int retryCount, String logPrefix) {
      this.node = node;
      this.channel = channel;
      this.execution = execution;
      this.retryCount = retryCount;
      this.logPrefix = logPrefix + "|" + execution;
    }

    // this gets invoked once the write completes.
    @Override
    public void operationComplete(Future<java.lang.Void> future) throws Exception {
      if (!future.isSuccess()) {
        LOG.debug(
            "[{}] Failed to send request on {}, trying next node (cause: {})",
            logPrefix,
            channel,
            future.cause());
        recordError(node, future.cause());
        sendRequest(null, execution, retryCount); // try next node
      } else {
        LOG.debug("[{}] Request sent on {}", logPrefix, channel);
        if (result.isDone()) {
          // If the handler completed since the last time we checked, cancel directly because we
          // don't know if cancelScheduledTasks() has run yet
          cancel();
        } else {
          inFlightCallbacks.add(this);
        }
      }
    }

    @Override
    public void onResponse(Frame responseFrame) {
      inFlightCallbacks.remove(this);
      if (result.isDone()) {
        return;
      }
      try {
        Message responseMessage = responseFrame.message;
        if (responseMessage instanceof SchemaChange) {
          // TODO schema agreement, and chain setFinalResult to the result
          setFinalResult((Result) responseMessage, responseFrame, this);
        } else if (responseMessage instanceof Result) {
          LOG.debug("[{}] Got result, completing", logPrefix);
          setFinalResult((Result) responseMessage, responseFrame, this);
        } else if (responseMessage instanceof Error) {
          LOG.debug("[{}] Got error response, processing", logPrefix);
          processErrorResponse((Error) responseMessage);
        } else {
          setFinalError(new IllegalStateException("Unexpected response " + responseMessage));
        }
      } catch (Throwable t) {
        setFinalError(t);
      }
    }

    private void processErrorResponse(Error errorMessage) {
      if (errorMessage.code == ProtocolConstants.ErrorCode.UNPREPARED) {
        LOG.debug("[{}] Statement is not prepared on {}, repreparing", logPrefix, node);
        ByteBuffer id = ByteBuffer.wrap(((Unprepared) errorMessage).id);
        RepreparePayload repreparePayload = session.getRepreparePayloads().get(id);
        if (repreparePayload == null) {
          throw new IllegalStateException(
              String.format(
                  "Tried to execute unprepared query %s but we don't have the data to reprepare it",
                  Bytes.toHexString(id)));
        }
        Prepare reprepareMessage = new Prepare(repreparePayload.query);
        AdminRequestHandler reprepareHandler =
            new AdminRequestHandler(
                channel,
                reprepareMessage,
                timeout,
                logPrefix,
                "Reprepare " + reprepareMessage.toString());
        reprepareHandler
            .start(repreparePayload.customPayload)
            .handle(
                (result, error) -> {
                  if (error != null) {
                    recordError(node, error);
                    LOG.debug("[{}] Reprepare failed, trying next node", logPrefix);
                    sendRequest(null, execution, retryCount);
                  } else {
                    LOG.debug("[{}] Reprepare sucessful, retrying", logPrefix);
                    sendRequest(node, execution, retryCount);
                  }
                  return null;
                });
        return;
      }
      CoordinatorException error = Conversions.toThrowable(node, errorMessage);
      if (error instanceof BootstrappingException) {
        LOG.debug("[{}] {} is bootstrapping, trying next node", logPrefix, node);
        recordError(node, error);
        sendRequest(null, execution, retryCount);
      } else if (error instanceof QueryValidationException
          || error instanceof FunctionFailureException
          || error instanceof ProtocolError) {
        LOG.debug("[{}] Unrecoverable error, rethrowing", logPrefix);
        setFinalError(error);
      } else {
        RetryDecision decision;
        if (error instanceof ReadTimeoutException) {
          ReadTimeoutException readTimeout = (ReadTimeoutException) error;
          decision =
              retryPolicy.onReadTimeout(
                  request,
                  readTimeout.getConsistencyLevel(),
                  readTimeout.getBlockFor(),
                  readTimeout.getReceived(),
                  readTimeout.wasDataPresent(),
                  retryCount);
        } else if (error instanceof WriteTimeoutException) {
          WriteTimeoutException writeTimeout = (WriteTimeoutException) error;
          decision =
              isIdempotent
                  ? retryPolicy.onWriteTimeout(
                      request,
                      writeTimeout.getConsistencyLevel(),
                      writeTimeout.getWriteType(),
                      writeTimeout.getBlockFor(),
                      writeTimeout.getReceived(),
                      retryCount)
                  : RetryDecision.RETHROW;
        } else if (error instanceof UnavailableException) {
          UnavailableException unavailable = (UnavailableException) error;
          decision =
              retryPolicy.onUnavailable(
                  request,
                  unavailable.getConsistencyLevel(),
                  unavailable.getRequired(),
                  unavailable.getAlive(),
                  retryCount);
        } else {
          decision =
              isIdempotent
                  ? retryPolicy.onErrorResponse(request, error, retryCount)
                  : RetryDecision.RETHROW;
        }
        processRetryDecision(decision, error);
      }
    }

    private void processRetryDecision(RetryDecision decision, Throwable error) {
      LOG.debug("[{}] Processing retry decision {}", logPrefix, decision);
      switch (decision) {
        case RETRY_SAME:
          recordError(node, error);
          sendRequest(node, execution, retryCount + 1);
          break;
        case RETRY_NEXT:
          recordError(node, error);
          sendRequest(null, execution, retryCount + 1);
          break;
        case RETHROW:
          setFinalError(error);
          break;
        case IGNORE:
          setFinalResult(Void.INSTANCE, null, this);
          break;
      }
    }

    @Override
    public void onFailure(Throwable error) {
      inFlightCallbacks.remove(this);
      if (result.isDone()) {
        return;
      }
      LOG.debug("[{}] Request failure, processing: {}", logPrefix, error.toString());
      RetryDecision decision =
          isIdempotent
              ? retryPolicy.onRequestAborted(request, error, retryCount)
              : RetryDecision.RETHROW;
      processRetryDecision(decision, error);
    }

    public void cancel() {
      try {
        if (!channel.closeFuture().isDone()) {
          this.channel.cancel(this);
        }
      } catch (Throwable t) {
        LOG.warn("[{}] Error cancelling", logPrefix, t);
      }
    }

    @Override
    public String toString() {
      return logPrefix;
    }
  }
}
