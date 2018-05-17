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
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.RequestThrottlingException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.BootstrappingException;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.FunctionFailureException;
import com.datastax.oss.driver.api.core.servererrors.ProtocolError;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.ProtocolFeature;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.concurrent.ScheduledFuture;
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
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles the lifecycle of the preparation of a CQL statement. */
@ThreadSafe
public abstract class CqlPrepareHandlerBase implements Throttled {

  private static final Logger LOG = LoggerFactory.getLogger(CqlPrepareHandlerBase.class);

  private final long startTimeNanos;
  private final String logPrefix;
  private final PrepareRequest request;
  private final ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache;
  private final DefaultSession session;
  private final InternalDriverContext context;
  private final DriverConfigProfile configProfile;
  private final Queue<Node> queryPlan;
  protected final CompletableFuture<PreparedStatement> result;
  private final Message message;
  private final EventExecutor scheduler;
  private final Duration timeout;
  private final ScheduledFuture<?> timeoutFuture;
  private final RetryPolicy retryPolicy;
  private final RequestThrottler throttler;
  private final Boolean prepareOnAllNodes;
  private volatile InitialPrepareCallback initialCallback;

  // The errors on the nodes that were already tried (lazily initialized on the first error).
  // We don't use a map because nodes can appear multiple times.
  private volatile List<Map.Entry<Node, Throwable>> errors;

  protected CqlPrepareHandlerBase(
      PrepareRequest request,
      ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    this.startTimeNanos = System.nanoTime();
    this.logPrefix = sessionLogPrefix + "|" + this.hashCode();
    LOG.trace("[{}] Creating new handler for prepare request {}", logPrefix, request);

    this.request = request;
    this.preparedStatementsCache = preparedStatementsCache;
    this.session = session;
    this.context = context;

    if (request.getConfigProfile() != null) {
      this.configProfile = request.getConfigProfile();
    } else {
      DriverConfig config = context.config();
      String profileName = request.getConfigProfileName();
      this.configProfile =
          (profileName == null || profileName.isEmpty())
              ? config.getDefaultProfile()
              : config.getProfile(profileName);
    }
    this.queryPlan =
        context
            .loadBalancingPolicyWrapper()
            .newQueryPlan(request, configProfile.getName(), session);
    this.retryPolicy = context.retryPolicy(configProfile.getName());

    this.result = new CompletableFuture<>();
    this.result.exceptionally(
        t -> {
          try {
            if (t instanceof CancellationException) {
              cancelTimeout();
            }
          } catch (Throwable t2) {
            Loggers.warnWithException(LOG, "[{}] Uncaught exception", logPrefix, t2);
          }
          return null;
        });
    ProtocolVersion protocolVersion = context.protocolVersion();
    ProtocolVersionRegistry registry = context.protocolVersionRegistry();
    CqlIdentifier keyspace = request.getKeyspace();
    if (keyspace != null
        && !registry.supports(protocolVersion, ProtocolFeature.PER_REQUEST_KEYSPACE)) {
      throw new IllegalArgumentException(
          "Can't use per-request keyspace with protocol " + protocolVersion);
    }
    this.message =
        new Prepare(request.getQuery(), (keyspace == null) ? null : keyspace.asInternal());
    this.scheduler = context.nettyOptions().ioEventLoopGroup().next();

    this.timeout = configProfile.getDuration(DefaultDriverOption.REQUEST_TIMEOUT);
    this.timeoutFuture = scheduleTimeout(timeout);
    this.prepareOnAllNodes = configProfile.getBoolean(DefaultDriverOption.PREPARE_ON_ALL_NODES);

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
    sendRequest(null, 0);
  }

  private ScheduledFuture<?> scheduleTimeout(Duration timeout) {
    if (timeout.toNanos() > 0) {
      return scheduler.schedule(
          () -> {
            setFinalError(new DriverTimeoutException("Query timed out after " + timeout));
            if (initialCallback != null) {
              initialCallback.cancel();
            }
          },
          timeout.toNanos(),
          TimeUnit.NANOSECONDS);
    } else {
      return null;
    }
  }

  private void cancelTimeout() {
    if (this.timeoutFuture != null) {
      this.timeoutFuture.cancel(false);
    }
  }

  private void sendRequest(Node node, int retryCount) {
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
      setFinalError(AllNodesFailedException.fromErrors(this.errors));
    } else {
      InitialPrepareCallback initialPrepareCallback =
          new InitialPrepareCallback(node, channel, retryCount);
      channel
          .write(message, false, request.getCustomPayload(), initialPrepareCallback)
          .addListener(initialPrepareCallback);
    }
  }

  private void recordError(Node node, Throwable error) {
    // Use a local variable to do only a single single volatile read in the nominal case
    List<Map.Entry<Node, Throwable>> errorsSnapshot = this.errors;
    if (errorsSnapshot == null) {
      synchronized (CqlPrepareHandlerBase.this) {
        errorsSnapshot = this.errors;
        if (errorsSnapshot == null) {
          this.errors = errorsSnapshot = new CopyOnWriteArrayList<>();
        }
      }
    }
    errorsSnapshot.add(new AbstractMap.SimpleEntry<>(node, error));
  }

  private void setFinalResult(Prepared prepared) {

    // Whatever happens below, we're done with this stream id
    throttler.signalSuccess(this);

    DefaultPreparedStatement newStatement =
        Conversions.toPreparedStatement(prepared, request, context);

    DefaultPreparedStatement cachedStatement = cache(newStatement);

    if (cachedStatement != newStatement) {
      // The statement already existed in the cache, assume it's because the client called
      // prepare() twice, and therefore it's already been prepared on other nodes.
      result.complete(cachedStatement);
    } else {
      session
          .getRepreparePayloads()
          .put(cachedStatement.getId(), cachedStatement.getRepreparePayload());
      if (prepareOnAllNodes) {
        prepareOnOtherNodes()
            .thenRun(
                () -> {
                  LOG.trace(
                      "[{}] Done repreparing on other nodes, completing the request", logPrefix);
                  result.complete(cachedStatement);
                })
            .exceptionally(
                error -> {
                  result.completeExceptionally(error);
                  return null;
                });
      } else {
        LOG.trace("[{}] Prepare on all nodes is disabled, completing the request", logPrefix);
        result.complete(cachedStatement);
      }
    }
  }

  private DefaultPreparedStatement cache(DefaultPreparedStatement preparedStatement) {
    DefaultPreparedStatement previous =
        preparedStatementsCache.putIfAbsent(preparedStatement.getId(), preparedStatement);
    if (previous != null) {
      LOG.warn(
          "Re-preparing already prepared query. "
              + "This is generally an anti-pattern and will likely affect performance. "
              + "Consider preparing the statement only once. Query='{}'",
          preparedStatement.getQuery());

      // The one object in the cache will get GCed once it's not referenced by the client anymore
      // since we use a weak reference. So we need to make sure that the instance we do return to
      // the user is the one that is in the cache.
      return previous;
    } else {
      return preparedStatement;
    }
  }

  private CompletionStage<Void> prepareOnOtherNodes() {
    List<CompletionStage<Void>> otherNodesFutures = new ArrayList<>();
    // Only process the rest of the query plan. Any node before that is either the coordinator, or
    // a node that failed (we assume that retrying right now has little chance of success).
    for (Node node : queryPlan) {
      otherNodesFutures.add(prepareOnOtherNode(node));
    }
    return CompletableFutures.allDone(otherNodesFutures);
  }

  // Try to reprepare on another node, after the initial query has succeeded. Errors are not
  // blocking, the preparation will be retried later on that node. Simply warn and move on.
  private CompletionStage<Void> prepareOnOtherNode(Node node) {
    LOG.trace("[{}] Repreparing on {}", logPrefix, node);
    DriverChannel channel = session.getChannel(node, logPrefix);
    if (channel == null) {
      LOG.trace("[{}] Could not get a channel to reprepare on {}, skipping", logPrefix, node);
      return CompletableFuture.completedFuture(null);
    } else {
      ThrottledAdminRequestHandler handler =
          new ThrottledAdminRequestHandler(
              channel,
              message,
              request.getCustomPayload(),
              timeout,
              throttler,
              session.getMetricUpdater(),
              logPrefix,
              message.toString());
      return handler
          .start()
          .handle(
              (result, error) -> {
                if (error == null) {
                  LOG.trace("[{}] Successfully reprepared on {}", logPrefix, node);
                } else {
                  Loggers.warnWithException(
                      LOG, "[{}] Error while repreparing on {}", node, logPrefix, error);
                }
                return null;
              });
    }
  }

  @Override
  public void onThrottleFailure(RequestThrottlingException error) {
    session
        .getMetricUpdater()
        .incrementCounter(DefaultSessionMetric.THROTTLING_ERRORS, configProfile.getName());
    setFinalError(error);
  }

  private void setFinalError(Throwable error) {
    if (result.completeExceptionally(error)) {
      cancelTimeout();
      if (error instanceof DriverTimeoutException) {
        throttler.signalTimeout(this);
      } else if (!(error instanceof RequestThrottlingException)) {
        throttler.signalError(this, error);
      }
    }
  }

  private class InitialPrepareCallback
      implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>> {
    private final Node node;
    private final DriverChannel channel;
    // How many times we've invoked the retry policy and it has returned a "retry" decision (0 for
    // the first attempt of each execution).
    private final int retryCount;

    private InitialPrepareCallback(Node node, DriverChannel channel, int retryCount) {
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
        sendRequest(null, retryCount); // try next host
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
          setFinalResult((Prepared) responseMessage);
        } else if (responseMessage instanceof Error) {
          LOG.trace("[{}] Got error response, processing", logPrefix);
          processErrorResponse((Error) responseMessage);
        } else {
          setFinalError(new IllegalStateException("Unexpected response " + responseMessage));
        }
      } catch (Throwable t) {
        setFinalError(t);
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
            new IllegalStateException(
                "Unexpected server error for a PREPARE query" + errorMessage));
        return;
      }
      CoordinatorException error = Conversions.toThrowable(node, errorMessage, context);
      if (error instanceof BootstrappingException) {
        LOG.trace("[{}] {} is bootstrapping, trying next node", logPrefix, node);
        recordError(node, error);
        sendRequest(null, retryCount);
      } else if (error instanceof QueryValidationException
          || error instanceof FunctionFailureException
          || error instanceof ProtocolError) {
        LOG.trace("[{}] Unrecoverable error, rethrowing", logPrefix);
        setFinalError(error);
      } else {
        // Because prepare requests are known to always be idempotent, we call the retry policy
        // directly, without checking the flag.
        RetryDecision decision = retryPolicy.onErrorResponse(request, error, retryCount);
        processRetryDecision(decision, error);
      }
    }

    private void processRetryDecision(RetryDecision decision, Throwable error) {
      LOG.trace("[{}] Processing retry decision {}", logPrefix, decision);
      switch (decision) {
        case RETRY_SAME:
          recordError(node, error);
          sendRequest(node, retryCount + 1);
          break;
        case RETRY_NEXT:
          recordError(node, error);
          sendRequest(null, retryCount + 1);
          break;
        case RETHROW:
          setFinalError(error);
          break;
        case IGNORE:
          setFinalError(
              new IllegalArgumentException(
                  "IGNORE decisions are not allowed for prepare requests, "
                      + "please fix your retry policy."));
          break;
      }
    }

    @Override
    public void onFailure(Throwable error) {
      if (result.isDone()) {
        return;
      }
      LOG.trace("[{}] Request failure, processing: {}", logPrefix, error.toString());
      RetryDecision decision = retryPolicy.onRequestAborted(request, error, retryCount);
      processRetryDecision(decision, error);
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
