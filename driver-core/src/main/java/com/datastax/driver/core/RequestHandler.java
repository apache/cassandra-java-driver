/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.core;

import com.codahale.metrics.Timer;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RetryPolicy.RetryDecision.Type;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy.SpeculativeExecutionPlan;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Handles a request to cassandra, dealing with host failover and retries on
 * unavailable/timeout.
 */
class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    final String id;

    private final SessionManager manager;
    private final Callback callback;

    private final QueryPlan queryPlan;
    private final SpeculativeExecutionPlan speculativeExecutionPlan;
    private final boolean allowSpeculativeExecutions;
    private final Set<SpeculativeExecution> runningExecutions = Sets.newCopyOnWriteArraySet();
    private final Set<Timeout> scheduledExecutions = Sets.newCopyOnWriteArraySet();
    private final Statement statement;
    private final io.netty.util.Timer scheduler;

    private volatile List<Host> triedHosts;
    private volatile ConcurrentMap<InetSocketAddress, Throwable> errors;

    private final Timer.Context timerContext;
    private final long startTime;

    private final AtomicBoolean isDone = new AtomicBoolean();
    private final AtomicInteger executionCount = new AtomicInteger();

    public RequestHandler(SessionManager manager, Callback callback, Statement statement) {
        this.id = Long.toString(System.identityHashCode(this));
        if (logger.isTraceEnabled())
            logger.trace("[{}] {}", id, statement);
        this.manager = manager;
        this.callback = callback;
        this.scheduler = manager.cluster.manager.connectionFactory.timer;

        callback.register(this);

        this.queryPlan = new QueryPlan(manager.loadBalancingPolicy().newQueryPlan(manager.poolsState.keyspace, statement));
        this.speculativeExecutionPlan = manager.speculativeExecutionPolicy().newPlan(manager.poolsState.keyspace, statement);
        this.allowSpeculativeExecutions = statement != Statement.DEFAULT
                && statement.isIdempotentWithDefault(manager.configuration().getQueryOptions());
        this.statement = statement;

        this.timerContext = metricsEnabled()
                ? metrics().getRequestsTimer().time()
                : null;
        this.startTime = System.nanoTime();
    }

    void sendRequest() {
        startNewExecution();
    }

    // Called when the corresponding ResultSetFuture is cancelled by the client
    void cancel() {
        if (!isDone.compareAndSet(false, true))
            return;

        cancelPendingExecutions(null);
    }

    private void startNewExecution() {
        if (isDone.get())
            return;

        Message.Request request = callback.request();
        int position = executionCount.incrementAndGet();

        SpeculativeExecution execution = new SpeculativeExecution(request, position);
        runningExecutions.add(execution);
        execution.findNextHostAndQuery();
    }

    private void scheduleExecution(long delayMillis) {
        if (isDone.get() || delayMillis <= 0)
            return;
        if (logger.isTraceEnabled())
            logger.trace("[{}] Schedule next speculative execution in {} ms", id, delayMillis);
        scheduledExecutions.add(scheduler.newTimeout(newExecutionTask, delayMillis, TimeUnit.MILLISECONDS));
    }

    private final TimerTask newExecutionTask = new TimerTask() {
        @Override
        public void run(final Timeout timeout) throws Exception {
            scheduledExecutions.remove(timeout);
            if (!isDone.get())
                // We're on the timer thread so reschedule to another executor
                manager.executor().execute(new Runnable() {
                    @Override
                    public void run() {
                        if (metricsEnabled())
                            metrics().getErrorMetrics().getSpeculativeExecutions().inc();
                        startNewExecution();
                    }
                });
        }
    };

    private void cancelPendingExecutions(SpeculativeExecution ignore) {
        for (SpeculativeExecution execution : runningExecutions)
            if (execution != ignore) // not vital but this produces nicer logs
                execution.cancel();
        for (Timeout execution : scheduledExecutions)
            execution.cancel();
    }

    private void setFinalResult(SpeculativeExecution execution, Connection connection, Message.Response response) {
        if (!isDone.compareAndSet(false, true)) {
            if (logger.isTraceEnabled())
                logger.trace("[{}] Got beaten to setting the result", execution.id);
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("[{}] Setting final result", execution.id);

        cancelPendingExecutions(execution);

        try {
            if (timerContext != null)
                timerContext.stop();

            ExecutionInfo info = execution.current.defaultExecutionInfo;
            if (triedHosts != null) {
                triedHosts.add(execution.current);
                info = new ExecutionInfo(triedHosts);
            }
            if (execution.retryConsistencyLevel != null)
                info = info.withAchievedConsistency(execution.retryConsistencyLevel);
            if (response.getCustomPayload() != null)
                info = info.withIncomingPayload(response.getCustomPayload());

            callback.onSet(connection, response, info, statement, System.nanoTime() - startTime);
        } catch (Exception e) {
            callback.onException(connection,
                    new DriverInternalError("Unexpected exception while setting final result from " + response, e),
                    System.nanoTime() - startTime, /*unused*/0);
        }
    }

    private void setFinalException(SpeculativeExecution execution, Connection connection, Exception exception) {
        if (!isDone.compareAndSet(false, true)) {
            if (logger.isTraceEnabled())
                logger.trace("[{}] Got beaten to setting final exception", execution.id);
            return;
        }

        if (logger.isTraceEnabled())
            logger.trace("[{}] Setting final exception", execution.id);

        cancelPendingExecutions(execution);

        try {
            if (timerContext != null)
                timerContext.stop();
        } finally {
            callback.onException(connection, exception, System.nanoTime() - startTime, /*unused*/0);
        }
    }

    // Triggered when an execution reaches the end of the query plan.
    // This is only a failure if there are no other running executions.
    private void reportNoMoreHosts(SpeculativeExecution execution) {
        runningExecutions.remove(execution);
        if (runningExecutions.isEmpty())
            setFinalException(execution, null, new NoHostAvailableException(
                    errors == null ? Collections.<InetSocketAddress, Throwable>emptyMap() : errors));
    }

    private boolean metricsEnabled() {
        return manager.configuration().getMetricsOptions().isEnabled();
    }

    private Metrics metrics() {
        return manager.cluster.manager.metrics;
    }

    private RetryPolicy retryPolicy() {
        return statement.getRetryPolicy() == null
                ? manager.configuration().getPolicies().getRetryPolicy()
                : statement.getRetryPolicy();
    }

    interface Callback extends Connection.ResponseCallback {
        void onSet(Connection connection, Message.Response response, ExecutionInfo info, Statement statement, long latency);

        void register(RequestHandler handler);
    }

    /**
     * An execution of the query against the cluster.
     * There is at least one instance per RequestHandler, and possibly more (depending on the SpeculativeExecutionPolicy).
     * Each instance may retry on the same host, or on other hosts as defined by the RetryPolicy.
     * All instances run concurrently and share the same query plan.
     * There are three ways a SpeculativeExecution can stop:
     * - it completes the query (with either a success or a fatal error), and reports the result to the RequestHandler
     * - it gets cancelled, either because another execution completed the query, or because the RequestHandler was cancelled
     * - it reaches the end of the query plan and informs the RequestHandler, which will decide what to do
     */
    class SpeculativeExecution implements Connection.ResponseCallback {
        final String id;
        private final Message.Request request;
        private volatile Host current;
        private volatile ConsistencyLevel retryConsistencyLevel;
        private final AtomicReference<QueryState> queryStateRef;
        private final AtomicBoolean nextExecutionScheduled = new AtomicBoolean();

        // This represents the number of times a retry has been triggered by the RetryPolicy (this is different from
        // queryStateRef.get().retryCount, because some retries don't involve the policy, for example after an
        // UNPREPARED response).
        // This is incremented by one writer at a time, so volatile is good enough.
        private volatile int retriesByPolicy;

        private volatile Connection.ResponseHandler connectionHandler;

        SpeculativeExecution(Message.Request request, int position) {
            this.id = RequestHandler.this.id + "-" + position;
            this.request = request;
            this.queryStateRef = new AtomicReference<QueryState>(QueryState.INITIAL);
            if (logger.isTraceEnabled())
                logger.trace("[{}] Starting", id);
        }

        void findNextHostAndQuery() {
            try {
                Host host;
                while (!isDone.get() && (host = queryPlan.next()) != null && !queryStateRef.get().isCancelled()) {
                    if (query(host))
                        return;
                }
                reportNoMoreHosts(this);
            } catch (Exception e) {
                // Shouldn't happen really, but if ever the loadbalancing policy returned iterator throws, we don't want to block.
                setFinalException(null, new DriverInternalError("An unexpected error happened while sending requests", e));
            }
        }

        private boolean query(final Host host) {
            HostConnectionPool pool = manager.pools.get(host);
            if (pool == null || pool.isClosed())
                return false;

            if (logger.isTraceEnabled())
                logger.trace("[{}] Querying node {}", id, host);

            if (allowSpeculativeExecutions && nextExecutionScheduled.compareAndSet(false, true))
                scheduleExecution(speculativeExecutionPlan.nextExecution(host));

            PoolingOptions poolingOptions = manager.configuration().getPoolingOptions();
            ListenableFuture<Connection> connectionFuture = pool.borrowConnection(
                    poolingOptions.getPoolTimeoutMillis(), TimeUnit.MILLISECONDS,
                    poolingOptions.getMaxQueueSize());
            Futures.addCallback(connectionFuture, new FutureCallback<Connection>() {
                @Override
                public void onSuccess(Connection connection) {
                    if (current != null) {
                        if (triedHosts == null)
                            triedHosts = new CopyOnWriteArrayList<Host>();
                        triedHosts.add(current);
                    }
                    current = host;
                    try {
                        write(connection, SpeculativeExecution.this);
                    } catch (ConnectionException e) {
                        // If we have any problem with the connection, move to the next node.
                        if (metricsEnabled())
                            metrics().getErrorMetrics().getConnectionErrors().inc();
                        if (connection != null)
                            connection.release();
                        logError(host.getSocketAddress(), e);
                        findNextHostAndQuery();
                    } catch (BusyConnectionException e) {
                        // The pool shouldn't have give us a busy connection unless we've maxed up the pool, so move on to the next host.
                        connection.release();
                        logError(host.getSocketAddress(), e);
                        findNextHostAndQuery();
                    } catch (RuntimeException e) {
                        if (connection != null)
                            connection.release();
                        logger.error("Unexpected error while querying " + host.getAddress(), e);
                        logError(host.getSocketAddress(), e);
                        findNextHostAndQuery();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    if (t instanceof BusyPoolException) {
                        logError(host.getSocketAddress(), t);
                    } else {
                        logger.error("Unexpected error while querying " + host.getAddress(), t);
                        logError(host.getSocketAddress(), t);
                    }
                    findNextHostAndQuery();
                }
            });
            return true;
        }

        private void write(Connection connection, Connection.ResponseCallback responseCallback) throws ConnectionException, BusyConnectionException {
            // Make sure cancel() does not see a stale connectionHandler if it sees the new query state
            // before connection.write has completed
            connectionHandler = null;

            // Ensure query state is "in progress" (can be already if connection.write failed on a previous node and we're retrying)
            while (true) {
                QueryState previous = queryStateRef.get();
                if (previous.isCancelled()) {
                    connection.release();
                    return;
                }
                if (previous.inProgress || queryStateRef.compareAndSet(previous, previous.startNext()))
                    break;
            }

            connectionHandler = connection.write(responseCallback, statement.getReadTimeoutMillis(), false);
            // Only start the timeout when we're sure connectionHandler is set. This avoids an edge case where onTimeout() was triggered
            // *before* the call to connection.write had returned.
            connectionHandler.startTimeout();

            // Note that we could have already received the response here (so onSet() / onException() would have been called). This is
            // why we only test for CANCELLED_WHILE_IN_PROGRESS below.

            // If cancel() was called after we set the state to "in progress", but before connection.write had completed, it might have
            // missed the new value of connectionHandler. So make sure that cancelHandler() gets called here (we might call it twice,
            // but it knows how to deal with it).
            if (queryStateRef.get() == QueryState.CANCELLED_WHILE_IN_PROGRESS && connectionHandler.cancelHandler())
                connection.release();
        }

        private RetryPolicy.RetryDecision computeRetryDecisionOnRequestError(DriverException exception) {
            RetryPolicy.RetryDecision decision;
            if (statement.isIdempotentWithDefault(manager.cluster.getConfiguration().getQueryOptions())) {
                decision = retryPolicy().onRequestError(statement, request().consistency(), exception, retriesByPolicy);
            } else {
                decision = RetryPolicy.RetryDecision.rethrow();
            }
            if (metricsEnabled()) {
                if (exception instanceof OperationTimedOutException) {
                    metrics().getErrorMetrics().getClientTimeouts().inc();
                    if (decision.getType() == Type.RETRY)
                        metrics().getErrorMetrics().getRetriesOnClientTimeout().inc();
                    if (decision.getType() == Type.IGNORE)
                        metrics().getErrorMetrics().getIgnoresOnClientTimeout().inc();
                } else if (exception instanceof ConnectionException) {
                    metrics().getErrorMetrics().getConnectionErrors().inc();
                    if (decision.getType() == Type.RETRY)
                        metrics().getErrorMetrics().getRetriesOnConnectionError().inc();
                    if (decision.getType() == Type.IGNORE)
                        metrics().getErrorMetrics().getIgnoresOnConnectionError().inc();
                } else {
                    metrics().getErrorMetrics().getOthers().inc();
                    if (decision.getType() == Type.RETRY)
                        metrics().getErrorMetrics().getRetriesOnOtherErrors().inc();
                    if (decision.getType() == Type.IGNORE)
                        metrics().getErrorMetrics().getIgnoresOnOtherErrors().inc();
                }
            }
            return decision;
        }

        private void processRetryDecision(RetryPolicy.RetryDecision retryDecision, Connection connection, Exception exceptionToReport) {
            switch (retryDecision.getType()) {
                case RETRY:
                    retriesByPolicy++;
                    if (logger.isDebugEnabled())
                        logger.debug("[{}] Doing retry {} for query {} at consistency {}", id, retriesByPolicy, statement, retryDecision.getRetryConsistencyLevel());
                    if (metricsEnabled())
                        metrics().getErrorMetrics().getRetries().inc();
                    // log error for the current host if we are switching to another one
                    if (!retryDecision.isRetryCurrent())
                        logError(connection.address, exceptionToReport);
                    retry(retryDecision.isRetryCurrent(), retryDecision.getRetryConsistencyLevel());
                    break;
                case RETHROW:
                    setFinalException(connection, exceptionToReport);
                    break;
                case IGNORE:
                    if (metricsEnabled())
                        metrics().getErrorMetrics().getIgnores().inc();
                    setFinalResult(connection, new Responses.Result.Void());
                    break;
            }
        }

        private void retry(final boolean retryCurrent, ConsistencyLevel newConsistencyLevel) {
            final Host h = current;
            if (newConsistencyLevel != null)
                this.retryConsistencyLevel = newConsistencyLevel;

            if (queryStateRef.get().isCancelled())
                return;

            if (!retryCurrent || !query(h))
                findNextHostAndQuery();
        }

        private void logError(InetSocketAddress address, Throwable exception) {
            logger.debug("[{}] Error querying {} : {}", id, address, exception.toString());
            if (errors == null) {
                synchronized (RequestHandler.this) {
                    if (errors == null) {
                        errors = new ConcurrentHashMap<InetSocketAddress, Throwable>();
                    }
                }
            }
            errors.put(address, exception);
        }

        void cancel() {
            // Atomically set a special QueryState, that will cause any further operation to abort.
            // We want to remember whether a request was in progress when we did this, so there are two cancel states.
            while (true) {
                QueryState previous = queryStateRef.get();
                if (previous.isCancelled()) {
                    return;
                } else if (previous.inProgress && queryStateRef.compareAndSet(previous, QueryState.CANCELLED_WHILE_IN_PROGRESS)) {
                    if (logger.isTraceEnabled())
                        logger.trace("[{}] Cancelled while in progress", id);
                    // The connectionHandler should be non-null, but we might miss the update if we're racing with write().
                    // If it's still null, this will be handled by re-checking queryStateRef at the end of write().
                    if (connectionHandler != null && connectionHandler.cancelHandler())
                        connectionHandler.connection.release();
                    return;
                } else if (!previous.inProgress && queryStateRef.compareAndSet(previous, QueryState.CANCELLED_WHILE_COMPLETE)) {
                    if (logger.isTraceEnabled())
                        logger.trace("[{}] Cancelled while complete", id);
                    return;
                }
            }
        }

        @Override
        public Message.Request request() {
            if (retryConsistencyLevel != null && retryConsistencyLevel != request.consistency())
                return request.copy(retryConsistencyLevel);
            else
                return request;
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
            QueryState queryState = queryStateRef.get();
            if (!queryState.isInProgressAt(retryCount) ||
                    !queryStateRef.compareAndSet(queryState, queryState.complete())) {
                logger.debug("onSet triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                        retryCount, queryState, queryStateRef.get());
                return;
            }

            Host queriedHost = current;
            Exception exceptionToReport = null;
            try {
                switch (response.type) {
                    case RESULT:
                        connection.release();
                        setFinalResult(connection, response);
                        break;
                    case ERROR:
                        Responses.Error err = (Responses.Error) response;
                        exceptionToReport = err.asException(connection.address);
                        RetryPolicy.RetryDecision retry = null;
                        RetryPolicy retryPolicy = retryPolicy();
                        switch (err.code) {
                            case READ_TIMEOUT:
                                connection.release();
                                assert err.infos instanceof ReadTimeoutException;
                                ReadTimeoutException rte = (ReadTimeoutException) err.infos;
                                retry = retryPolicy.onReadTimeout(statement,
                                        rte.getConsistencyLevel(),
                                        rte.getRequiredAcknowledgements(),
                                        rte.getReceivedAcknowledgements(),
                                        rte.wasDataRetrieved(),
                                        retriesByPolicy);
                                if (metricsEnabled()) {
                                    metrics().getErrorMetrics().getReadTimeouts().inc();
                                    if (retry.getType() == Type.RETRY)
                                        metrics().getErrorMetrics().getRetriesOnReadTimeout().inc();
                                    if (retry.getType() == Type.IGNORE)
                                        metrics().getErrorMetrics().getIgnoresOnReadTimeout().inc();
                                }
                                break;
                            case WRITE_TIMEOUT:
                                connection.release();
                                assert err.infos instanceof WriteTimeoutException;
                                WriteTimeoutException wte = (WriteTimeoutException) err.infos;
                                if (statement.isIdempotentWithDefault(manager.cluster.getConfiguration().getQueryOptions()))
                                    retry = retryPolicy.onWriteTimeout(statement,
                                            wte.getConsistencyLevel(),
                                            wte.getWriteType(),
                                            wte.getRequiredAcknowledgements(),
                                            wte.getReceivedAcknowledgements(),
                                            retriesByPolicy);
                                else {
                                    retry = RetryPolicy.RetryDecision.rethrow();
                                }
                                if (metricsEnabled()) {
                                    metrics().getErrorMetrics().getWriteTimeouts().inc();
                                    if (retry.getType() == Type.RETRY)
                                        metrics().getErrorMetrics().getRetriesOnWriteTimeout().inc();
                                    if (retry.getType() == Type.IGNORE)
                                        metrics().getErrorMetrics().getIgnoresOnWriteTimeout().inc();
                                }
                                break;
                            case UNAVAILABLE:
                                connection.release();
                                assert err.infos instanceof UnavailableException;
                                UnavailableException ue = (UnavailableException) err.infos;
                                retry = retryPolicy.onUnavailable(statement,
                                        ue.getConsistencyLevel(),
                                        ue.getRequiredReplicas(),
                                        ue.getAliveReplicas(),
                                        retriesByPolicy);
                                if (metricsEnabled()) {
                                    metrics().getErrorMetrics().getUnavailables().inc();
                                    if (retry.getType() == Type.RETRY)
                                        metrics().getErrorMetrics().getRetriesOnUnavailable().inc();
                                    if (retry.getType() == Type.IGNORE)
                                        metrics().getErrorMetrics().getIgnoresOnUnavailable().inc();
                                }
                                break;
                            case OVERLOADED:
                                connection.release();
                                assert exceptionToReport instanceof OverloadedException;
                                logger.warn("Host {} is overloaded.", connection.address);
                                retry = computeRetryDecisionOnRequestError((OverloadedException) exceptionToReport);
                                break;
                            case SERVER_ERROR:
                                connection.release();
                                assert exceptionToReport instanceof ServerError;
                                logger.warn("{} replied with server error ({}), defuncting connection.", connection.address, err.message);
                                // Defunct connection
                                connection.defunct(exceptionToReport);
                                retry = computeRetryDecisionOnRequestError((ServerError) exceptionToReport);
                                break;
                            case IS_BOOTSTRAPPING:
                                connection.release();
                                assert exceptionToReport instanceof BootstrappingException;
                                logger.error("Query sent to {} but it is bootstrapping. This shouldn't happen but trying next host.", connection.address);
                                if (metricsEnabled()) {
                                    metrics().getErrorMetrics().getOthers().inc();
                                }
                                logError(connection.address, exceptionToReport);
                                retry(false, null);
                                return;
                            case UNPREPARED:
                                // Do not release connection yet, because we might reuse it to send the PREPARE message (see write() call below)
                                assert err.infos instanceof MD5Digest;
                                MD5Digest id = (MD5Digest) err.infos;
                                PreparedStatement toPrepare = manager.cluster.manager.preparedQueries.get(id);
                                if (toPrepare == null) {
                                    // This shouldn't happen
                                    connection.release();
                                    String msg = String.format("Tried to execute unknown prepared query %s", id);
                                    logger.error(msg);
                                    setFinalException(connection, new DriverInternalError(msg));
                                    return;
                                }

                                String currentKeyspace = connection.keyspace();
                                String prepareKeyspace = toPrepare.getQueryKeyspace();
                                if (prepareKeyspace != null && (currentKeyspace == null || !currentKeyspace.equals(prepareKeyspace))) {
                                    // This shouldn't happen in normal use, because a user shouldn't try to execute
                                    // a prepared statement with the wrong keyspace set.
                                    // Fail fast (we can't change the keyspace to reprepare, because we're using a pooled connection
                                    // that's shared with other requests).
                                    connection.release();
                                    throw new IllegalStateException(String.format("Statement was prepared on keyspace %s, can't execute it on %s (%s)",
                                            toPrepare.getQueryKeyspace(), connection.keyspace(), toPrepare.getQueryString()));
                                }

                                logger.info("Query {} is not prepared on {}, preparing before retrying executing. "
                                                + "Seeing this message a few times is fine, but seeing it a lot may be source of performance problems",
                                        toPrepare.getQueryString(), connection.address);

                                write(connection, prepareAndRetry(toPrepare.getQueryString()));
                                // we're done for now, the prepareAndRetry callback will handle the rest
                                return;
                            default:
                                connection.release();
                                if (metricsEnabled())
                                    metrics().getErrorMetrics().getOthers().inc();
                                break;
                        }

                        if (retry == null)
                            setFinalResult(connection, response);
                        else {
                            processRetryDecision(retry, connection, exceptionToReport);
                        }
                        break;
                    default:
                        connection.release();
                        setFinalResult(connection, response);
                        break;
                }
            } catch (Exception e) {
                exceptionToReport = e;
                setFinalException(connection, e);
            } finally {
                if (queriedHost != null && statement != Statement.DEFAULT) {
                    manager.cluster.manager.reportQuery(queriedHost, statement, exceptionToReport, latency);
                }
            }
        }

        private Connection.ResponseCallback prepareAndRetry(final String toPrepare) {
            // do not bother inspecting retry policy at this step, no other decision
            // makes sense than retry on the same host if the query was prepared,
            // or on another host, if an error/timeout occurred.
            // The original request hasn't been executed so far, so there is no risk
            // of re-executing non-idempotent statements.
            return new Connection.ResponseCallback() {

                @Override
                public Message.Request request() {
                    Requests.Prepare request = new Requests.Prepare(toPrepare);
                    // propagate the original custom payload in the prepare request
                    request.setCustomPayload(statement.getOutgoingPayload());
                    return request;
                }

                @Override
                public int retryCount() {
                    return SpeculativeExecution.this.retryCount();
                }

                @Override
                public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
                    QueryState queryState = queryStateRef.get();
                    if (!queryState.isInProgressAt(retryCount) ||
                            !queryStateRef.compareAndSet(queryState, queryState.complete())) {
                        logger.debug("onSet triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                                retryCount, queryState, queryStateRef.get());
                        return;
                    }

                    connection.release();

                    switch (response.type) {
                        case RESULT:
                            if (((Responses.Result) response).kind == Responses.Result.Kind.PREPARED) {
                                logger.debug("Scheduling retry now that query is prepared");
                                retry(true, null);
                            } else {
                                logError(connection.address, new DriverException("Got unexpected response to prepare message: " + response));
                                retry(false, null);
                            }
                            break;
                        case ERROR:
                            logError(connection.address, new DriverException("Error preparing query, got " + response));
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            retry(false, null);
                            break;
                        default:
                            // Something's wrong, so we return but we let setFinalResult propagate the exception
                            SpeculativeExecution.this.setFinalResult(connection, response);
                            break;
                    }
                }

                @Override
                public void onException(Connection connection, Exception exception, long latency, int retryCount) {
                    SpeculativeExecution.this.onException(connection, exception, latency, retryCount);
                }

                @Override
                public boolean onTimeout(Connection connection, long latency, int retryCount) {
                    QueryState queryState = queryStateRef.get();
                    if (!queryState.isInProgressAt(retryCount) ||
                            !queryStateRef.compareAndSet(queryState, queryState.complete())) {
                        logger.debug("onTimeout triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                                retryCount, queryState, queryStateRef.get());
                        return false;
                    }
                    connection.release();
                    logError(connection.address, new OperationTimedOutException(connection.address, "Timed out waiting for response to PREPARE message"));
                    retry(false, null);
                    return true;
                }
            };
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency, int retryCount) {
            QueryState queryState = queryStateRef.get();
            if (!queryState.isInProgressAt(retryCount) ||
                    !queryStateRef.compareAndSet(queryState, queryState.complete())) {
                logger.debug("onException triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                        retryCount, queryState, queryStateRef.get());
                return;
            }

            Host queriedHost = current;
            try {
                connection.release();

                if (exception instanceof ConnectionException) {
                    RetryPolicy.RetryDecision decision = computeRetryDecisionOnRequestError((ConnectionException) exception);
                    processRetryDecision(decision, connection, exception);
                    return;
                }
                setFinalException(connection, exception);
            } catch (Exception e) {
                // This shouldn't happen, but if it does, we want to signal the callback, not let it hang indefinitely
                setFinalException(null, new DriverInternalError("An unexpected error happened while handling exception " + exception, e));
            } finally {
                if (queriedHost != null && statement != Statement.DEFAULT)
                    manager.cluster.manager.reportQuery(queriedHost, statement, exception, latency);
            }
        }

        @Override
        public boolean onTimeout(Connection connection, long latency, int retryCount) {
            QueryState queryState = queryStateRef.get();
            if (!queryState.isInProgressAt(retryCount) ||
                    !queryStateRef.compareAndSet(queryState, queryState.complete())) {
                logger.debug("onTimeout triggered but the response was completed by another thread, cancelling (retryCount = {}, queryState = {}, queryStateRef = {})",
                        retryCount, queryState, queryStateRef.get());
                return false;
            }

            Host queriedHost = current;

            OperationTimedOutException timeoutException = new OperationTimedOutException(connection.address, "Timed out waiting for server response");

            try {
                connection.release();

                RetryPolicy.RetryDecision decision = computeRetryDecisionOnRequestError(timeoutException);
                processRetryDecision(decision, connection, timeoutException);
            } catch (Exception e) {
                // This shouldn't happen, but if it does, we want to signal the callback, not let it hang indefinitely
                setFinalException(null, new DriverInternalError("An unexpected error happened while handling timeout", e));
            } finally {
                if (queriedHost != null && statement != Statement.DEFAULT)
                    manager.cluster.manager.reportQuery(queriedHost, statement, timeoutException, latency);
            }
            return true;
        }

        @Override
        public int retryCount() {
            return queryStateRef.get().retryCount;
        }

        private void setFinalException(Connection connection, Exception exception) {
            RequestHandler.this.setFinalException(this, connection, exception);
        }

        private void setFinalResult(Connection connection, Message.Response response) {
            RequestHandler.this.setFinalResult(this, connection, response);
        }
    }

    /**
     * The state of a SpeculativeExecution.
     * <p/>
     * This is used to prevent races between request completion (either success or error) and timeout.
     * A retry is in progress once we have written the request to the connection and until we get back a response (see onSet
     * or onException) or a timeout (see onTimeout).
     * The count increments on each retry.
     */
    static class QueryState {
        static final QueryState INITIAL = new QueryState(-1, false);
        static final QueryState CANCELLED_WHILE_IN_PROGRESS = new QueryState(Integer.MIN_VALUE, false);
        static final QueryState CANCELLED_WHILE_COMPLETE = new QueryState(Integer.MIN_VALUE + 1, false);

        final int retryCount;
        final boolean inProgress;

        private QueryState(int count, boolean inProgress) {
            this.retryCount = count;
            this.inProgress = inProgress;
        }

        boolean isInProgressAt(int retryCount) {
            return inProgress && this.retryCount == retryCount;
        }

        QueryState complete() {
            assert inProgress;
            return new QueryState(retryCount, false);
        }

        QueryState startNext() {
            assert !inProgress;
            return new QueryState(retryCount + 1, true);
        }

        public boolean isCancelled() {
            return this == CANCELLED_WHILE_IN_PROGRESS || this == CANCELLED_WHILE_COMPLETE;
        }

        @Override
        public String toString() {
            return String.format("QueryState(count=%d, inProgress=%s, cancelled=%s)", retryCount, inProgress, isCancelled());
        }
    }

    /**
     * Wraps the iterator return by {@link com.datastax.driver.core.policies.LoadBalancingPolicy} to make it safe for
     * concurrent access by multiple threads.
     */
    static class QueryPlan {
        private final Iterator<Host> iterator;

        QueryPlan(Iterator<Host> iterator) {
            this.iterator = iterator;
        }

        /**
         * @return null if there are no more hosts
         */
        synchronized Host next() {
            return iterator.hasNext() ? iterator.next() : null;
        }
    }
}
