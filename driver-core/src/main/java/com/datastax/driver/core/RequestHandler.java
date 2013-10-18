/*
 *      Copyright (C) 2012 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import java.nio.ByteBuffer;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.exceptions.*;

import com.codahale.metrics.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles a request to cassandra, dealing with host failover and retries on
 * unavailable/timeout.
 */
class RequestHandler implements Connection.ResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);

    private final Session.Manager manager;
    private final Callback callback;

    private final Iterator<Host> queryPlan;
    private final Statement statement;
    private volatile Host current;
    private volatile List<Host> triedHosts;
    private volatile HostConnectionPool currentPool;

    private volatile int queryRetries;
    private volatile ConsistencyLevel retryConsistencyLevel;

    private volatile Map<InetAddress, Throwable> errors;

    private volatile boolean isCanceled;
    private volatile Connection.ResponseHandler connectionHandler;

    private final Timer.Context timerContext;
    private final long startTime;

    public RequestHandler(Session.Manager manager, Callback callback, Statement statement) {
        this.manager = manager;
        this.callback = callback;

        callback.register(this);

        this.queryPlan = manager.loadBalancingPolicy().newQueryPlan(manager.poolsState.keyspace, statement);
        this.statement = statement;

        this.timerContext = metricsEnabled()
                          ? metrics().getRequestsTimer().time()
                          : null;
        this.startTime = System.nanoTime();
    }

    private boolean metricsEnabled() {
        return manager.configuration().getMetricsOptions() != null;
    }

    private Metrics metrics() {
        return manager.cluster.manager.metrics;
    }

    public void sendRequest() {
        try {
            while (queryPlan.hasNext() && !isCanceled) {
                Host host = queryPlan.next();
                logger.trace("Querying node {}", host);
                if (query(host))
                    return;
            }
            setFinalException(null, new NoHostAvailableException(errors == null ? Collections.<InetAddress, Throwable>emptyMap() : errors));
        } catch (Exception e) {
            // Shouldn't happen really, but if ever the loadbalancing policy returned iterator throws, we don't want to block.
            setFinalException(null, new DriverInternalError("An unexpected error happened while sending requests", e));
        }
    }

    private boolean query(Host host) {
        currentPool = manager.pools.get(host);
        if (currentPool == null || currentPool.isShutdown())
            return false;

        Connection connection = null;
        try {
            // Note: this is not perfectly correct to use getConnectTimeoutMillis(), but
            // until we provide a more fancy to control query timeouts, it's not a bad solution either
            connection = currentPool.borrowConnection(manager.configuration().getSocketOptions().getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
            if (current != null) {
                if (triedHosts == null)
                    triedHosts = new ArrayList<Host>();
                triedHosts.add(current);
            }
            current = host;
            connectionHandler = connection.write(this);
            return true;
        } catch (ConnectionException e) {
            // If we have any problem with the connection, move to the next node.
            if (metricsEnabled())
                metrics().getErrorMetrics().getConnectionErrors().inc();
            if (connection != null)
                currentPool.returnConnection(connection);
            logError(host.getAddress(), e);
            return false;
        } catch (BusyConnectionException e) {
            // The pool shoudln't have give us a busy connection unless we've maxed up the pool, so move on to the next host.
            if (connection != null)
                currentPool.returnConnection(connection);
            logError(host.getAddress(), e);
            return false;
        } catch (TimeoutException e) {
            // We timeout, log it but move to the next node.
            logError(host.getAddress(), new DriverException("Timeout while trying to acquire available connection (you may want to increase the driver number of per-host connections)"));
            return false;
        } catch (RuntimeException e) {
            if (connection != null)
                currentPool.returnConnection(connection);
            logger.error("Unexpected error while querying " + host.getAddress(), e);
            logError(host.getAddress(), e);
            return false;
        }
    }

    private void logError(InetAddress address, Throwable exception) {
        logger.debug("Error querying {}, trying next host (error is: {})", address, exception.toString());
        if (errors == null)
            errors = new HashMap<InetAddress, Throwable>();
        errors.put(address, exception);
    }

    private void retry(final boolean retryCurrent, ConsistencyLevel newConsistencyLevel) {
        final Host h = current;
        this.retryConsistencyLevel = newConsistencyLevel;

        // We should not retry on the current thread as this will be an IO thread.
        manager.executor().execute(new Runnable() {
            @Override
            public void run() {
                if (retryCurrent) {
                    if (query(h))
                        return;
                }
                sendRequest();
            }
        });
    }

    public void cancel() {
        isCanceled = true;
        if (connectionHandler != null)
            connectionHandler.cancelHandler();
    }

    @Override
    public Message.Request request() {

        Message.Request request = callback.request();
        if (retryConsistencyLevel != null && retryConsistencyLevel != consistencyOf(request))
            request = manager.makeRequestMessage(statement, retryConsistencyLevel, serialConsistencyOf(request), pagingStateOf(request));
        return request;
    }

    private ConsistencyLevel consistencyOf(Message.Request request) {
        switch (request.type) {
            case QUERY:   return ((Requests.Query)request).options.consistency;
            case EXECUTE: return ((Requests.Execute)request).options.consistency;
            case BATCH:   return ((Requests.Batch)request).consistency;
            default:      return null;
        }
    }

    private ConsistencyLevel serialConsistencyOf(Message.Request request) {
        switch (request.type) {
            case QUERY:   return ((Requests.Query)request).options.serialConsistency;
            case EXECUTE: return ((Requests.Execute)request).options.serialConsistency;
            default:      return null;
        }
    }

    private ByteBuffer pagingStateOf(Message.Request request) {
        switch (request.type) {
            case QUERY:   return ((Requests.Query)request).options.pagingState;
            case EXECUTE: return ((Requests.Execute)request).options.pagingState;
            default:      return null;
        }
    }

    private void setFinalResult(Connection connection, Message.Response response) {
        if (timerContext != null)
            timerContext.stop();

        ExecutionInfo info = current.defaultExecutionInfo;
        if (triedHosts != null)
        {
            triedHosts.add(current);
            info = new ExecutionInfo(triedHosts);
        }
        if (retryConsistencyLevel != null)
            info = info.withAchievedConsistency(retryConsistencyLevel);
        callback.onSet(connection, response, info, statement, System.nanoTime() - startTime);
    }

    private void setFinalException(Connection connection, Exception exception) {
        if (timerContext != null)
            timerContext.stop();
        callback.onException(connection, exception, System.nanoTime() - startTime);
    }

    private void returnConnection(Connection connection) {
        // In most case currentPool won't be null since we set it before sending the
        // query. However, it's possible that for the same write we call both onSet
        // and onException (especially if a node dies, we'll error out the handler and
        // that may race with a result that just came in before the death). That fine
        // though, but it means currentPool might be null (in which case the connection
        // has been returned already to its pool anyway).
        if (currentPool != null)
            currentPool.returnConnection(connection);
    }

    @Override
    public void onSet(Connection connection, Message.Response response, long latency) {

        returnConnection(connection);

        Host queriedHost = current;
        try {
            switch (response.type) {
                case RESULT:
                    setFinalResult(connection, response);
                    break;
                case ERROR:
                    Responses.Error err = (Responses.Error)response;
                    RetryPolicy.RetryDecision retry = null;
                    RetryPolicy retryPolicy = statement.getRetryPolicy() == null
                                            ? manager.configuration().getPolicies().getRetryPolicy()
                                            : statement.getRetryPolicy();
                    switch (err.code) {
                        case READ_TIMEOUT:
                            assert err.infos instanceof ReadTimeoutException;
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getReadTimeouts().inc();

                            ReadTimeoutException rte = (ReadTimeoutException)err.infos;
                            retry = retryPolicy.onReadTimeout(statement,
                                                              rte.getConsistencyLevel(),
                                                              rte.getRequiredAcknowledgements(),
                                                              rte.getReceivedAcknowledgements(),
                                                              rte.wasDataRetrieved(),
                                                              queryRetries);
                            break;
                        case WRITE_TIMEOUT:
                            assert err.infos instanceof WriteTimeoutException;
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getWriteTimeouts().inc();

                            WriteTimeoutException wte = (WriteTimeoutException)err.infos;
                            retry = retryPolicy.onWriteTimeout(statement,
                                                               wte.getConsistencyLevel(),
                                                               wte.getWriteType(),
                                                               wte.getRequiredAcknowledgements(),
                                                               wte.getReceivedAcknowledgements(),
                                                               queryRetries);
                            break;
                        case UNAVAILABLE:
                            assert err.infos instanceof UnavailableException;
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getUnavailables().inc();

                            UnavailableException ue = (UnavailableException)err.infos;
                            retry = retryPolicy.onUnavailable(statement,
                                                              ue.getConsistencyLevel(),
                                                              ue.getRequiredReplicas(),
                                                              ue.getAliveReplicas(),
                                                              queryRetries);
                            break;
                        case OVERLOADED:
                            // Try another node
                            logger.warn("Host {} is overloaded, trying next host.", connection.address);
                            logError(connection.address, new DriverException("Host overloaded"));
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            retry(false, null);
                            return;
                        case IS_BOOTSTRAPPING:
                            // Try another node
                            logger.error("Query sent to {} but it is bootstrapping. This shouldn't happen but trying next host.", connection.address);
                            logError(connection.address, new DriverException("Host is boostrapping"));
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            retry(false, null);
                            return;
                        case UNPREPARED:
                            assert err.infos instanceof MD5Digest;
                            MD5Digest id = (MD5Digest)err.infos;
                            PreparedStatement toPrepare = manager.cluster.manager.preparedQueries.get(id);
                            if (toPrepare == null) {
                                // This shouldn't happen
                                String msg = String.format("Tried to execute unknown prepared query %s", id);
                                logger.error(msg);
                                setFinalException(connection, new DriverInternalError(msg));
                                return;
                            }

                            logger.trace("Preparing required prepared query {} in keyspace {}", toPrepare.getQueryString(), toPrepare.getQueryKeyspace());
                            String currentKeyspace = connection.keyspace();
                            String prepareKeyspace = toPrepare.getQueryKeyspace();
                            // This shouldn't happen in normal use, because a user shouldn't try to execute
                            // a prepared statement with the wrong keyspace set. However, if it does, we'd rather
                            // prepare the query correctly and let the query executing return a meaningful error message
                            if (prepareKeyspace != null && (currentKeyspace == null || !currentKeyspace.equals(prepareKeyspace)))
                            {
                                logger.trace("Setting keyspace for prepared query to {}", prepareKeyspace);
                                connection.setKeyspace(prepareKeyspace);
                            }

                            try {
                                connection.write(prepareAndRetry(toPrepare.getQueryString()));
                            } finally {
                                // Always reset the previous keyspace if needed
                                if (connection.keyspace() == null || !connection.keyspace().equals(currentKeyspace))
                                {
                                    logger.trace("Setting back keyspace post query preparation to {}", currentKeyspace);
                                    connection.setKeyspace(currentKeyspace);
                                }
                            }
                            // we're done for now, the prepareAndRetry callback will handle the rest
                            return;
                        default:
                            if (metricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            break;
                    }

                    if (retry == null)
                        setFinalResult(connection, response);
                    else {
                        switch (retry.getType()) {
                            case RETRY:
                                ++queryRetries;
                                if (logger.isTraceEnabled())
                                    logger.trace("Doing retry {} for query {} at consistency {}", new Object[]{ queryRetries, statement, retry.getRetryConsistencyLevel()});
                                if (metricsEnabled())
                                    metrics().getErrorMetrics().getRetries().inc();
                                retry(true, retry.getRetryConsistencyLevel());
                                break;
                            case RETHROW:
                                setFinalResult(connection, response);
                                break;
                            case IGNORE:
                                if (metricsEnabled())
                                    metrics().getErrorMetrics().getIgnores().inc();
                                setFinalResult(connection, new Responses.Result.Void());
                                break;
                        }
                    }
                    break;
                default:
                    setFinalResult(connection, response);
                    break;
            }
        } catch (Exception e) {
            setFinalException(connection, e);
        } finally {
            if (queriedHost != null)
                manager.cluster.manager.reportLatency(queriedHost, latency);
        }
    }

    private Connection.ResponseCallback prepareAndRetry(final String toPrepare) {
        return new Connection.ResponseCallback() {

            @Override
            public Message.Request request() {
                return new Requests.Prepare(toPrepare);
            }

            @Override
            public void onSet(Connection connection, Message.Response response, long latency) {
                // TODO should we check the response ?
                switch (response.type) {
                    case RESULT:
                        if (((Responses.Result)response).kind == Responses.Result.Kind.PREPARED) {
                            logger.trace("Scheduling retry now that query is prepared");
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
                        RequestHandler.this.setFinalResult(connection, response);
                        break;
                }
            }

            @Override
            public void onException(Connection connection, Exception exception, long latency) {
                RequestHandler.this.onException(connection, exception, latency);
            }

            @Override
            public void onTimeout(Connection connection, long latency) {
                logError(connection.address, new DriverException("Timeout waiting for response to prepare message"));
                retry(false, null);
            }
        };
    }

    @Override
    public void onException(Connection connection, Exception exception, long latency) {

        returnConnection(connection);

        Host queriedHost = current;
        try {
            if (exception instanceof ConnectionException) {
                if (metricsEnabled())
                    metrics().getErrorMetrics().getConnectionErrors().inc();
                ConnectionException ce = (ConnectionException)exception;
                logError(ce.address, ce);
                retry(false, null);
                return;
            }
            setFinalException(connection, exception);
        } finally {
            if (queriedHost != null)
                manager.cluster.manager.reportLatency(queriedHost, latency);
        }
    }

    @Override
    public void onTimeout(Connection connection, long latency) {
        returnConnection(connection);
        Host queriedHost = current;
        logError(connection.address, new DriverException("Timeout during read"));
        retry(false, null);

        if (queriedHost != null)
            manager.cluster.manager.reportLatency(queriedHost, latency);
    }

    interface Callback extends Connection.ResponseCallback {
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, Statement statement, long latency);
        public void register(RequestHandler handler);
    }
}
