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

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.PreparedQueryNotFoundException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;

import com.yammer.metrics.core.TimerContext;

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
    private final TimerContext timerContext;

    private final Iterator<Host> queryPlan;
    private final Query query;
    private volatile Host current;
    private volatile List<Host> triedHosts;
    private volatile HostConnectionPool currentPool;

    private volatile int queryRetries;
    private volatile ConsistencyLevel retryConsistencyLevel;

    private volatile Map<InetAddress, String> errors;

    public RequestHandler(Session.Manager manager, Callback callback, Query query) {
        this.manager = manager;
        this.callback = callback;

        this.queryPlan = manager.loadBalancer.newQueryPlan(query);
        this.query = query;

        this.timerContext = manager.configuration().isMetricsEnabled()
                          ? metrics().getRequestsTimer().time()
                          : null;
    }

    private Metrics metrics() {
        return manager.cluster.manager.metrics;
    }

    public void sendRequest() {

        while (queryPlan.hasNext()) {
            Host host = queryPlan.next();
            if (query(host))
                return;
        }
        setFinalException(null, new NoHostAvailableException(errors == null ? Collections.<InetAddress, String>emptyMap() : errors));
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
            connection.write(this);
            return true;
        } catch (ConnectionException e) {
            // If we have any problem with the connection, move to the next node.
            if (manager.configuration().isMetricsEnabled())
                metrics().getErrorMetrics().getConnectionErrors().inc();
            if (connection != null)
                currentPool.returnConnection(connection);
            logError(host.getAddress(), e.getMessage());
            return false;
        } catch (BusyConnectionException e) {
            // The pool shoudln't have give us a busy connection unless we've maxed up the pool, so move on to the next host.
            if (connection != null)
                currentPool.returnConnection(connection);
            logError(host.getAddress(), e.getMessage());
            return false;
        } catch (TimeoutException e) {
            // We timeout, log it but move to the next node.
            logError(host.getAddress(), "Timeout while trying to acquire available connection");
            return false;
        } catch (RuntimeException e) {
            if (connection != null)
                currentPool.returnConnection(connection);
            logger.error("Unexpected error while querying " + host.getAddress(), e);
            logError(host.getAddress(), e.getMessage());
            return false;
        }
    }

    private void logError(InetAddress address, String msg) {
        logger.debug("Error querying {}, trying next host (error is: {})", address, msg);
        if (errors == null)
            errors = new HashMap<InetAddress, String>();
        errors.put(address, msg);
    }

    private void retry(final boolean retryCurrent, ConsistencyLevel newConsistencyLevel) {
        final Host h = current;
        this.retryConsistencyLevel = newConsistencyLevel;

        // We should not retry on the current thread as this will be an IO thread.
        manager.executor().execute(new Runnable() {
            public void run() {
                if (retryCurrent) {
                    if (query(h))
                        return;
                }
                sendRequest();
            }
        });
    }

    public Message.Request request() {

        Message.Request request = callback.request();
        if (retryConsistencyLevel != null) {
            org.apache.cassandra.db.ConsistencyLevel cl = ConsistencyLevel.toCassandraCL(retryConsistencyLevel);
            if (request instanceof QueryMessage) {
                QueryMessage qm = (QueryMessage)request;
                if (qm.consistency != cl)
                    request = new QueryMessage(qm.query, cl);
            }
            else if (request instanceof ExecuteMessage) {
                ExecuteMessage em = (ExecuteMessage)request;
                if (em.consistency != cl)
                    request = new ExecuteMessage(em.statementId, em.values, cl);
            }
        }
        return request;
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
        callback.onSet(connection, response, info);
    }

    private void setFinalException(Connection connection, Exception exception) {
        if (timerContext != null)
            timerContext.stop();
        callback.onException(connection, exception);
    }

    public void onSet(Connection connection, Message.Response response) {

        if (currentPool == null) {
            // This should not happen but is probably not reason to fail completely
            logger.error("No current pool set; this should not happen");
        } else {
            currentPool.returnConnection(connection);
        }

        try {
            switch (response.type) {
                case RESULT:
                    setFinalResult(connection, response);
                    break;
                case ERROR:
                    ErrorMessage err = (ErrorMessage)response;
                    RetryPolicy.RetryDecision retry = null;
                    RetryPolicy retryPolicy = query.getRetryPolicy() == null
                                            ? manager.configuration().getPolicies().getRetryPolicy()
                                            : query.getRetryPolicy();
                    switch (err.error.code()) {
                        case READ_TIMEOUT:
                            assert err.error instanceof ReadTimeoutException;
                            if (manager.configuration().isMetricsEnabled())
                                metrics().getErrorMetrics().getReadTimeouts().inc();

                            ReadTimeoutException rte = (ReadTimeoutException)err.error;
                            ConsistencyLevel rcl = ConsistencyLevel.from(rte.consistency);
                            retry = retryPolicy.onReadTimeout(query, rcl, rte.blockFor, rte.received, rte.dataPresent, queryRetries);
                            break;
                        case WRITE_TIMEOUT:
                            assert err.error instanceof WriteTimeoutException;
                            if (manager.configuration().isMetricsEnabled())
                                metrics().getErrorMetrics().getWriteTimeouts().inc();

                            WriteTimeoutException wte = (WriteTimeoutException)err.error;
                            ConsistencyLevel wcl = ConsistencyLevel.from(wte.consistency);
                            retry = retryPolicy.onWriteTimeout(query, wcl, WriteType.from(wte.writeType), wte.blockFor, wte.received, queryRetries);
                            break;
                        case UNAVAILABLE:
                            assert err.error instanceof UnavailableException;
                            if (manager.configuration().isMetricsEnabled())
                                metrics().getErrorMetrics().getUnavailables().inc();

                            UnavailableException ue = (UnavailableException)err.error;
                            ConsistencyLevel ucl = ConsistencyLevel.from(ue.consistency);
                            retry = retryPolicy.onUnavailable(query, ucl, ue.required, ue.alive, queryRetries);
                            break;
                        case OVERLOADED:
                            // Try another node
                            logger.warn("Host {} is overloaded, trying next host.", connection.address);
                            logError(connection.address, "Host overloaded");
                            if (manager.configuration().isMetricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            retry(false, null);
                            return;
                        case IS_BOOTSTRAPPING:
                            // Try another node
                            logger.error("Query sent to {} but it is bootstrapping. This shouldn't happen but trying next host.", connection.address);
                            logError(connection.address, "Host is boostrapping");
                            if (manager.configuration().isMetricsEnabled())
                                metrics().getErrorMetrics().getOthers().inc();
                            retry(false, null);
                            return;
                        case UNPREPARED:
                            assert err.error instanceof PreparedQueryNotFoundException;
                            PreparedQueryNotFoundException pqnf = (PreparedQueryNotFoundException)err.error;
                            PreparedStatement toPrepare = manager.cluster.manager.preparedQueries.get(pqnf.id);
                            if (toPrepare == null) {
                                // This shouldn't happen
                                String msg = String.format("Tried to execute unknown prepared query %s", pqnf.id);
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
                            if (manager.configuration().isMetricsEnabled())
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
                                    logger.trace("Doing retry {} for query {} at consistency {}", new Object[]{ queryRetries, query, retry.getRetryConsistencyLevel()});
                                if (manager.configuration().isMetricsEnabled())
                                    metrics().getErrorMetrics().getRetries().inc();
                                retry(true, retry.getRetryConsistencyLevel());
                                break;
                            case RETHROW:
                                setFinalResult(connection, response);
                                break;
                            case IGNORE:
                                if (manager.configuration().isMetricsEnabled())
                                    metrics().getErrorMetrics().getIgnores().inc();
                                setFinalResult(connection, new ResultMessage.Void());
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
        }
    }

    private Connection.ResponseCallback prepareAndRetry(final String toPrepare) {
        return new Connection.ResponseCallback() {

            public Message.Request request() {
                return new PrepareMessage(toPrepare);
            }

            public void onSet(Connection connection, Message.Response response) {
                // TODO should we check the response ?
                switch (response.type) {
                    case RESULT:
                        if (((ResultMessage)response).kind == ResultMessage.Kind.PREPARED) {
                            logger.trace("Scheduling retry now that query is prepared");
                            retry(true, null);
                        } else {
                            logError(connection.address, "Got unexpected response to prepare message: " + response);
                            retry(false, null);
                        }
                        break;
                    case ERROR:
                        logError(connection.address, "Error preparing query, got " + response);
                        if (manager.configuration().isMetricsEnabled())
                            metrics().getErrorMetrics().getOthers().inc();
                        retry(false, null);
                        break;
                    default:
                        // Something's wrong, so we return but we let setFinalResult propagate the exception
                        RequestHandler.this.setFinalResult(connection, response);
                        break;
                }
            }

            public void onException(Connection connection, Exception exception) {
                RequestHandler.this.onException(connection, exception);
            }
        };
    }

    public void onException(Connection connection, Exception exception) {

        if (connection != null) {
            if (currentPool == null) {
                // This should not happen but is probably not reason to fail completely
                logger.error("No current pool set; this should not happen");
            } else {
                currentPool.returnConnection(connection);
            }
        }

        if (exception instanceof ConnectionException) {
            if (manager.configuration().isMetricsEnabled())
                metrics().getErrorMetrics().getConnectionErrors().inc();
            ConnectionException ce = (ConnectionException)exception;
            logError(ce.address, ce.getMessage());
            retry(false, null);
            return;
        }

        setFinalException(connection, exception);
    }

    interface Callback extends Connection.ResponseCallback {
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info);
    }
}
