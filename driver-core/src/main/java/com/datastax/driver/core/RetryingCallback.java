package com.datastax.driver.core;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection callback that handle retrying another node if the connection fails.
 *
 * For queries, this also handle retrying the query if the RetryPolicy say so.
 */
class RetryingCallback implements Connection.ResponseCallback {

    private static final Logger logger = LoggerFactory.getLogger(RetryingCallback.class);

    private final Session.Manager manager;
    private final Connection.ResponseCallback callback;

    private final Iterator<Host> queryPlan;
    private volatile Host current;
    private volatile HostConnectionPool currentPool;

    private volatile int queryRetries;
    private volatile ConsistencyLevel retryConsistencyLevel;

    private volatile Map<InetAddress, String> errors;

    public RetryingCallback(Session.Manager manager, Connection.ResponseCallback callback, QueryOptions queryOptions) {
        this.manager = manager;
        this.callback = callback;

        this.queryPlan = manager.loadBalancer.newQueryPlan(queryOptions);
    }

    public void sendRequest() {

        while (queryPlan.hasNext()) {
            Host host = queryPlan.next();
            if (query(host))
                return;
        }
        callback.onException(null, new NoHostAvailableException(errors == null ? Collections.<InetAddress, String>emptyMap() : errors));
    }

    private boolean query(Host host) {
        currentPool = manager.pools.get(host);
        if (currentPool == null || currentPool.isShutdown())
            return false;

        Connection connection = null;
        try {
            // Note: this is not perfectly correct to use getConnectTimeoutMillis(), but
            // until we provide a more fancy to control query timeouts, it's not a bad solution either
            connection = currentPool.borrowConnection(manager.configuration().getConnectionsConfiguration().getSocketOptions().getConnectTimeoutMillis(), TimeUnit.MILLISECONDS);
            current = host;
            connection.write(this);
            return true;
        } catch (ConnectionException e) {
            // If we have any problem with the connection, move to the next node.
            currentPool.returnConnection(connection);
            logError(host.getAddress(), e.getMessage());
            return false;
        } catch (BusyConnectionException e) {
            // The pool shoudln't have give us a busy connection unless we've maxed up the pool, so move on to the next host.
            currentPool.returnConnection(connection);
            logError(host.getAddress(), e.getMessage());
            return false;
        } catch (TimeoutException e) {
            // We timeout, log it but move to the next node.
            currentPool.returnConnection(connection);
            logError(host.getAddress(), "Timeout while trying to acquire available connection");
            currentPool.returnConnection(connection);
            return false;
        } catch (RuntimeException e) {
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
                    callback.onSet(connection, response);
                    break;
                case ERROR:
                    ErrorMessage err = (ErrorMessage)response;
                    RetryPolicy.RetryDecision retry = null;
                    RetryPolicy retryPolicy = manager.configuration().getPolicies().getRetryPolicy();
                    switch (err.error.code()) {
                        case READ_TIMEOUT:
                            assert err.error instanceof ReadTimeoutException;
                            ReadTimeoutException rte = (ReadTimeoutException)err.error;
                            ConsistencyLevel rcl = ConsistencyLevel.from(rte.consistency);
                            retry = retryPolicy.onReadTimeout(rcl, rte.received, rte.blockFor, rte.dataPresent, queryRetries);
                            break;
                        case WRITE_TIMEOUT:
                            assert err.error instanceof WriteTimeoutException;
                            WriteTimeoutException wte = (WriteTimeoutException)err.error;
                            ConsistencyLevel wcl = ConsistencyLevel.from(wte.consistency);
                            retry = retryPolicy.onWriteTimeout(wcl, WriteType.from(wte.writeType), wte.received, wte.blockFor, queryRetries);
                            break;
                        case UNAVAILABLE:
                            assert err.error instanceof UnavailableException;
                            UnavailableException ue = (UnavailableException)err.error;
                            ConsistencyLevel ucl = ConsistencyLevel.from(ue.consistency);
                            retry = retryPolicy.onUnavailable(ucl, ue.required, ue.alive, queryRetries);
                            break;
                        case OVERLOADED:
                            // Try another node
                            retry(false, null);
                            return;
                        case IS_BOOTSTRAPPING:
                            // Try another node
                            logger.error("Query sent to {} but it is bootstrapping. This shouldn't happen but trying next host.", connection.address);
                            retry(false, null);
                            return;
                        case UNPREPARED:
                            assert err.error instanceof PreparedQueryNotFoundException;
                            PreparedQueryNotFoundException pqnf = (PreparedQueryNotFoundException)err.error;
                            String toPrepare = manager.cluster.manager.preparedQueries.get(pqnf.id);
                            if (toPrepare == null) {
                                // This shouldn't happen
                                String msg = String.format("Tried to execute unknown prepared query %s", pqnf.id);
                                logger.error(msg);
                                callback.onException(connection, new DriverInternalError(msg));
                                return;
                            }

                            try {
                                Message.Response prepareResponse = connection.write(new PrepareMessage(toPrepare)).get();
                                // TODO check return ?
                                retry = RetryPolicy.RetryDecision.retry(null);
                            } catch (InterruptedException e) {
                                logError(connection.address, "Interrupted while preparing query to execute");
                                retry(false, null);
                                return;
                            } catch (ExecutionException e) {
                                logError(connection.address, "Unexpected problem while preparing query to execute: " + e.getCause().getMessage());
                                retry(false, null);
                                return;
                            } catch (ConnectionException e) {
                                logger.debug("Connection exception while preparing missing statement", e);
                                logError(e.address, e.getMessage());
                                retry(false, null);
                                return;
                            }
                    }

                    if (retry == null)
                        callback.onSet(connection, response);
                    else {
                        switch (retry.getType()) {
                            case RETRY:
                                ++queryRetries;
                                retry(true, retry.getRetryConsistencyLevel());
                                break;
                            case RETHROW:
                                callback.onSet(connection, response);
                                break;
                            case IGNORE:
                                callback.onSet(connection, new ResultMessage.Void());
                                break;
                        }
                    }
                    break;
                default:
                    callback.onSet(connection, response);
                    break;
            }
        } catch (Exception e) {
            callback.onException(connection, e);
        }
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
            ConnectionException ce = (ConnectionException)exception;
            logError(ce.address, ce.getMessage());
            retry(false, null);
            return;
        }

        callback.onException(connection, exception);
    }

}
