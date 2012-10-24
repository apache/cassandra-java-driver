package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.configuration.RetryPolicy;
import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.utils.SimpleFuture;

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

    private volatile int queryRetries;
    private volatile ConsistencyLevel retryConsistencyLevel;

    private volatile Map<InetSocketAddress, String> errors;

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
        callback.onException(new NoHostAvailableException(errors == null ? Collections.<InetSocketAddress, String>emptyMap() : errors));
    }

    private boolean query(Host host) {
        HostConnectionPool pool = manager.pools.get(host);
        if (pool == null || pool.isShutdown())
            return false;

        try {
            Connection connection = pool.borrowConnection(manager.DEFAULT_PER_HOST_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
            current = host;
            try {
                connection.write(this);
                return true;
            } finally {
                pool.returnConnection(connection);
            }
        } catch (ConnectionException e) {
            // If we have any problem with the connection, move to the next node.
            logError(host.getAddress(), e.getMessage());
            return false;
        } catch (TimeoutException e) {
            // We timeout, log it but move to the next node.
            logError(host.getAddress(), "Timeout while trying to acquire available connection");
            return false;
        }
    }

    private void logError(InetSocketAddress address, String msg) {
        logger.debug(String.format("Error querying %s, trying next host (error is: %s)", address, msg));
        if (errors == null)
            errors = new HashMap<InetSocketAddress, String>();
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
                        // TODO: log error as this shouldn't happen
                        // Try another node
                        logger.error("Query sent to %s but it is bootstrapping. This shouldn't happen but trying next host.", connection.address);
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
                            callback.onException(new DriverInternalError(msg));
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
                switch (retry.getType()) {
                    case RETRY:
                        ++queryRetries;
                        retry(true, retry.getRetryConsistencyLevel());
                        break;
                    case RETHROW:
                        callback.onSet(connection, response);
                        break;
                    case IGNORE:
                        callback.onSet(connection, ResultMessage.Void.instance());
                        break;
                }
                break;
            default:
                callback.onSet(connection, response);
                break;
        }
    }

    public void onException(Exception exception) {

        if (exception instanceof ConnectionException) {
            ConnectionException ce = (ConnectionException)exception;
            logError(ce.address, ce.getMessage());
            retry(false, null);
            return;
        }

        callback.onException(exception);
    }

}
