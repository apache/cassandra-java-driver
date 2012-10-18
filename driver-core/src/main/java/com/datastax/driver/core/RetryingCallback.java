package com.datastax.driver.core;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.exceptions.*;
import com.datastax.driver.core.utils.SimpleFuture;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
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

    private final boolean isQuery;
    private volatile int queryRetries;

    private volatile Map<InetSocketAddress, String> errors;

    public RetryingCallback(Session.Manager manager, Connection.ResponseCallback callback) {
        this.manager = manager;
        this.callback = callback;

        this.queryPlan = manager.loadBalancer.newQueryPlan();
        this.isQuery = request() instanceof QueryMessage || request() instanceof ExecuteMessage;
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

    private void retry(final boolean retryCurrent) {
        final Host h = current;

        // We should not retry on the current thread as this will be an IO thread.
        manager.cluster.manager.executor.execute(new Runnable() {
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
        return callback.request();
    }

    public void onSet(Connection connection, Message.Response response) {
        switch (response.type) {
            case RESULT:
                callback.onSet(connection, response);
                break;
            case ERROR:
                ErrorMessage err = (ErrorMessage)response;
                boolean retry = false;
                switch (err.error.code()) {
                    case READ_TIMEOUT:
                        assert err.error instanceof ReadTimeoutException;
                        ReadTimeoutException rte = (ReadTimeoutException)err.error;
                        ConsistencyLevel rcl = ConsistencyLevel.from(rte.consistency);
                        retry = manager.retryPolicy.onReadTimeout(rcl, rte.received, rte.blockFor, rte.dataPresent, queryRetries);
                        break;
                    case WRITE_TIMEOUT:
                        assert err.error instanceof WriteTimeoutException;
                        WriteTimeoutException wte = (WriteTimeoutException)err.error;
                        ConsistencyLevel wcl = ConsistencyLevel.from(wte.consistency);
                        retry = manager.retryPolicy.onWriteTimeout(wcl, wte.received, wte.blockFor, queryRetries);
                        break;
                    case UNAVAILABLE:
                        assert err.error instanceof UnavailableException;
                        UnavailableException ue = (UnavailableException)err.error;
                        ConsistencyLevel ucl = ConsistencyLevel.from(ue.consistency);
                        retry = manager.retryPolicy.onUnavailable(ucl, ue.required, ue.alive, queryRetries);
                        break;
                    case OVERLOADED:
                        // TODO: maybe we could make that part of the retrying policy?
                        if (queryRetries == 0)
                            retry = true;
                        break;
                    case IS_BOOTSTRAPPING:
                        // TODO: log error as this shouldn't happen
                        // retry once
                        if (queryRetries == 0)
                            retry = true;
                        break;
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
                            retry = true;
                        } catch (InterruptedException e) {
                            logError(connection.address, "Interrupted while preparing query to execute");
                            retry(false);
                            return;
                        } catch (ExecutionException e) {
                            logError(connection.address, "Unexpected problem while preparing query to execute: " + e.getCause().getMessage());
                            retry(false);
                            return;
                        } catch (ConnectionException e) {
                            logger.debug("Connection exception while preparing missing statement", e);
                            logError(e.address, e.getMessage());
                            retry(false);
                            return;
                        }
                }
                if (retry) {
                    ++queryRetries;
                    retry(true);
                } else {
                    callback.onSet(connection, response);
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
            retry(false);
            return;
        }

        callback.onException(exception);
    }

}
