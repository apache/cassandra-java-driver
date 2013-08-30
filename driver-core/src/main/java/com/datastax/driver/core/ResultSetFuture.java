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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.datastax.driver.core.exceptions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A future on a {@link ResultSet}.
 *
 * Note that this class implements <a href="http://code.google.com/p/guava-libraries/">Guava</a>'s {@code
 * ListenableFuture} and can so be used with Guava's future utilities.
 */
public class ResultSetFuture extends SimpleFuture<ResultSet> {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetFuture.class);

    private final Session.Manager session;
    final ResponseCallback callback;

    ResultSetFuture(Session.Manager session, Message.Request request) {
        this.session = session;
        this.callback = new ResponseCallback(request);
    }

    // The reason this exists is because we don't want to expose its method
    // publicly (otherwise Future could implement RequestHandler.Callback directly)
    class ResponseCallback implements RequestHandler.Callback {

        private final Message.Request request;
        private volatile RequestHandler handler;

        ResponseCallback(Message.Request request) {
            this.request = request;
        }

        @Override
        public void register(RequestHandler handler) {
            this.handler = handler;
        }

        @Override
        public Message.Request request() {
            return request;
        }

        @Override
        public void onSet(Connection connection, Message.Response response, ExecutionInfo info, long latency) {
            try {
                switch (response.type) {
                    case RESULT:
                        ResultMessage rm = (ResultMessage)response;
                        switch (rm.kind) {
                            case SET_KEYSPACE:
                                // propagate the keyspace change to other connections
                                session.poolsState.setKeyspace(((ResultMessage.SetKeyspace)rm).keyspace);
                                set(ResultSet.fromMessage(rm, session, info));
                                break;
                            case SCHEMA_CHANGE:
                                ResultMessage.SchemaChange scc = (ResultMessage.SchemaChange)rm;
                                ResultSet rs = ResultSet.fromMessage(rm, session, info);
                                switch (scc.change) {
                                    case CREATED:
                                        if (scc.columnFamily.isEmpty()) {
                                            session.cluster.manager.refreshSchema(connection, ResultSetFuture.this, rs, null, null);
                                        } else {
                                            session.cluster.manager.refreshSchema(connection, ResultSetFuture.this, rs, scc.keyspace, null);
                                        }
                                        break;
                                    case DROPPED:
                                        if (scc.columnFamily.isEmpty()) {
                                            // If that the one keyspace we are logged in, reset to null (it shouldn't really happen but ...)
                                            // Note: Actually, Cassandra doesn't do that so we don't either as this could confuse prepared statements.
                                            // We'll add it back if CASSANDRA-5358 changes that behavior
                                            //if (scc.keyspace.equals(session.poolsState.keyspace))
                                            //    session.poolsState.setKeyspace(null);
                                            session.cluster.manager.refreshSchema(connection, ResultSetFuture.this, rs, null, null);
                                        } else {
                                            session.cluster.manager.refreshSchema(connection, ResultSetFuture.this, rs, scc.keyspace, null);
                                        }
                                        break;
                                    case UPDATED:
                                        if (scc.columnFamily.isEmpty()) {
                                            session.cluster.manager.refreshSchema(connection, ResultSetFuture.this, rs, scc.keyspace, null);
                                        } else {
                                            session.cluster.manager.refreshSchema(connection, ResultSetFuture.this, rs, scc.keyspace, scc.columnFamily);
                                        }
                                        break;
                                    default:
                                        logger.info("Ignoring unknown schema change result");
                                        break;
                                }
                                break;
                            default:
                                set(ResultSet.fromMessage(rm, session, info));
                                break;
                        }
                        break;
                    case ERROR:
                        setException(convertException(((ErrorMessage)response).error));
                        break;
                    default:
                        // This mean we have probably have a bad node, so defunct the connection
                        connection.defunct(new ConnectionException(connection.address, String.format("Got unexpected %s response", response.type)));
                        setException(new DriverInternalError(String.format("Got unexpected %s response from %s", response.type, connection.address)));
                        break;
                }
            } catch (RuntimeException e) {
                // If we get a bug here, the client will not get it, so better forwarding the error
                setException(new DriverInternalError("Unexpected error while processing response from " + connection.address, e));
            }
        }

        @Override
        public void onSet(Connection connection, Message.Response response, long latency) {
            // This is only called for internal calls (i.e, when the callback is not wrapped in ResponseHandler),
            // so don't bother with ExecutionInfo.
            onSet(connection, response, null, latency);
        }

        @Override
        public void onException(Connection connection, Exception exception, long latency) {
            setException(exception);
        }

        @Override
        public void onTimeout(Connection connection, long latency) {
            // This is only called for internal calls (i.e, when the callback is not wrapped in ResponseHandler).
            // So just set an exception for the final result, which should be handled correctly by said internal call.
            setException(new ConnectionException(connection.address, "Operation Timeouted"));
        }
    }

    /**
     * Waits for the query to return and return its result.
     *
     * This method is usually more convenient than {@link #get} because it:
     * <ul>
     *   <li>Waits for the result uninterruptibly, and so doesn't throw
     *   {@link InterruptedException}.</li>
     *   <li>Returns meaningful exceptions, instead of having to deal
     *   with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, that is an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query is invalid (syntax error,
     * unauthorized or any other validation problem).
     */
    public ResultSet getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            extractCauseFromExecutionException(e);
            throw new AssertionError();
        }
    }

    /**
     * Waits for the provided time for the query to return and return its
     * result if available.
     *
     * This method is usually more convenient than {@link #get} because it:
     * <ul>
     *   <li>Waits for the result uninterruptibly, and so doesn't throw
     *   {@link InterruptedException}.</li>
     *   <li>Returns meaningful exceptions, instead of having to deal
     *   with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     * contacted successfully to execute this query.
     * @throws QueryExecutionException if the query triggered an execution
     * exception, that is an exception thrown by Cassandra when it cannot execute
     * the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     * unauthorized or any other validation problem).
     * @throws TimeoutException if the wait timed out (Note that this is
     * different from a Cassandra timeout, which is a {@code
     * QueryExecutionException}).
     */
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(this, timeout, unit);
        } catch (ExecutionException e) {
            extractCauseFromExecutionException(e);
            throw new AssertionError();
        }
    }

    /**
     * Attempts to cancel the execution of the request corresponding to this
     * future. This attempt will fail if the request has already returned.
     * <p>
     * Please note that this only cancle the request driver side, but nothing
     * is done to interrupt the execution of the request Cassandra side (and that even
     * if {@code mayInterruptIfRunning} is true) since  Cassandra does not
     * support such interruption.
     * <p>
     * This method can be used to ensure no more work is performed driver side
     * (which, while it doesn't include stopping a request already submitted
     * to a Cassandra node, may include not retrying another Cassandra host on
     * failure/timeout) if the ResultSet is not going to be retried. Typically,
     * the code to wait for a request result for a maximum of 1 second could
     * look like:
     * <pre>
     *   ResultSetFuture future = session.executeAsync(...some query...);
     *   try {
     *       ResultSet result = future.get(1, TimeUnit.SECONDS);
     *       ... process result ...
     *   } catch (TimeoutException e) {
     *       future.cancel(true); // Ensure any ressource used by this query driver
     *                            // side is released immediately
     *       ... handle timeout ...
     *   }
     * <pre>
     *
     * @param mayInterruptIfRunning the value of this parameter is currently
     * ignored.
     * @return {@code false} if the future could not be cancelled (it has already
     * completed normally); {@code true} otherwise.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!super.cancel(mayInterruptIfRunning))
            return false;

        callback.handler.cancel();
        return true;
    }

    static void extractCauseFromExecutionException(ExecutionException e) {
        // We could just rethrow e.getCause(). However, the cause of the ExecutionException has likely been
        // created on the I/O thread receiving the response. Which means that the stacktrace associated
        // with said cause will make no mention of the current thread. This is painful for say, finding
        // out which execute() statement actually raised the exception. So instead, we re-create the
        // exception.
        if (e.getCause() instanceof DriverException)
            throw ((DriverException)e.getCause()).copy();
        else
            throw new DriverInternalError("Unexpected exception thrown", e.getCause());
    }

    static void extractCause(Throwable cause) {
        // Same as above
        if (cause instanceof DriverException)
            throw ((DriverException)cause).copy();
        throw new DriverInternalError("Unexpected exception thrown", cause);
    }

    static Exception convertException(org.apache.cassandra.exceptions.TransportException te) {
        switch (te.code()) {
            case SERVER_ERROR:
                return new DriverInternalError("An unexpected error occured server side: " + te.getMessage());
            case PROTOCOL_ERROR:
                return new DriverInternalError("An unexpected protocol error occured. This is a bug in this library, please report: " + te.getMessage());
            case UNAVAILABLE:
                org.apache.cassandra.exceptions.UnavailableException ue = (org.apache.cassandra.exceptions.UnavailableException)te;
                return new UnavailableException(ConsistencyLevel.from(ue.consistency), ue.required, ue.alive);
            case OVERLOADED:
                return new DriverInternalError("Queried host was overloaded; this shouldn't happen, another node should have been tried");
            case IS_BOOTSTRAPPING:
                return new DriverInternalError("Queried host was boostrapping; this shouldn't happen, another node should have been tried");
            case TRUNCATE_ERROR:
                return new TruncateException(te.getMessage());
            case WRITE_TIMEOUT:
                org.apache.cassandra.exceptions.WriteTimeoutException wte = (org.apache.cassandra.exceptions.WriteTimeoutException)te;
                return new WriteTimeoutException(ConsistencyLevel.from(wte.consistency), WriteType.from(wte.writeType), wte.received, wte.blockFor);
            case READ_TIMEOUT:
                org.apache.cassandra.exceptions.ReadTimeoutException rte = (org.apache.cassandra.exceptions.ReadTimeoutException)te;
                return new ReadTimeoutException(ConsistencyLevel.from(rte.consistency), rte.received, rte.blockFor, rte.dataPresent);
            case SYNTAX_ERROR:
                return new SyntaxError(te.getMessage());
            case UNAUTHORIZED:
                return new UnauthorizedException(te.getMessage());
            case INVALID:
                return new InvalidQueryException(te.getMessage());
            case CONFIG_ERROR:
                return new InvalidConfigurationInQueryException(te.getMessage());
            case ALREADY_EXISTS:
                org.apache.cassandra.exceptions.AlreadyExistsException aee = (org.apache.cassandra.exceptions.AlreadyExistsException)te;
                return new AlreadyExistsException(aee.ksName, aee.cfName);
            default:
                return new DriverInternalError("Unknown error return code: " + te.code());
        }
    }
}
