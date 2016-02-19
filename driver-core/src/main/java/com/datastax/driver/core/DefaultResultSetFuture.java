/*
 *      Copyright (C) 2012-2015 DataStax Inc.
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

import com.datastax.driver.core.exceptions.*;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.datastax.driver.core.SchemaElement.KEYSPACE;

/**
 * Internal implementation of ResultSetFuture.
 */
class DefaultResultSetFuture extends AbstractFuture<ResultSet> implements ResultSetFuture, RequestHandler.Callback {

    private static final Logger logger = LoggerFactory.getLogger(ResultSetFuture.class);

    private final SessionManager session;
    private final ProtocolVersion protocolVersion;
    private final Message.Request request;
    private volatile RequestHandler handler;

    DefaultResultSetFuture(SessionManager session, ProtocolVersion protocolVersion, Message.Request request) {
        this.session = session;
        this.protocolVersion = protocolVersion;
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
    public void onSet(Connection connection, Message.Response response, ExecutionInfo info, Statement statement, long latency) {
        try {
            switch (response.type) {
                case RESULT:
                    Responses.Result rm = (Responses.Result) response;
                    switch (rm.kind) {
                        case SET_KEYSPACE:
                            // propagate the keyspace change to other connections
                            session.poolsState.setKeyspace(((Responses.Result.SetKeyspace) rm).keyspace);
                            set(ArrayBackedResultSet.fromMessage(rm, session, protocolVersion, info, statement));
                            break;
                        case SCHEMA_CHANGE:
                            ResultSet rs = ArrayBackedResultSet.fromMessage(rm, session, protocolVersion, info, statement);
                            final Cluster.Manager cluster = session.cluster.manager;
                            if (!cluster.configuration.getQueryOptions().isMetadataEnabled()) {
                                cluster.waitForSchemaAgreementAndSignal(connection, this, rs);
                            } else {
                                Responses.Result.SchemaChange scc = (Responses.Result.SchemaChange) rm;
                                switch (scc.change) {
                                    case CREATED:
                                    case UPDATED:
                                        cluster.refreshSchemaAndSignal(connection, this, rs, scc.targetType, scc.targetKeyspace, scc.targetName, scc.targetSignature);
                                        break;
                                    case DROPPED:
                                        if (scc.targetType == KEYSPACE) {
                                            // If that the one keyspace we are logged in, reset to null (it shouldn't really happen but ...)
                                            // Note: Actually, Cassandra doesn't do that so we don't either as this could confuse prepared statements.
                                            // We'll add it back if CASSANDRA-5358 changes that behavior
                                            //if (scc.keyspace.equals(session.poolsState.keyspace))
                                            //    session.poolsState.setKeyspace(null);
                                            final KeyspaceMetadata removedKeyspace = cluster.metadata.removeKeyspace(scc.targetKeyspace);
                                            if (removedKeyspace != null) {
                                                cluster.executor.submit(new Runnable() {
                                                    @Override
                                                    public void run() {
                                                        cluster.metadata.triggerOnKeyspaceRemoved(removedKeyspace);
                                                    }
                                                });
                                            }
                                        } else {
                                            KeyspaceMetadata keyspace = session.cluster.manager.metadata.keyspaces.get(scc.targetKeyspace);
                                            if (keyspace == null) {
                                                logger.warn("Received a DROPPED notification for {} {}.{}, but this keyspace is unknown in our metadata",
                                                        scc.targetType, scc.targetKeyspace, scc.targetName);
                                            } else {
                                                switch (scc.targetType) {
                                                    case TABLE:
                                                        // we can't tell whether it's a table or a view,
                                                        // but since two objects cannot have the same name,
                                                        // try removing both
                                                        final TableMetadata removedTable = keyspace.removeTable(scc.targetName);
                                                        if (removedTable != null) {
                                                            cluster.executor.submit(new Runnable() {
                                                                @Override
                                                                public void run() {
                                                                    cluster.metadata.triggerOnTableRemoved(removedTable);
                                                                }
                                                            });
                                                        } else {
                                                            final MaterializedViewMetadata removedView = keyspace.removeMaterializedView(scc.targetName);
                                                            if (removedView != null) {
                                                                cluster.executor.submit(new Runnable() {
                                                                    @Override
                                                                    public void run() {
                                                                        cluster.metadata.triggerOnMaterializedViewRemoved(removedView);
                                                                    }
                                                                });
                                                            }
                                                        }
                                                        break;
                                                    case TYPE:
                                                        final UserType removedType = keyspace.removeUserType(scc.targetName);
                                                        if (removedType != null) {
                                                            cluster.executor.submit(new Runnable() {
                                                                @Override
                                                                public void run() {
                                                                    cluster.metadata.triggerOnUserTypeRemoved(removedType);
                                                                }
                                                            });
                                                        }
                                                        break;
                                                    case FUNCTION:
                                                        final FunctionMetadata removedFunction = keyspace.removeFunction(Metadata.fullFunctionName(scc.targetName, scc.targetSignature));
                                                        if (removedFunction != null) {
                                                            cluster.executor.submit(new Runnable() {
                                                                @Override
                                                                public void run() {
                                                                    cluster.metadata.triggerOnFunctionRemoved(removedFunction);
                                                                }
                                                            });
                                                        }
                                                        break;
                                                    case AGGREGATE:
                                                        final AggregateMetadata removedAggregate = keyspace.removeAggregate(Metadata.fullFunctionName(scc.targetName, scc.targetSignature));
                                                        if (removedAggregate != null) {
                                                            cluster.executor.submit(new Runnable() {
                                                                @Override
                                                                public void run() {
                                                                    cluster.metadata.triggerOnAggregateRemoved(removedAggregate);
                                                                }
                                                            });
                                                        }
                                                        break;
                                                }
                                            }
                                        }
                                        session.cluster.manager.waitForSchemaAgreementAndSignal(connection, this, rs);
                                        break;
                                    default:
                                        logger.info("Ignoring unknown schema change result");
                                        break;
                                }
                            }
                            break;
                        default:
                            set(ArrayBackedResultSet.fromMessage(rm, session, protocolVersion, info, statement));
                            break;
                    }
                    break;
                case ERROR:
                    setException(((Responses.Error) response).asException(connection.address));
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
    public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
        // This is only called for internal calls (i.e, when the callback is not wrapped in ResponseHandler),
        // so don't bother with ExecutionInfo.
        onSet(connection, response, null, null, latency);
    }

    @Override
    public void onException(Connection connection, Exception exception, long latency, int retryCount) {
        setException(exception);
    }

    @Override
    public boolean onTimeout(Connection connection, long latency, int retryCount) {
        // This is only called for internal calls (i.e, when the future is not wrapped in RequestHandler).
        // So just set an exception for the final result, which should be handled correctly by said internal call.
        setException(new OperationTimedOutException(connection.address));
        return true;
    }

    // We sometimes need (in the driver) to set the future from outside this class,
    // but AbstractFuture#set is protected so this method. We don't want it public
    // however, no particular reason to give users rope to hang themselves.
    void setResult(ResultSet rs) {
        set(rs);
    }

    /**
     * Waits for the query to return and return its result.
     * <p/>
     * This method is usually more convenient than {@link #get} because it:
     * <ul>
     * <li>Waits for the result uninterruptibly, and so doesn't throw
     * {@link InterruptedException}.</li>
     * <li>Returns meaningful exceptions, instead of having to deal
     * with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to execute this query.
     * @throws QueryExecutionException  if the query triggered an execution
     *                                  exception, that is an exception thrown by Cassandra when it cannot execute
     *                                  the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query is invalid (syntax error,
     *                                  unauthorized or any other validation problem).
     */
    @Override
    public ResultSet getUninterruptibly() {
        try {
            return Uninterruptibles.getUninterruptibly(this);
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Waits for the provided time for the query to return and return its
     * result if available.
     * <p/>
     * This method is usually more convenient than {@link #get} because it:
     * <ul>
     * <li>Waits for the result uninterruptibly, and so doesn't throw
     * {@link InterruptedException}.</li>
     * <li>Returns meaningful exceptions, instead of having to deal
     * with ExecutionException.</li>
     * </ul>
     * As such, it is the preferred way to get the future result.
     *
     * @throws NoHostAvailableException if no host in the cluster can be
     *                                  contacted successfully to execute this query.
     * @throws QueryExecutionException  if the query triggered an execution
     *                                  exception, that is an exception thrown by Cassandra when it cannot execute
     *                                  the query with the requested consistency level successfully.
     * @throws QueryValidationException if the query if invalid (syntax error,
     *                                  unauthorized or any other validation problem).
     * @throws TimeoutException         if the wait timed out (Note that this is
     *                                  different from a Cassandra timeout, which is a {@code
     *                                  QueryExecutionException}).
     */
    @Override
    public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws TimeoutException {
        try {
            return Uninterruptibles.getUninterruptibly(this, timeout, unit);
        } catch (ExecutionException e) {
            throw DriverThrowables.propagateCause(e);
        }
    }

    /**
     * Attempts to cancel the execution of the request corresponding to this
     * future. This attempt will fail if the request has already returned.
     * <p/>
     * Please note that this only cancels the request driver side, but nothing
     * is done to interrupt the execution of the request Cassandra side (and that even
     * if {@code mayInterruptIfRunning} is true) since  Cassandra does not
     * support such interruption.
     * <p/>
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
     *       future.cancel(true); // Ensure any resource used by this query driver
     *                            // side is released immediately
     *       ... handle timeout ...
     *   }
     * <pre>
     *
     * @param mayInterruptIfRunning the value of this parameter is currently
     *                              ignored.
     * @return {@code false} if the future could not be cancelled (it has already
     * completed normally); {@code true} otherwise.
     */
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!super.cancel(mayInterruptIfRunning))
            return false;

        if (handler != null) {
            handler.cancel();
        }
        return true;
    }

    @Override
    public int retryCount() {
        // This is only called for internal calls (i.e, when the future is not wrapped in RequestHandler).
        // There is no retry logic in that case, so the value does not really matter.
        return 0;
    }
}
