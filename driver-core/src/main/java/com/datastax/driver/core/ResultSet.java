package com.datastax.driver.core;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ServerError;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.apache.cassandra.transport.messages.ResultMessage;

import com.datastax.driver.core.exceptions.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The result of a query.
 *
 * Note that this class is not thread-safe.
 */
public class ResultSet implements Iterable<CQLRow> {

    private static final Logger logger = LoggerFactory.getLogger(ResultSet.class);

    private static final Queue<List<ByteBuffer>> EMPTY_QUEUE = new ArrayDeque(0);
    private static final ResultSet EMPTY = new ResultSet(ColumnDefinitions.EMPTY, EMPTY_QUEUE, null, null);

    private final ColumnDefinitions metadata;
    private final Queue<List<ByteBuffer>> rows;
    private final QueryTrace trace;

    private final InetAddress queriedHost;

    private ResultSet(ColumnDefinitions metadata, Queue<List<ByteBuffer>> rows, QueryTrace trace, InetAddress queriedHost) {

        this.metadata = metadata;
        this.rows = rows;
        this.trace = trace;
        this.queriedHost = queriedHost;
    }

    private static ResultSet fromMessage(ResultMessage msg, Session.Manager session, InetAddress queriedHost) {

        UUID tracingId = msg.getTracingId();
        QueryTrace trace = tracingId == null ? null : new QueryTrace(tracingId, session);

        switch (msg.kind) {
            case VOID:
                return empty(trace, queriedHost);
            case ROWS:
                ResultMessage.Rows r = (ResultMessage.Rows)msg;
                ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[r.result.metadata.names.size()];
                for (int i = 0; i < defs.length; i++)
                    defs[i] = ColumnDefinitions.Definition.fromTransportSpecification(r.result.metadata.names.get(i));

                return new ResultSet(new ColumnDefinitions(defs), new ArrayDeque(r.result.rows), trace, queriedHost);
            case SET_KEYSPACE:
            case SCHEMA_CHANGE:
                return empty(trace, queriedHost);
            case PREPARED:
                throw new RuntimeException("Prepared statement received when a ResultSet was expected");
            default:
                logger.error("Received unknow result type '{}'; returning empty result set", msg.kind);
                return empty(trace, queriedHost);
        }
    }

    private static ResultSet empty(QueryTrace trace, InetAddress queriedHost) {
        return trace == null ? EMPTY : new ResultSet(ColumnDefinitions.EMPTY, EMPTY_QUEUE, trace, queriedHost);
    }

    // Note: we don't really want to expose this publicly, partly because we don't return it with empty result set.
    // But for now this is convenient for tests. We'll see later if we want another solution.
    InetAddress getQueriedHost() {
        return queriedHost;
    }

    /**
     * The columns returned in this ResultSet.
     *
     * @return the columns returned in this ResultSet.
     */
    public ColumnDefinitions getColumnDefinitions() {
        return metadata;
    }

    /**
     * Test whether this ResultSet has more results.
     *
     * @return whether this ResultSet has more results.
     */
    public boolean isExhausted() {
        return rows.isEmpty();
    }

    /**
     * Returns the the next result from this ResultSet.
     *
     * @return the next row in this resultSet or null if this ResultSet is
     * exhausted.
     */
    public CQLRow fetchOne() {
        return CQLRow.fromData(metadata, rows.poll());
    }

    /**
     * Returns all the remaining rows in this ResultSet as a list.
     *
     * @return a list containing the remaining results of this ResultSet. The
     * returned list is empty if and only the ResultSet is exhausted.
     */
    public List<CQLRow> fetchAll() {
        if (isExhausted())
            return Collections.emptyList();

        List<CQLRow> result = new ArrayList<CQLRow>(rows.size());
        for (CQLRow row : this)
            result.add(row);
        return result;
    }

    /**
     * An iterator over the rows contained in this ResultSet.
     *
     * The {@link Iterator#next} method is equivalent to calling {@link #fetchOne}.
     * So this iterator will consume results from this ResultSet and after a
     * full iteration, the ResultSet will be empty.
     *
     * The returned iterator does not support the {@link Iterator#remove} method.
     *
     * @return an iterator that will consume and return the remaining rows of
     * this ResultSet.
     */
    public Iterator<CQLRow> iterator() {
        return new Iterator<CQLRow>() {

            public boolean hasNext() {
                return !rows.isEmpty();
            }

            public CQLRow next() {
                return CQLRow.fromData(metadata, rows.poll());
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * The query trace if tracing was enabled on this query.
     *
     * @return the {@code QueryTrace} object for this query if tracing was
     * enable for this query, or {@code null} otherwise.
     */
    public QueryTrace getQueryTrace() {
        return trace;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ResultSet[ exhausted: ").append(isExhausted());
        sb.append(", ").append(metadata).append("]");
        return sb.toString();
    }

    public static class Future extends SimpleFuture<ResultSet>
    {
        private final Session.Manager session;
        private final Message.Request request;
        final ResponseCallback callback = new ResponseCallback();

        Future(Session.Manager session, Message.Request request) {
            this.session = session;
            this.request = request;
        }

        // The only reason this exists is because we don't want to expose its
        // method publicly (otherwise Future could have implemented
        // Connection.ResponseCallback directly)
        class ResponseCallback implements Connection.ResponseCallback {

            public Message.Request request() {
                return request;
            }

            public void onSet(Connection connection, Message.Response response) {
                try {
                    switch (response.type) {
                        case RESULT:
                            ResultMessage rm = (ResultMessage)response;
                            switch (rm.kind) {
                                case SET_KEYSPACE:
                                    // propagate the keyspace change to other connections
                                    session.poolsState.setKeyspace(((ResultMessage.SetKeyspace)rm).keyspace);
                                    set(ResultSet.fromMessage(rm, session, connection.address));
                                    break;
                                case SCHEMA_CHANGE:
                                    ResultMessage.SchemaChange scc = (ResultMessage.SchemaChange)rm;
                                    ResultSet rs = ResultSet.fromMessage(rm, session, connection.address);
                                    switch (scc.change) {
                                        case CREATED:
                                            if (scc.columnFamily.isEmpty()) {
                                                session.cluster.manager.refreshSchema(connection, Future.this, rs, null, null);
                                            } else {
                                                session.cluster.manager.refreshSchema(connection, Future.this, rs, scc.keyspace, null);
                                            }
                                            break;
                                        case DROPPED:
                                            if (scc.columnFamily.isEmpty()) {
                                                // If that the one keyspace we are logged in, reset to null (it shouldn't really happen but ...)
                                                if (scc.keyspace.equals(session.poolsState.keyspace))
                                                    session.poolsState.setKeyspace(null);
                                                session.cluster.manager.refreshSchema(connection, Future.this, rs, null, null);
                                            } else {
                                                session.cluster.manager.refreshSchema(connection, Future.this, rs, scc.keyspace, null);
                                            }
                                            break;
                                        case UPDATED:
                                            if (scc.columnFamily.isEmpty()) {
                                                session.cluster.manager.refreshSchema(connection, Future.this, rs, scc.keyspace, null);
                                            } else {
                                                session.cluster.manager.refreshSchema(connection, Future.this, rs, scc.keyspace, scc.columnFamily);
                                            }
                                            break;
                                    }
                                    break;
                                default:
                                    set(ResultSet.fromMessage(rm, session, connection.address));
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

            public void onException(Connection connection, Exception exception) {
                setException(exception);
            }
        }

        /**
         * Waits for the query to return and return its result.
         *
         * This method is usually more convenient than {@link #get} as it:
         * <ul>
         *   <li>It waits for the result uninterruptibly, and so doesn't throw
         *   {@link InterruptedException}.</li>
         *   <li>It returns meaningful exceptions, instead of having to deal
         *   with ExecutionException.</li>
         * </ul>
         * As such, it is the preferred way to get the future result.
         *
         * @throws NoHostAvailableException if no host in the cluster can be
         * contacted successfully to execute this query.
         * @throws QueryExecutionException if the query triggered an execution
         * exception, i.e. an exception thrown by Cassandra when it cannot execute
         * the query with the requested consistency level successfully.
         * @throws QueryValidationException if the query if invalid (syntax error,
         * unauthorized or any other validation problem).
         */
        public ResultSet getUninterruptibly() throws NoHostAvailableException {
            try {
                while (true) {
                    try {
                        return super.get();
                    } catch (InterruptedException e) {
                        // We said 'uninterruptibly'
                    }
                }
            } catch (ExecutionException e) {
                extractCauseFromExecutionException(e);
                throw new AssertionError();
            }
        }

        /**
         * Waits for the given time for the query to return and return its
         * result if available.
         *
         * This method is usually more convenient than {@link #get} as it:
         * <ul>
         *   <li>It waits for the result uninterruptibly, and so doesn't throw
         *   {@link InterruptedException}.</li>
         *   <li>It returns meaningful exceptions, instead of having to deal
         *   with ExecutionException.</li>
         * </ul>
         * As such, it is the preferred way to get the future result.
         *
         * @throws NoHostAvailableException if no host in the cluster can be
         * contacted successfully to execute this query.
         * @throws QueryExecutionException if the query triggered an execution
         * exception, i.e. an exception thrown by Cassandra when it cannot execute
         * the query with the requested consistency level successfully.
         * @throws QueryValidationException if the query if invalid (syntax error,
         * unauthorized or any other validation problem).
         * @throws TimeoutException if the wait timed out (Note that this is
         * different from a Cassandra timeout, which is a {@code
         * QueryExecutionException}).
         */
        public ResultSet getUninterruptibly(long timeout, TimeUnit unit) throws NoHostAvailableException, TimeoutException {
            long start = System.nanoTime();
            long timeoutNanos = unit.toNanos(timeout);
            try {
                while (true) {
                    try {
                        return super.get(timeoutNanos, TimeUnit.NANOSECONDS);
                    } catch (InterruptedException e) {
                        // We said 'uninterruptibly'
                        long now = System.nanoTime();
                        long elapsedNanos = now - start;
                        timeout = timeoutNanos - elapsedNanos;
                        start = now;
                    }
                }
            } catch (ExecutionException e) {
                extractCauseFromExecutionException(e);
                throw new AssertionError();
            }
        }

        static void extractCauseFromExecutionException(ExecutionException e) throws NoHostAvailableException {
            extractCause(e.getCause());
        }

        static void extractCause(Throwable cause) throws NoHostAvailableException {
            if (cause instanceof NoHostAvailableException)
                throw (NoHostAvailableException)cause;
            else if (cause instanceof QueryExecutionException)
                throw (QueryExecutionException)cause;
            else if (cause instanceof DriverUncheckedException)
                throw (DriverUncheckedException)cause;
            else
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
}
