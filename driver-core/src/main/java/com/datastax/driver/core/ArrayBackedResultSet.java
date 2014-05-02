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
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.exceptions.DriverInternalError;

/**
 * Default implementation of a result set, backed by an ArrayDeque of ArrayList.
 */
class ArrayBackedResultSet implements ResultSet {

    private static final Logger logger = LoggerFactory.getLogger(ResultSet.class);

    private static final Queue<List<ByteBuffer>> EMPTY_QUEUE = new ArrayDeque<List<ByteBuffer>>(0);

    private final ColumnDefinitions metadata;
    private final Queue<List<ByteBuffer>> rows;

    private final List<ExecutionInfo> infos;

    /*
     * The fetching state of this result set. The fetchState will always be in one of
     * the 3 following state:
     *   1) fetchState is null or reference a null: fetching is done, there
     *      is nothing more to fetch and no query in progress.
     *   2) fetchState.get().nextStart is not null: there is more pages to fetch. In
     *      that case, inProgress is *guaranteed* to be null.
     *   3) fetchState.get().inProgress is not null: a page is being fetched.
     *      In that case, nextStart is *guaranteed* to be null.
     *
     * Also note that while ResultSet doesn't pretend to be thread-safe, the actual
     * fetch is done asynchronously and so we do need to be volatile below.
     */
    private volatile FetchingState fetchState;
    // The two following info can be null, but only if fetchState == null
    private final SessionManager session;
    private final Statement statement;

    private ArrayBackedResultSet(ColumnDefinitions metadata,
                                 Queue<List<ByteBuffer>> rows,
                                 ExecutionInfo info,
                                 ByteBuffer initialPagingState,
                                 SessionManager session,
                                 Statement statement) {
        this.metadata = metadata;
        this.rows = rows;
        this.session = session;

        if (initialPagingState == null) {
            this.fetchState = null;
            this.infos = Collections.<ExecutionInfo>singletonList(info);
        } else {
            this.fetchState = new FetchingState(initialPagingState, null);
            this.infos = new ArrayList<ExecutionInfo>();
            this.infos.add(info);
        }

        this.statement = statement;
        assert fetchState == null || (session != null && statement != null);
    }

    static ArrayBackedResultSet fromMessage(Responses.Result msg, SessionManager session, ExecutionInfo info, Statement statement) {

        UUID tracingId = msg.getTracingId();
        info = tracingId == null || info == null ? info : info.withTrace(new QueryTrace(tracingId, session));

        switch (msg.kind) {
            case VOID:
                return empty(info);
            case ROWS:
                Responses.Result.Rows r = (Responses.Result.Rows)msg;

                ColumnDefinitions columnDefs;
                if (r.metadata.columns == null) {
                    assert statement instanceof BoundStatement;
                    columnDefs = ((BoundStatement)statement).statement.getPreparedId().resultSetMetadata;
                    assert columnDefs != null;
                } else {
                    columnDefs = r.metadata.columns;
                }

                return new ArrayBackedResultSet(columnDefs, r.data, info, r.metadata.pagingState, session, statement);
            case SET_KEYSPACE:
            case SCHEMA_CHANGE:
                return empty(info);
            case PREPARED:
                throw new RuntimeException("Prepared statement received when a ResultSet was expected");
            default:
                logger.error("Received unknown result type '{}'; returning empty result set", msg.kind);
                return empty(info);
        }
    }

    private static ArrayBackedResultSet empty(ExecutionInfo info) {
        return new ArrayBackedResultSet(ColumnDefinitions.EMPTY, EMPTY_QUEUE, info, null, null, null);
    }

    public ColumnDefinitions getColumnDefinitions()
    {
        return metadata;
    }

    public boolean isExhausted() {
        if (!rows.isEmpty())
            return false;

        // This is slightly tricky: the fact that we do paged fetches underneath
        // should be completely transparent, so we can't answer false until
        // we've actually fetch the next results, since there is not absolute
        // guarantee that this won't come back empty.
        fetchMoreResultsBlocking();

        // ResultSet is *not* thread-safe, so either the last fetch has
        // returned result, or we should be done with fetching
        assert !rows.isEmpty() || isFullyFetched();
        return rows.isEmpty();
    }

    public Row one() {
        List<ByteBuffer> nextRow = rows.poll();
        if (nextRow != null)
            return ArrayBackedRow.fromData(metadata, nextRow);

        fetchMoreResultsBlocking();
        return ArrayBackedRow.fromData(metadata, rows.poll());
    }

    public List<Row> all() {
        if (isExhausted())
            return Collections.emptyList();

        // The result may have more that rows.size() if there is some page fetching
        // going on, but it won't be less and it the most common case where we don't page.
        List<Row> result = new ArrayList<Row>(rows.size());
        for (Row row : this)
            result.add(row);
        return result;
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {

            @Override
            public boolean hasNext() {
                return !isExhausted();
            }

            @Override
            public Row next() {
                return ArrayBackedResultSet.this.one();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public int getAvailableWithoutFetching() {
        return rows.size();
    }

    public boolean isFullyFetched() {
        return fetchState == null;
    }

    private void fetchMoreResultsBlocking() {
        try {
            Uninterruptibles.getUninterruptibly(fetchMoreResults());
        } catch (ExecutionException e) {
            throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    public ListenableFuture<Void> fetchMoreResults() {
        if (isFullyFetched())
            return Futures.immediateFuture(null);

        ListenableFuture<Void> inProgress = fetchState.inProgress;
        if (inProgress != null)
            return inProgress;

        assert fetchState.nextStart != null;
        ByteBuffer state = fetchState.nextStart;
        SettableFuture<Void> future = SettableFuture.create();
        fetchState = new FetchingState(null, future);
        return queryNextPage(state, future);
    }

    private ListenableFuture<Void> queryNextPage(ByteBuffer nextStart, final SettableFuture<Void> future) {

        assert !(statement instanceof BatchStatement);

        final Message.Request request = session.makeRequestMessage(statement, nextStart);
        session.execute(new RequestHandler.Callback() {

            @Override
            public Message.Request request() {
                return request;
            }

            @Override
            public void register(RequestHandler handler) {
            }

            @Override
            public void onSet(Connection connection, Message.Response response, ExecutionInfo info, Statement statement, long latency) {
                try {
                    switch (response.type) {
                        case RESULT:
                            Responses.Result rm = (Responses.Result)response;
                            // If we're paging, the query was a SELECT, so we don't have to handle SET_KEYSPACE and SCHEMA_CHANGE really
                            ArrayBackedResultSet tmp = ArrayBackedResultSet.fromMessage(rm, ArrayBackedResultSet.this.session, info, statement);

                            ArrayBackedResultSet.this.rows.addAll(tmp.rows);
                            ArrayBackedResultSet.this.fetchState = tmp.fetchState;
                            ArrayBackedResultSet.this.infos.addAll(tmp.infos);
                            future.set(null);
                            break;
                        case ERROR:
                            future.setException(((Responses.Error)response).asException(connection.address));
                            break;
                        default:
                            // This mean we have probably have a bad node, so defunct the connection
                            connection.defunct(new ConnectionException(connection.address, String.format("Got unexpected %s response", response.type)));
                            future.setException(new DriverInternalError(String.format("Got unexpected %s response from %s", response.type, connection.address)));
                            break;
                    }
                } catch (RuntimeException e) {
                    // If we get a bug here, the client will not get it, so better forwarding the error
                    future.setException(new DriverInternalError("Unexpected error while processing response from " + connection.address, e));
                }
            }

            // This is only called for internal calls, so don't bother with ExecutionInfo
            @Override
            public void onSet(Connection connection, Message.Response response, long latency) {
                onSet(connection, response, null, null, latency);
            }

            @Override
            public void onException(Connection connection, Exception exception, long latency) {
                future.setException(exception);
            }

            @Override
            public void onTimeout(Connection connection, long latency) {
                // This won't be called directly since this will be wrapped by RequestHandler.
                throw new UnsupportedOperationException();
            }

        }, statement);

        return future;
    }

    public ExecutionInfo getExecutionInfo() {
        return infos.get(infos.size() - 1);
    }

    public List<ExecutionInfo> getAllExecutionInfo() {
        return new ArrayList<ExecutionInfo>(infos);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ResultSet[ exhausted: ").append(isExhausted());
        sb.append(", ").append(metadata).append(']');
        return sb.toString();
    }

    private static class FetchingState {
        public final ByteBuffer nextStart;
        public final ListenableFuture<Void> inProgress;

        FetchingState(ByteBuffer nextStart, ListenableFuture<Void> inProgress) {
            assert (nextStart == null) != (inProgress == null);
            this.nextStart = nextStart;
            this.inProgress = inProgress;
        }
    }
}

