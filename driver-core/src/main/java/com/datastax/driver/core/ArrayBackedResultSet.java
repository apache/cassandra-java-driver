/*
 *      Copyright (C) 2012-2014 DataStax Inc.
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
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;

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
abstract class ArrayBackedResultSet implements ResultSet {

    private static final Logger logger = LoggerFactory.getLogger(ResultSet.class);

    private static final Queue<List<ByteBuffer>> EMPTY_QUEUE = new ArrayDeque<List<ByteBuffer>>(0);

    protected final ColumnDefinitions metadata;
    protected final Token.Factory tokenFactory;
    private final boolean wasApplied;

    private ArrayBackedResultSet(ColumnDefinitions metadata, Token.Factory tokenFactory, List<ByteBuffer> firstRow) {
        this.metadata = metadata;
        this.tokenFactory = tokenFactory;
        this.wasApplied = checkWasApplied(firstRow, metadata);
    }

    static ArrayBackedResultSet fromMessage(Responses.Result msg, SessionManager session, ExecutionInfo info, Statement statement) {
        info = update(info, msg, session);

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

                Token.Factory tokenFactory = (session == null) ? null
                    : session.getCluster().getMetadata().tokenFactory();

                // info can be null only for internal calls, but we don't page those. We assert
                // this explicitly because MultiPage implementation don't support info == null.
                assert r.metadata.pagingState == null || info != null;
                return r.metadata.pagingState == null
                    ? new SinglePage(columnDefs, tokenFactory, r.data, info)
                    : new MultiPage(columnDefs, tokenFactory, r.data, info, r.metadata.pagingState, session, statement);

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

    private static ExecutionInfo update(ExecutionInfo info, Responses.Result msg, SessionManager session) {
        UUID tracingId = msg.getTracingId();
        return tracingId == null || info == null ? info : info.withTrace(new QueryTrace(tracingId, session));
    }

    private static ArrayBackedResultSet empty(ExecutionInfo info) {
        return new SinglePage(ColumnDefinitions.EMPTY, null, EMPTY_QUEUE, info);
    }

    public ColumnDefinitions getColumnDefinitions() {
        return metadata;
    }

    public List<Row> all() {
        if (isExhausted())
            return Collections.emptyList();

        // We may have more than 'getAvailableWithoutFetching' results but we won't have less, and
        // at least in the single page case this will be exactly the size we want so ...
        List<Row> result = new ArrayList<Row>(getAvailableWithoutFetching());
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

    @Override
    public boolean wasApplied() {
        return wasApplied;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ResultSet[ exhausted: ").append(isExhausted());
        sb.append(", ").append(metadata).append(']');
        return sb.toString();
    }

    private static class SinglePage extends ArrayBackedResultSet {

        private final Queue<List<ByteBuffer>> rows;
        private final ExecutionInfo info;

        private SinglePage(ColumnDefinitions metadata,
                           Token.Factory tokenFactory,
                           Queue<List<ByteBuffer>> rows,
                           ExecutionInfo info) {
            super(metadata, tokenFactory, rows.peek());
            this.info = info;
            this.rows = rows;
        }

        public boolean isExhausted() {
            return rows.isEmpty();
        }

        public Row one() {
            return ArrayBackedRow.fromData(metadata, tokenFactory, rows.poll());
        }

        public int getAvailableWithoutFetching() {
            return rows.size();
        }

        public boolean isFullyFetched() {
            return true;
        }

        public ListenableFuture<Void> fetchMoreResults() {
            return Futures.immediateFuture(null);
        }

        public ExecutionInfo getExecutionInfo() {
            return info;
        }

        public List<ExecutionInfo> getAllExecutionInfo() {
            return Collections.singletonList(info);
        }
    }

    private static class MultiPage extends ArrayBackedResultSet {

        private Queue<List<ByteBuffer>> currentPage;
        private final Queue<Queue<List<ByteBuffer>>> nextPages = new ConcurrentLinkedQueue<Queue<List<ByteBuffer>>>();

        private final Deque<ExecutionInfo> infos = new LinkedBlockingDeque<ExecutionInfo>();

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

        private final SessionManager session;
        private final Statement statement;

        private MultiPage(ColumnDefinitions metadata,
                          Token.Factory tokenFactory,
                          Queue<List<ByteBuffer>> rows,
                          ExecutionInfo info,
                          ByteBuffer pagingState,
                          SessionManager session,
                          Statement statement) {

            // Note: as of Cassandra 2.1.0, it turns out that the result of a CAS update is never paged, so
            // we could hard-code the result of wasApplied in this class to "true". However, we can not be sure
            // that this will never change, so apply the generic check by peeking at the first row.
            super(metadata, tokenFactory, rows.peek());
            this.currentPage = rows;
            this.infos.offer(info.withPagingState(pagingState).withStatement(statement));

            this.fetchState = new FetchingState(pagingState, null);
            this.session = session;
            this.statement = statement;
        }

        public boolean isExhausted() {
            prepareNextRow();
            return currentPage.isEmpty();
        }

        public Row one() {
            prepareNextRow();
            return ArrayBackedRow.fromData(metadata, tokenFactory, currentPage.poll());
        }

        public int getAvailableWithoutFetching() {
            int available = currentPage.size();
            for (Queue<List<ByteBuffer>> page : nextPages)
                available += page.size();
            return available;
        }

        public boolean isFullyFetched() {
            return fetchState == null;
        }

        // Ensure that after the call the next row to consume is in 'currentPage', i.e. that
        // 'currentPage' is empty IFF the ResultSet if fully exhausted.
        private void prepareNextRow() {
            while (currentPage.isEmpty()) {
                // Grab the current state now to get a consistent view in this iteration.
                FetchingState fetchingState = this.fetchState;

                Queue<List<ByteBuffer>> nextPage = nextPages.poll();
                if (nextPage != null) {
                    currentPage = nextPage;
                    continue;
                }
                if (fetchingState == null)
                    return;

                // We need to know if there is more result, so fetch the next page and
                // wait on it.
                try {
                    Uninterruptibles.getUninterruptibly(fetchMoreResults());
                } catch (ExecutionException e) {
                    throw DefaultResultSetFuture.extractCauseFromExecutionException(e);
                }
            }
        }

        public ListenableFuture<Void> fetchMoreResults() {
            return fetchMoreResults(this.fetchState);
        }

        private ListenableFuture<Void> fetchMoreResults(FetchingState fetchState) {
            if (fetchState == null)
                return Futures.immediateFuture(null);

            if (fetchState.inProgress != null)
                return fetchState.inProgress;

            assert fetchState.nextStart != null;
            ByteBuffer state = fetchState.nextStart;
            SettableFuture<Void> future = SettableFuture.create();
            this.fetchState = new FetchingState(null, future);
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
                                info = update(info, rm, MultiPage.this.session);
                                if (rm.kind == Responses.Result.Kind.ROWS) {
                                    Responses.Result.Rows rows = (Responses.Result.Rows)rm;
                                    if (rows.metadata.pagingState != null)
                                        info = info.withPagingState(rows.metadata.pagingState).withStatement(statement);
                                    MultiPage.this.nextPages.offer(rows.data);
                                    MultiPage.this.fetchState = rows.metadata.pagingState == null ? null : new FetchingState(rows.metadata.pagingState, null);
                                } else if (rm.kind == Responses.Result.Kind.VOID) {
                                    // We shouldn't really get a VOID message here but well, no harm in handling it I suppose
                                    MultiPage.this.fetchState = null;
                                } else {
                                    logger.error("Received unknown result type '{}' during paging: ignoring message", rm.kind);
                                    // This mean we have probably have a bad node, so defunct the connection
                                    connection.defunct(new ConnectionException(connection.address, String.format("Got unexpected %s result response", rm.kind)));
                                    future.setException(new DriverInternalError(String.format("Got unexpected %s result response from %s", rm.kind, connection.address)));
                                    return;
                                }

                                MultiPage.this.infos.offer(info);
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
                public void onSet(Connection connection, Message.Response response, long latency, int retryCount) {
                    onSet(connection, response, null, null, latency);
                }

                @Override
                public void onException(Connection connection, Exception exception, long latency, int retryCount) {
                    future.setException(exception);
                }

                @Override
                public boolean onTimeout(Connection connection, long latency, int retryCount) {
                    // This won't be called directly since this will be wrapped by RequestHandler.
                    throw new UnsupportedOperationException();
                }

                @Override
                public int retryCount() {
                    // This is only called for internal calls (i.e, when the callback is not wrapped in RequestHandler).
                    // There is no retry logic in that case, so the value does not really matter.
                    return 0;
                }
            }, statement);

            return future;
        }

        public ExecutionInfo getExecutionInfo() {
            return infos.getLast();
        }

        public List<ExecutionInfo> getAllExecutionInfo() {
            return new ArrayList<ExecutionInfo>(infos);
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

    // This method checks the value of the "[applied]" column manually, to avoid instantiating an ArrayBackedRow
    // object that we would throw away immediately.
    private static boolean checkWasApplied(List<ByteBuffer> firstRow, ColumnDefinitions metadata) {
        // If the column is not present or not a boolean, we assume the query
        // was not a conditional statement, and therefore return true.
        if (firstRow == null)
            return true;
        int[] is = metadata.findAllIdx("[applied]");
        if (is == null)
            return true;
        int i = is[0];
        if (!DataType.cboolean().equals(metadata.getType(i)))
            return true;

        // Otherwise return the value of the column
        ByteBuffer value = firstRow.get(i);
        if (value == null || value.remaining() == 0)
            return false;

        return TypeCodec.BooleanCodec.instance.deserializeNoBoxing(value);
    }
}
