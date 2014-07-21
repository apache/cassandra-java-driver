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
    protected final ProtocolVersion protocolVersion;

    private ArrayBackedResultSet(ColumnDefinitions metadata, ProtocolVersion protocolVersion) {
        this.metadata = metadata;
        this.protocolVersion = protocolVersion;
    }

    static ArrayBackedResultSet fromMessage(Responses.Result msg, SessionManager session, ProtocolVersion protocolVersion, ExecutionInfo info, Statement statement) {
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

                // info can be null only for internal calls, but we don't page those. We assert
                // this explicitly because MultiPage implementation don't support info == null.
                assert r.metadata.pagingState == null || info != null;
                return r.metadata.pagingState == null
                     ? new SinglePage(columnDefs, protocolVersion, r.data, info)
                     : new MultiPage(columnDefs, protocolVersion, r.data, info, r.metadata.pagingState, session, statement);

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
        // We could pass the protocol version but we know we won't need it so passing a bogus value (-1)
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
                           ProtocolVersion protocolVersion,
                           Queue<List<ByteBuffer>> rows,
                           ExecutionInfo info) {
            super(metadata, protocolVersion);
            this.info = info;
            this.rows = rows;
        }

        public boolean isExhausted() {
            return rows.isEmpty();
        }

        public Row one() {
            return ArrayBackedRow.fromData(metadata, protocolVersion, rows.poll());
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
                          ProtocolVersion protocolVersion,
                          Queue<List<ByteBuffer>> rows,
                          ExecutionInfo info,
                          ByteBuffer pagingState,
                          SessionManager session,
                          Statement statement) {

            super(metadata, protocolVersion);
            this.currentPage = rows;
            this.infos.offer(info);

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
            return ArrayBackedRow.fromData(metadata, protocolVersion, currentPage.poll());
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
}
