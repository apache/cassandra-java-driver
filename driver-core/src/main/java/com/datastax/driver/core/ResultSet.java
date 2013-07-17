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

import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Message;
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
public class ResultSet implements Iterable<Row> {

    private static final Logger logger = LoggerFactory.getLogger(ResultSet.class);

    private static final Queue<List<ByteBuffer>> EMPTY_QUEUE = new ArrayDeque<List<ByteBuffer>>(0);

    private final ColumnDefinitions metadata;
    private final Queue<List<ByteBuffer>> rows;

    private final ExecutionInfo info;

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
     * fetch is done asynchonously and so we do need to be volatile below.
     */
    private volatile FetchingState fetchState;
    // The two following info can be null, but only if fetchState == null
    private final Session.Manager session;
    private final Query query;

    private ResultSet(ColumnDefinitions metadata,
                      Queue<List<ByteBuffer>> rows,
                      ExecutionInfo info,
                      PagingState initialPagingState,
                      Session.Manager session,
                      Query query) {
        this.metadata = metadata;
        this.rows = rows;
        this.session = session;
        this.info = info;
        this.fetchState = initialPagingState == null ? null : new FetchingState(initialPagingState, null);
        this.query = query;

        assert fetchState == null || (session != null && query != null);
    }

    static ResultSet fromMessage(ResultMessage msg, Session.Manager session, ExecutionInfo info, Query query) {

        UUID tracingId = msg.getTracingId();
        info = tracingId == null || info == null ? info : info.withTrace(new QueryTrace(tracingId, session));

        switch (msg.kind) {
            case VOID:
                return empty(info);
            case ROWS:
                ResultMessage.Rows r = (ResultMessage.Rows)msg;
                org.apache.cassandra.cql3.ResultSet.Metadata metadata = r.result.metadata;

                ColumnDefinitions columnDefs;
                if (metadata.flags.contains(org.apache.cassandra.cql3.ResultSet.Flag.NO_METADATA)) {
                    assert query instanceof BoundStatement;
                    columnDefs = ((BoundStatement)query).statement.resultSetMetadata;
                    assert columnDefs != null;
                } else {
                    ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[metadata.names.size()];
                    for (int i = 0; i < defs.length; i++)
                        defs[i] = ColumnDefinitions.Definition.fromTransportSpecification(metadata.names.get(i));
                    columnDefs = new ColumnDefinitions(defs);
                }

                PagingState initialState = metadata.flags.contains(org.apache.cassandra.cql3.ResultSet.Flag.HAS_MORE_PAGES) ? metadata.pagingState : null;

                return new ResultSet(columnDefs, new ArrayDeque<List<ByteBuffer>>(r.result.rows), info, initialState, session, query);
            case SET_KEYSPACE:
            case SCHEMA_CHANGE:
                return empty(info);
            case PREPARED:
                throw new RuntimeException("Prepared statement received when a ResultSet was expected");
            default:
                logger.error("Received unknow result type '{}'; returning empty result set", msg.kind);
                return empty(info);
        }
    }

    private static ResultSet empty(ExecutionInfo info) {
        return new ResultSet(ColumnDefinitions.EMPTY, EMPTY_QUEUE, info, null, null, null);
    }

    /**
     * Returns the columns returned in this ResultSet.
     *
     * @return the columns returned in this ResultSet.
     */
    public ColumnDefinitions getColumnDefinitions() {
        return metadata;
    }

    /**
     * Returns whether this ResultSet has more results.
     *
     * @return whether this ResultSet has more results.
     */
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

    /**
     * Returns the the next result from this ResultSet.
     *
     * @return the next row in this resultSet or null if this ResultSet is
     * exhausted.
     */
    public Row one() {
        List<ByteBuffer> nextRow = rows.poll();
        if (nextRow != null)
            return Row.fromData(metadata, nextRow);

        fetchMoreResultsBlocking();
        return Row.fromData(metadata, rows.poll());
    }

    /**
     * Returns all the remaining rows in this ResultSet as a list.
     *
     * @return a list containing the remaining results of this ResultSet. The
     * returned list is empty if and only the ResultSet is exhausted.
     */
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

    /**
     * Returns an iterator over the rows contained in this ResultSet.
     *
     * The {@link Iterator#next} method is equivalent to calling {@link #one}.
     * So this iterator will consume results from this ResultSet and after a
     * full iteration, the ResultSet will be empty.
     *
     * The returned iterator does not support the {@link Iterator#remove} method.
     *
     * @return an iterator that will consume and return the remaining rows of
     * this ResultSet.
     */
    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {

            @Override
            public boolean hasNext() {
                return !isExhausted();
            }

            @Override
            public Row next() {
                return ResultSet.this.one();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * The number of rows that can be retrieved from this result set without
     * blocking to fetch.
     *
     * @return the number of rows readily available in this result set. If
     * {@link #isFullyFetched()}, this is the total number of rows remaining
     * in this result set (after which the result set will be exhausted).
     */
    public int getAvailableWithoutFetching() {
        return rows.size();
    }

    /**
     * Whether all results from this result set has been fetched from the
     * database.
     * <p>
     * Note that if {@code isFullyFetched()}, then {@link #getAvailableWithoutFetching}
     * will return how much rows remains in the result set before exhaustion. But
     * please note that {@code !isFullyFetched()} never guarantees that the result set
     * is not exhausted (you should call {@code isExhausted()} to make sure of it).
     *
     * @return whether all results have been fetched.
     */
    public boolean isFullyFetched() {
        return fetchState == null;
    }

    private void fetchMoreResultsBlocking() {
        try {
            Uninterruptibles.getUninterruptibly(fetchMoreResults());
        } catch (ExecutionException e) {
            throw ResultSetFuture.extractCauseFromExecutionException(e);
        }
    }

    /**
     * Force the fetching the next page of results for this result set, if any.
     * <p>
     * This method is entirely optional. It will be called automatically while
     * the result set is consumed (through {@link #one}, {@link #all} or iteration)
     * when needed (i.e. when {@code getAvailableWithoutFetching() == 0} and
     * {@code isFullyFetched() == false}).
     * <p>
     * You can however call this method manually to force the fetching of the
     * next page of results. This can allow to prefetch results before they are
     * stricly needed. For instance, if you want to prefetch the next page of
     * results as soon as there is less than 100 rows readily available in this
     * result set, you can do:
     * <pre>
     *   ResultSet rs = session.execute(...);
     *   Iterator&lt;Row&gt; iter = rs.iterator();
     *   while (iter.hasNext()) {
     *       if (rs.getAvailableWithoutFetching() == 100 && !rs.isFullyFetched())
     *           rs.fetchMoreResults();
     *       Row row = iter.next()
     *       ... process the row ...
     *   }
     * </pre>
     * This method is not blocking, so in the example above, the call to {@code
     * fetchMoreResults} will not block the processing of the 100 currently available
     * rows (but {@code iter.hasNext()} will block once those rows have been processed
     * until the fetch query returns, if it hasn't yet).
     * <p>
     * Only one page of results (for a given result set) can be
     * fetched at any given time. If this method is called twice and the query
     * triggered by the first call has not returned yet when the second one is
     * performed, then the 2nd call will simply return a future on the currently
     * in progress query.
     *
     * @return a future on the completion of fetching the next page of results.
     * If the result set is already fully retrieved ({@code isFullyFetched() == true}),
     * then the returned future will return immediately but not particular error will be
     * thrown (you should thus call {@code isFullyFetched() to know if calling this
     * method can be of any use}).
     */
    public ListenableFuture<Void> fetchMoreResults() {
        if (isFullyFetched())
            return Futures.immediateFuture(null);

        if (fetchState.inProgress != null)
            return fetchState.inProgress;

        assert fetchState.nextStart != null;
        PagingState state = fetchState.nextStart;
        SettableFuture<Void> future = SettableFuture.create();
        fetchState = new FetchingState(null, future);
        return queryNextPage(state, future);
    }

    private ListenableFuture<Void> queryNextPage(PagingState nextStart, final SettableFuture<Void> future) {

        assert !(query instanceof BatchStatement);

        final Message.Request request = Session.makeRequestMessage(query, nextStart);
        session.execute(new RequestHandler.Callback() {

            @Override
            public Message.Request request() {
                return request;
            }

            @Override
            public void register(RequestHandler handler) {
            }

            @Override
            public void onSet(Connection connection, Message.Response response, ExecutionInfo info, Query query) {
                try {
                    switch (response.type) {
                        case RESULT:
                            ResultMessage rm = (ResultMessage)response;
                            // If we're paging, the query was a SELECT, so we don't have to handle SET_KEYSPACE and SCHEMA_CHANGE really
                            ResultSet tmp = ResultSet.fromMessage(rm, ResultSet.this.session, info, query);

                            // TODO: we shouldn't ignore the execution infos!
                            ResultSet.this.rows.addAll(tmp.rows);
                            ResultSet.this.fetchState = tmp.fetchState;
                            future.set(null);
                            break;
                        case ERROR:
                            future.setException(ResultSetFuture.convertException(((ErrorMessage)response).error));
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
            public void onSet(Connection connection, Message.Response response) {
                onSet(connection, response, null, null);
            }

            @Override
            public void onException(Connection connection, Exception exception) {
                future.setException(exception);
            }

            @Override
            public void onTimeout(Connection connection) {
                // This won't be called directly since this will be wrapped by RequestHandler.
                throw new UnsupportedOperationException();
            }

        }, query);

        return future;
    }

    /**
     * Returns information on the execution of this query.
     * <p>
     * The returned object includes basic information such as the queried hosts,
     * but also the Cassandra query trace if tracing was enabled for the query.
     *
     * @return the execution info for this query.
     */
    public ExecutionInfo getExecutionInfo() {
        return info;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ResultSet[ exhausted: ").append(isExhausted());
        sb.append(", ").append(metadata).append("]");
        return sb.toString();
    }

    private static class FetchingState {
        public final PagingState nextStart;
        public final ListenableFuture<Void> inProgress;

        FetchingState(PagingState nextStart, ListenableFuture<Void> inProgress) {
            assert (nextStart == null) != (inProgress == null);
            this.nextStart = nextStart;
            this.inProgress = inProgress;
        }
    }
}
