package com.datastax.driver.core;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.transport.messages.ResultMessage;

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

    static ResultSet fromMessage(ResultMessage msg, Session.Manager session, InetAddress queriedHost) {

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
}
