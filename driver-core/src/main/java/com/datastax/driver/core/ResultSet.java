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

import org.apache.cassandra.transport.messages.ResultMessage;

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

    private ResultSet(ColumnDefinitions metadata, Queue<List<ByteBuffer>> rows, ExecutionInfo info) {
        this.metadata = metadata;
        this.rows = rows;
        this.info = info;
    }

    static ResultSet fromMessage(ResultMessage msg, Session.Manager session, ExecutionInfo info) {

        UUID tracingId = msg.getTracingId();
        info = tracingId == null || info == null ? info : info.withTrace(new QueryTrace(tracingId, session));

        switch (msg.kind) {
            case VOID:
                return empty(info);
            case ROWS:
                ResultMessage.Rows r = (ResultMessage.Rows)msg;
                ColumnDefinitions.Definition[] defs = new ColumnDefinitions.Definition[r.result.metadata.names.size()];
                for (int i = 0; i < defs.length; i++)
                    defs[i] = ColumnDefinitions.Definition.fromTransportSpecification(r.result.metadata.names.get(i));

                return new ResultSet(new ColumnDefinitions(defs), new ArrayDeque<List<ByteBuffer>>(r.result.rows), info);
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
        return new ResultSet(ColumnDefinitions.EMPTY, EMPTY_QUEUE, info);
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
        return rows.isEmpty();
    }

    /**
     * Returns the the next result from this ResultSet.
     *
     * @return the next row in this resultSet or null if this ResultSet is
     * exhausted.
     */
    public Row one() {
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
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {

            public boolean hasNext() {
                return !rows.isEmpty();
            }

            public Row next() {
                return Row.fromData(metadata, rows.poll());
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
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
}
