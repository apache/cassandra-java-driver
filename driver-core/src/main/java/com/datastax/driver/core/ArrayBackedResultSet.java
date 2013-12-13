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
 * Default implementation of a result set, backed by an ArrayDeque of ArrayList.
 */
class ArrayBackedResultSet implements ResultSet {

    private static final Logger logger = LoggerFactory.getLogger(ResultSet.class);

    private static final Queue<List<ByteBuffer>> EMPTY_QUEUE = new ArrayDeque<List<ByteBuffer>>(0);

    private final ColumnDefinitions metadata;
    private final Queue<List<ByteBuffer>> rows;
    private final ExecutionInfo info;

    private ArrayBackedResultSet(ColumnDefinitions metadata, Queue<List<ByteBuffer>> rows, ExecutionInfo info) {
        this.metadata = metadata;
        this.rows = rows;
        this.info = info;
    }

    static ResultSet fromMessage(ResultMessage msg, SessionManager session, ExecutionInfo info) {

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

                return new ArrayBackedResultSet(new ColumnDefinitions(defs), new ArrayDeque<List<ByteBuffer>>(r.result.rows), info);
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
        return new ArrayBackedResultSet(ColumnDefinitions.EMPTY, EMPTY_QUEUE, info);
    }

    public ColumnDefinitions getColumnDefinitions() {
        return metadata;
    }

    public boolean isExhausted() {
        return rows.isEmpty();
    }

    public Row one() {
        return ArrayBackedRow.fromData(metadata, rows.poll());
    }

    public List<Row> all() {
        if (isExhausted())
            return Collections.emptyList();

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
                return !rows.isEmpty();
            }

            @Override
            public Row next() {
                return ArrayBackedRow.fromData(metadata, rows.poll());
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

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

