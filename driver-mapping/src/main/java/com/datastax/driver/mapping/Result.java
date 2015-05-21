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
package com.datastax.driver.mapping;

import java.nio.ByteBuffer;
import java.util.*;

import com.datastax.driver.core.*;

/**
 * A {@code ResultSet} mapped to an entity class.
 */
public class Result<T> implements Iterable<T> {

    private final ResultSet rs;
    private final EntityMapper<T> mapper;
    private final ProtocolVersion protocolVersion;
    private final boolean useAlias;

    Result(ResultSet rs, EntityMapper<T> mapper, ProtocolVersion protocolVersion) {
        this(rs, mapper, protocolVersion, false);
    }

    Result(ResultSet rs, EntityMapper<T> mapper, ProtocolVersion protocolVersion, boolean useAlias) {
        this.rs = rs;
        this.mapper = mapper;
        this.protocolVersion = protocolVersion;
        this.useAlias = useAlias;
    }

    private T map(Row row) {
        T entity = mapper.newEntity();
        for (ColumnMapper<T> cm : mapper.allColumns()) {
            if (!useAlias && cm.kind == ColumnMapper.Kind.COMPUTED)
                continue;
            ByteBuffer bytes = row.getBytesUnsafe(cm.getAlias() != null && this.useAlias ? cm.getAlias() : cm.getColumnName());
            if (bytes != null)
                cm.setValue(entity, cm.getDataType().deserialize(bytes, protocolVersion));
        }
        return entity;
    }

    /**
     * Test whether this mapped result set has more results.
     *
     * @return whether this mapped result set has more results.
     */
    public boolean isExhausted() {
        return rs.isExhausted();
    }

    /**
     * Returns the next result (i.e. the entity corresponding to the next row
     * in the result set).
     *
     * @return the next result in this mapped result set or null if it is exhausted.
     */
    public T one() {
        Row row = rs.one();
        return row == null ? null : map(row);
    }

    /**
     * Returns all the remaining results (entities) in this mapped result set
     * as a list.
     *
     * @return a list containing the remaining results of this mapped result
     * set. The returned list is empty if and only the result set is exhausted.
     */
    public List<T> all() {
        List<Row> rows = rs.all();
        List<T> entities = new ArrayList<T>(rows.size());
        for (Row row : rows) {
            entities.add(map(row));
        }
        return entities;
    }

    /**
     * An iterator over the entities of this mapped result set.
     *
     * The {@link Iterator#next} method is equivalent to calling {@link #one}.
     * So this iterator will consume results and after a full iteration, the
     * mapped result set (and underlying {@code ResultSet}) will be empty.
     *
     * The returned iterator does not support the {@link Iterator#remove} method.
     *
     * @return an iterator that will consume and return the remaining rows of
     * this mapped result set.
     */
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private final Iterator<Row> rowIterator = rs.iterator();

            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            public T next() {
                return map(rowIterator.next());
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
        return rs.getExecutionInfo();
    }
}
