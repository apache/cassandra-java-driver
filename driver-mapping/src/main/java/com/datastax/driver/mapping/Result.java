package com.datastax.driver.mapping;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;


public class Result<T> implements Iterable<T> {

    private final ResultSet rs;
    private final EntityMapper mapper;

    Result(ResultSet rs, EntityMapper mapper) {
        this.rs = rs;
        this.mapper = mapper;
    }

    /**
     * Test whether this ResultSet has more results.
     *
     * @return whether this ResultSet has more results.
     */
    public boolean isExhausted() {
        return rs.isExhausted();
    }

    /**
     * Returns the next result.
     *
     * @return the next row in this resultSet or null if this ResultSet is
     * exhausted.
     */
    public T fetchOne() {
        Row row = rs.fetchOne();
        return row == null ? null : map(row);
    }

    /**
     * Returns all the remaining rows in this ResultSet as a list.
     *
     * @return a list containing the remaining results of this ResultSet. The
     * returned list is empty if and only the ResultSet is exhausted.
     */
    public List<T> fetchAll() {
        List<Row> rows = rs.fetchAll();
        List<T> entities = new ArrayList<T>(rows.size());
        for (Row row : rows) {
            entities.add(map(row));
        }
        return entities;
    }

    @SuppressWarnings("unchecked")
    private T map(Row row) {
        return (T)mapper.rowToEntity(row);
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
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private final Iterator<Row> rowIterator = rs.iterator();

            public boolean hasNext() {
                return rowIterator.hasNext();
            }

            public T next() {
                Row row = rowIterator.next();
                return map(row);
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
        return rs.getQueryTrace();
    }

}
