package com.datastax.driver.core;

import java.util.Iterator;
import java.util.List;

/**
 * The result of a query.
 */
public class ResultSet implements Iterable<CQLRow> {

    /**
     * The columns returned in this ResultSet.
     *
     * @return the columns returned in this ResultSet.
     */
    public Columns columns() {
        return null;
    }

    /**
     * Test whether this ResultSet has more results.
     *
     * @return whether this ResultSet has more results.
     */
    public boolean isExhausted() {
        return true;
    }

    /**
     * Returns the the next result from this ResultSet.
     *
     * @return the next row in this resultSet or null if this ResultSet is
     * exhausted.
     */
    public CQLRow fetchOne() {
        return null;
    }

    /**
     * Returns all the remaining rows in this ResultSet as a list.
     *
     * @return a list containing the remaining results of this ResultSet. The
     * returned list is empty if and only the ResultSet is exhausted.
     */
    public List<CQLRow> fetchAll() {
        return null;
    }

    /**
     * An iterator over the rows contained in this ResultSet.
     *
     * The {@link Iterator.next iterator next()} method is equivalent to
     * calling {@link fetchOne}. So this iterator will consume results from
     * this ResultSet and after a full iteration, the ResultSet will be empty.
     *
     * The returned iterator does not support the {@link Iterato.remove} method.
     *
     * @return an iterator that will consume and return the remaining rows of
     * this ResultSet.
     */
    public Iterator<CQLRow> iterator() {
        return null;
    }

    public static class Future // implements java.util.concurrent.Future<ResultSet>
    {
        // TODO
    }
}
