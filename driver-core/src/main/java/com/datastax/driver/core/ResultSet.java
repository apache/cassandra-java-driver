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

import java.util.Iterator;
import java.util.List;

/**
 * The result of a query.
 *
 * Note that this class is not thread-safe.
 */
public interface ResultSet extends Iterable<Row> {

    /**
     * Returns the columns returned in this ResultSet.
     *
     * @return the columns returned in this ResultSet.
     */
    public ColumnDefinitions getColumnDefinitions();

    /**
     * Returns whether this ResultSet has more results.
     *
     * @return whether this ResultSet has more results.
     */
    public boolean isExhausted();

    /**
     * Returns the the next result from this ResultSet.
     *
     * @return the next row in this resultSet or null if this ResultSet is
     * exhausted.
     */
    public Row one();

    /**
     * Returns all the remaining rows in this ResultSet as a list.
     *
     * @return a list containing the remaining results of this ResultSet. The
     * returned list is empty if and only the ResultSet is exhausted.
     */
    public List<Row> all();

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
    public Iterator<Row> iterator();

    /**
     * Returns information on the execution of this query.
     * <p>
     * The returned object includes basic information such as the queried hosts,
     * but also the Cassandra query trace if tracing was enabled for the query.
     *
     * @return the execution info for this query.
     */
    public ExecutionInfo getExecutionInfo();
}
