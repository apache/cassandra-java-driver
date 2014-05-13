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

import com.google.common.util.concurrent.ListenableFuture;

/**
 * The result of a query.
 * <p>
 * The retrieval of the rows of a ResultSet is generally paged (a first page
 * of result is fetched and the next one is only fetched once all the results
 * of the first one has been consumed). The size of the pages can be configured
 * either globally through {@link QueryOptions#setFetchSize} or per-statement
 * with {@link Statement#setFetchSize}. Though new pages are automatically (and
 * transparently) fetched when needed, it is possible to force the retrieval
 * of the next page early through {@link #fetchMoreResults}. Please note however
 * that this ResultSet paging is not available with the version 1 of the native
 * protocol (i.e. with Cassandra 1.2 or if version 1 has been explicitly requested
 * through {@link Cluster.Builder#withProtocolVersion}). If the protocol version 1
 * is in use, a ResultSet is always fetched in it's entirely and it's up to the
 * client to make sure that no query can yield ResultSet that won't hold in memory.
 * <p>
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
     * <p>
     * Note that, contrary to {@code iterator()} or successive calls to
     * {@code one()}, this method forces fetching the full content of the ResultSet
     * at once, holding it all in memory in particular. It is thus recommended
     * to prefer iterations through {@code iterator()} when possible, especially
     * if the ResultSet can be big.
     *
     * @return a list containing the remaining results of this ResultSet. The
     * returned list is empty if and only the ResultSet is exhausted. The ResultSet
     * will be exhausted after a call to this method.
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
     * The number of rows that can be retrieved from this result set without
     * blocking to fetch.
     *
     * @return the number of rows readily available in this result set. If
     * {@link #isFullyFetched()}, this is the total number of rows remaining
     * in this result set (after which the result set will be exhausted).
     */
    public int getAvailableWithoutFetching();

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
    public boolean isFullyFetched();

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
     * strictly needed. For instance, if you want to prefetch the next page of
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
    public ListenableFuture<Void> fetchMoreResults();

    /**
     * Returns information on the execution of the last query made for this ResultSet.
     * <p>
     * Note that in most cases, a ResultSet is fetched with only one query, but large
     * result sets can be paged and thus be retrieved by multiple queries. In that
     * case this method return the {@code ExecutionInfo} for the last query
     * performed. To retrieve the information for all queries, use {@link #getAllExecutionInfo}.
     * <p>
     * The returned object includes basic information such as the queried hosts,
     * but also the Cassandra query trace if tracing was enabled for the query.
     *
     * @return the execution info for the last query made for this ResultSet.
     */
    public ExecutionInfo getExecutionInfo();

    /**
     * Return the execution information for all queries made to retrieve this
     * ResultSet.
     * <p>
     * Unless the ResultSet is large enough to get paged underneath, the returned
     * list will be singleton. If paging has been used however, the returned list
     * contains the {@code ExecutionInfo} for all the queries done to obtain this
     * ResultSet (at the time of the call) in the order those queries were made.
     *
     * @return a list of the execution info for all the queries made for this ResultSet.
     */
    public List<ExecutionInfo> getAllExecutionInfo();
}
