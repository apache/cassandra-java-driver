/*
 * Copyright (C) 2012-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;

/**
 * Defines an iterable whose elements can be remotely fetched and paged,
 * possibly asynchronously.
 */
public interface PagingIterable<S extends PagingIterable<S, T>, T> extends Iterable<T> {

    /**
     * Returns whether this result set has more results.
     *
     * @return whether this result set has more results.
     */
    boolean isExhausted();

    /**
     * Whether all results from this result set have been fetched from the
     * database.
     * <p/>
     * Note that if {@code isFullyFetched()}, then {@link #getAvailableWithoutFetching}
     * will return how many rows remain in the result set before exhaustion. But
     * please note that {@code !isFullyFetched()} never guarantees that the result set
     * is not exhausted (you should call {@link #isExhausted()} to verify it).
     *
     * @return whether all results have been fetched.
     */
    boolean isFullyFetched();

    /**
     * The number of rows that can be retrieved from this result set without
     * blocking to fetch.
     *
     * @return the number of rows readily available in this result set. If
     * {@link #isFullyFetched()}, this is the total number of rows remaining
     * in this result set (after which the result set will be exhausted).
     */
    int getAvailableWithoutFetching();

    /**
     * Force fetching the next page of results for this result set, if any.
     * <p/>
     * This method is entirely optional. It will be called automatically while
     * the result set is consumed (through {@link #one}, {@link #all} or iteration)
     * when needed (i.e. when {@code getAvailableWithoutFetching() == 0} and
     * {@code isFullyFetched() == false}).
     * <p/>
     * You can however call this method manually to force the fetching of the
     * next page of results. This can allow to prefetch results before they are
     * strictly needed. For instance, if you want to prefetch the next page of
     * results as soon as there is less than 100 rows readily available in this
     * result set, you can do:
     * <pre>
     *   ResultSet rs = session.execute(...);
     *   Iterator&lt;Row&gt; iter = rs.iterator();
     *   while (iter.hasNext()) {
     *       if (rs.getAvailableWithoutFetching() == 100 &amp;&amp; !rs.isFullyFetched())
     *           rs.fetchMoreResults();
     *       Row row = iter.next()
     *       ... process the row ...
     *   }
     * </pre>
     * This method is not blocking, so in the example above, the call to {@code
     * fetchMoreResults} will not block the processing of the 100 currently available
     * rows (but {@code iter.hasNext()} will block once those rows have been processed
     * until the fetch query returns, if it hasn't yet).
     * <p/>
     * Only one page of results (for a given result set) can be
     * fetched at any given time. If this method is called twice and the query
     * triggered by the first call has not returned yet when the second one is
     * performed, then the 2nd call will simply return a future on the currently
     * in progress query.
     *
     * @return a future on the completion of fetching the next page of results.
     * If the result set is already fully retrieved ({@code isFullyFetched() == true}),
     * then the returned future will return immediately but not particular error will be
     * thrown (you should thus call {@link #isFullyFetched()} to know if calling this
     * method can be of any use}).
     */
    ListenableFuture<S> fetchMoreResults();

    /**
     * Returns the next result from this result set.
     *
     * @return the next row in this result set or null if this result set is
     * exhausted.
     */
    T one();

    /**
     * Returns all the remaining rows in this result set as a list.
     * <p/>
     * Note that, contrary to {@link #iterator()} or successive calls to
     * {@link #one()}, this method forces fetching the full content of the result set
     * at once, holding it all in memory in particular. It is thus recommended
     * to prefer iterations through {@link #iterator()} when possible, especially
     * if the result set can be big.
     *
     * @return a list containing the remaining results of this result set. The
     * returned list is empty if and only the result set is exhausted. The result set
     * will be exhausted after a call to this method.
     */
    List<T> all();

    /**
     * Returns an iterator over the rows contained in this result set.
     * <p/>
     * The {@link Iterator#next} method is equivalent to calling {@link #one}.
     * So this iterator will consume results from this result set and after a
     * full iteration, the result set will be empty.
     * <p/>
     * The returned iterator does not support the {@link Iterator#remove} method.
     *
     * @return an iterator that will consume and return the remaining rows of
     * this result set.
     */
    Iterator<T> iterator();

    /**
     * Returns information on the execution of the last query made for this result set.
     * <p/>
     * Note that in most cases, a result set is fetched with only one query, but large
     * result sets can be paged and thus be retrieved by multiple queries. In that
     * case this method return the {@link ExecutionInfo} for the last query
     * performed. To retrieve the information for all queries, use {@link #getAllExecutionInfo}.
     * <p/>
     * The returned object includes basic information such as the queried hosts,
     * but also the Cassandra query trace if tracing was enabled for the query.
     *
     * @return the execution info for the last query made for this result set.
     */
    ExecutionInfo getExecutionInfo();

    /**
     * Return the execution information for all queries made to retrieve this
     * result set.
     * <p/>
     * Unless the result set is large enough to get paged underneath, the returned
     * list will be singleton. If paging has been used however, the returned list
     * contains the {@link ExecutionInfo} objects for all the queries done to obtain this
     * result set (at the time of the call) in the order those queries were made.
     *
     * @return a list of the execution info for all the queries made for this result set.
     */
    List<ExecutionInfo> getAllExecutionInfo();

}
