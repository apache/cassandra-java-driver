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
package com.datastax.driver.graph;

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

/**
 * The result set containing the Graph data returned from a query,
 */
public class GraphResultSet implements Iterable<GraphTraversalResult> {
    private final ResultSet rs;

    GraphResultSet(ResultSet rs) {
        this.rs = rs;
    }

    /**
     * API
     */

    /**
     * Returns whether this GraphResultSet has more results.
     *
     * @return whether this GraphResultSet has more results.
     */
    public boolean isExhausted() {
        return rs.isExhausted();
    }

    /**
     * Returns the next result from this GraphResultSet.
     *
     * @return the next GraphTraversalResult in this GraphResultSet or null if this GraphResultSet is
     * exhausted.
     */
    public GraphTraversalResult one() {
        return GraphTraversalResult.fromRow(rs.one());
    }

    /**
     * Returns all the remaining GraphTraversalResults in this GraphResultSet as a list.
     * <p/>
     * Note that, contrary to {@code iterator()} or successive calls to
     * {@code one()}, this method forces fetching the full content of the GraphResultSet
     * at once, holding it all in memory in particular. It is thus recommended
     * to prefer iterations through {@code iterator()} when possible, especially
     * if the GraphResultSet can be big.
     *
     * @return a list containing the remaining results of this GraphResultSet. The
     * returned list is empty if and only the GraphResultSet is exhausted. The GraphResultSet
     * will be exhausted after a call to this method.
     */
    public List<GraphTraversalResult> all() {
        return Lists.transform(rs.all(), new Function<Row, GraphTraversalResult>() {
            @Override
            public GraphTraversalResult apply(Row input) {
                return GraphTraversalResult.fromRow(input);
            }
        });
    }

    /**
     * Returns an iterator over the GraphTraversalResults contained in this GraphResultSet.
     * <p/>
     * The {@link Iterator#next} method is equivalent to calling {@link #one}.
     * So this iterator will consume results from this GraphResultSet and after a
     * full iteration, the GraphResultSet will be empty.
     * <p/>
     * The returned iterator does not support the {@link Iterator#remove} method.
     *
     * @return an iterator that will consume and return the remaining GraphTraversalResults of
     * this GraphResultSet.
     */
    public Iterator<GraphTraversalResult> iterator() {
        return new Iterator<GraphTraversalResult>() {
            @Override
            public boolean hasNext() {
                return !rs.isExhausted();
            }

            @Override
            public GraphTraversalResult next() {
                return GraphResultSet.this.one();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * The number of GraphTraversalResults that can be retrieved from this result set without
     * blocking to fetch.
     *
     * @return the number of GraphTraversalResults readily available in this result set. If
     * {@link #isFullyFetched()}, this is the total number of GraphTraversalResults remaining
     * in this result set (after which the result set will be exhausted).
     */
    public int getAvailableWithoutFetching() {
        return rs.getAvailableWithoutFetching();
    }

    /**
     * Whether all results from this result set have been fetched from the
     * database.
     * <p/>
     * Note that if {@code isFullyFetched()}, then {@link #getAvailableWithoutFetching}
     * will return how many GraphTraversalResults remain in the result set before exhaustion. But
     * please note that {@code !isFullyFetched()} never guarantees that the result set
     * is not exhausted (you should call {@code isExhausted()} to verify it).
     *
     * @return whether all results have been fetched.
     */
    public boolean isFullyFetched() {
        return rs.isFullyFetched();
    }

    /**
     * Force fetching the next page of results for this result set, if any.
     * <p/>
     */
    // TODO : tests for this.
    public ListenableFuture<GraphResultSet> fetchMoreResults() {
        return Futures.transform(rs.fetchMoreResults(), new Function<ResultSet, GraphResultSet>() {
            @Override
            public GraphResultSet apply(ResultSet input) {
                return new GraphResultSet(input);
            }
        });
    }

    /**
     * Returns information on the execution of the last query made for this GraphResultSet.
     * <p/>
     * Note that in most cases, a GraphResultSet is fetched with only one query, but large
     * result sets can be paged and thus be retrieved by multiple queries. In that
     * case this method return the {@code ExecutionInfo} for the last query
     * performed. To retrieve the information for all queries, use {@link #getAllExecutionInfo}.
     * <p/>
     * The returned object includes basic information such as the queried hosts,
     * but also the Cassandra query trace if tracing was enabled for the query.
     *
     * @return the execution info for the last query made for this GraphResultSet.
     */
    public ExecutionInfo getExecutionInfo() {
        return rs.getExecutionInfo();
    }

    /**
     * Return the execution information for all queries made to retrieve this
     * GraphResultSet.
     * <p/>
     * Unless the GraphResultSet is large enough to get paged underneath, the returned
     * list will be singleton. If paging has been used however, the returned list
     * contains the {@code ExecutionInfo} for all the queries done to obtain this
     * GraphResultSet (at the time of the call) in the order those queries were made.
     *
     * @return a list of the execution info for all the queries made for this GraphResultSet.
     */
    public List<ExecutionInfo> getAllExecutionInfo() {
        return rs.getAllExecutionInfo();
    }

    /**
     * If the query that produced this GraphResultSet was a conditional update,
     * return whether it was successfully applied.
     * <p/>
     * For consistency, this method always returns {@code true} for
     * non-conditional queries (although there is no reason to call the method
     * in that case). This is also the case for conditional DDL statements
     * ({@code CREATE KEYSPACE... IF NOT EXISTS}, {@code CREATE TABLE... IF NOT EXISTS}),
     * for which Cassandra doesn't return an {@code [applied]} column.
     * <p/>
     * Note that, for versions of Cassandra strictly lower than 2.0.9 and 2.1.0-rc2,
     * a server-side bug (CASSANDRA-7337) causes this method to always return
     * {@code true} for batches containing conditional queries.
     *
     * @return if the query was a conditional update, whether it was applied.
     * {@code true} for other types of queries.
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-7337">CASSANDRA-7337</a>
     */
    public boolean wasApplied() {
        return rs.wasApplied();
    }
}
