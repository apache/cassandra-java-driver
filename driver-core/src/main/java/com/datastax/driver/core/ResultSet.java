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


/**
 * The result of a query.
 * <p/>
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
 * <p/>
 * Note that this class is not thread-safe.
 */
public interface ResultSet extends PagingIterable<ResultSet, Row> {

    // redeclared only to make clirr happy
    @Override
    Row one();

    /**
     * Returns the columns returned in this ResultSet.
     *
     * @return the columns returned in this ResultSet.
     */
    public ColumnDefinitions getColumnDefinitions();

    /**
     * If the query that produced this ResultSet was a conditional update,
     * return whether it was successfully applied.
     * <p/>
     * This is equivalent to calling:
     * <p/>
     * <pre>
     * rs.one().getBool("[applied]");
     * </pre>
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
    public boolean wasApplied();
}
