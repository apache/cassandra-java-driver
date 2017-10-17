/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.cql;

import java.util.concurrent.CompletionStage;

/**
 * The result of an asynchronous CQL query.
 *
 * @see CqlSession#executeAsync(Statement)
 * @see CqlSession#executeAsync(String)
 */
public interface AsyncResultSet {

  ColumnDefinitions getColumnDefinitions();

  ExecutionInfo getExecutionInfo();

  /** How many rows are left before the current page is exhausted. */
  int remaining();

  /**
   * The rows in the current page. To keep iterating beyond that, use {@link #hasMorePages()} and
   * {@link #fetchNextPage()}.
   *
   * <p>Note that this method always returns the same object, and that that object can only be
   * iterated once: rows are "consumed" as they are read.
   */
  Iterable<Row> currentPage();

  /**
   * Whether there are more pages of results. If so, call {@link #fetchNextPage()} to fetch the next
   * one asynchronously.
   */
  boolean hasMorePages();

  /**
   * Fetch the next page of results asynchronously.
   *
   * @throws IllegalStateException if there are no more pages. Use {@link #hasMorePages()} to check
   *     if you can call this method.
   */
  CompletionStage<AsyncResultSet> fetchNextPage() throws IllegalStateException;

  /**
   * If the query that produced this result was a conditional update, indicate whether it was
   * successfully applied.
   *
   * <p>This is equivalent to calling:
   *
   * <pre>
   *   this.iterator().next().getBoolean("[applied]")
   * </pre>
   *
   * Except that this method peeks at the next row without consuming it.
   *
   * <p>For consistency, this method always returns {@code true} for non-conditional queries
   * (although there is no reason to call the method in that case). This is also the case for
   * conditional DDL statements ({@code CREATE KEYSPACE... IF NOT EXISTS}, {@code CREATE TABLE... IF
   * NOT EXISTS}), for which Cassandra doesn't return an {@code [applied]} column.
   *
   * <p>Note that, for versions of Cassandra strictly lower than 2.1.0-rc2, a server-side bug (<a
   * href="https://issues.apache.org/jira/browse/CASSANDRA-7337">CASSANDRA-7337</a>) causes this
   * method to always return {@code true} for batches containing conditional queries.
   */
  boolean wasApplied();
}
