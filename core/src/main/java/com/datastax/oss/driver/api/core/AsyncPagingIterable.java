/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.AsyncPagingIterableWrapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Iterator;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

/**
 * An iterable of elements which are fetched asynchronously by the driver, possibly in multiple
 * requests.
 */
public interface AsyncPagingIterable<ElementT, SelfT extends AsyncPagingIterable<ElementT, SelfT>> {

  /** Metadata about the columns returned by the CQL request that was used to build this result. */
  @NonNull
  ColumnDefinitions getColumnDefinitions();

  /** Returns {@linkplain ExecutionInfo information about the execution} of this page of results. */
  @NonNull
  ExecutionInfo getExecutionInfo();

  /** How many rows are left before the current page is exhausted. */
  int remaining();

  /**
   * The elements in the current page. To keep iterating beyond that, use {@link #hasMorePages()}
   * and {@link #fetchNextPage()}.
   *
   * <p>Note that this method always returns the same object, and that that object can only be
   * iterated once: elements are "consumed" as they are read.
   */
  @NonNull
  Iterable<ElementT> currentPage();

  /**
   * Returns the next element, or {@code null} if the results are exhausted.
   *
   * <p>This is convenient for queries that are known to return exactly one element, for example
   * count queries.
   */
  @Nullable
  default ElementT one() {
    Iterator<ElementT> iterator = currentPage().iterator();
    return iterator.hasNext() ? iterator.next() : null;
  }

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
  @NonNull
  CompletionStage<SelfT> fetchNextPage() throws IllegalStateException;

  /**
   * If the query that produced this result was a CQL conditional update, indicate whether it was
   * successfully applied.
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

  /**
   * Creates a new instance by transforming each element of this iterable with the provided
   * function.
   *
   * <p>Note that both instances share the same underlying data: consuming elements from the
   * transformed iterable will also consume them from this object, and vice-versa.
   */
  default <TargetT> MappedAsyncPagingIterable<TargetT> map(
      Function<? super ElementT, ? extends TargetT> elementMapper) {
    return new AsyncPagingIterableWrapper<>(this, elementMapper);
  }
}
