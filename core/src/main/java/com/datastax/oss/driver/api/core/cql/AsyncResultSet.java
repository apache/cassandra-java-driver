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

import com.datastax.oss.driver.api.core.session.Session;
import java.util.concurrent.CompletionStage;

/**
 * The result of an asynchronous CQL query.
 *
 * <p>If the query is paged, the rows returned by {@link #iterator()} represent <b>only the current
 * page</b>. To keep iterating beyond that, use {@link #fetchNextPage()}.
 *
 * <p>Note that this object can only be iterated once: rows are "consumed" as they are read,
 * subsequent calls to {@code iterator()} will return an empty iterator.
 *
 * @see Session#executeAsync(Statement)
 * @see Session#executeAsync(String)
 */
public interface AsyncResultSet extends Iterable<Row> {

  ColumnDefinitions getColumnDefinitions();

  ExecutionInfo getExecutionInfo();

  /** How many rows are left before the current page is exhausted. */
  int remaining();

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
}
