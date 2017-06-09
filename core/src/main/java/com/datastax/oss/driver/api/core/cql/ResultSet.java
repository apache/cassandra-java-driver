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

import java.util.List;

/**
 * The result of a synchronous CQL query.
 *
 * <p>It uses {@link AsyncResultSet asynchronous calls} internally, but blocks on the results in
 * order to provide a synchronous API to its clients. If the query is paged, only the first page
 * will be fetched initially, and iteration will trigger background fetches of the next pages when
 * necessary.
 *
 * <p>Note that this object can only be iterated once: rows are "consumed" as they are read,
 * subsequent calls to {@code iterator()} will the same iterator instance.
 *
 * <p>Implementations of this type are <b>not</b> thread-safe. They can only be iterated by the
 * thread that invoked {@code session.execute}.
 *
 * @see CqlSession#execute(Statement)
 * @see CqlSession#execute(String)
 */
public interface ResultSet extends Iterable<Row> {

  ColumnDefinitions getColumnDefinitions();

  /**
   * The execution information for the last query performed for this result set.
   *
   * <p>This is a shortcut for:
   *
   * <pre>
   * getExecutionInfos().get(getExecutionInfos().size() - 1)
   * </pre>
   *
   * @see #getExecutionInfos()
   */
  default ExecutionInfo getExecutionInfo() {
    List<ExecutionInfo> infos = getExecutionInfos();
    return infos.get(infos.size() - 1);
  }

  /**
   * The execution information for all the queries that have been performed so far to assemble this
   * result set.
   *
   * <p>This will have multiple elements if the query is paged, since the driver performs blocking
   * background queries to fetch additional pages transparently as the result set is being iterated.
   */
  List<ExecutionInfo> getExecutionInfos();

  /**
   * Whether all pages have been fetched from the database.
   *
   * <p>If this is {@code false}, it means that more blocking background queries will be triggered
   * as iteration continues.
   */
  boolean isFullyFetched();

  /**
   * The number of rows that can be returned from this result set before a blocking background query
   * needs to be performed to retrieve more results. In other words, this is the number of rows
   * remaining in the current page.
   */
  int getAvailableWithoutFetching();

  /**
   * Forces the driver to fetch the next page of results, regardless of whether the current page is
   * exhausted.
   *
   * <p>If all pages have already been fetched ({@code isFullyFetched() == true}), this method has
   * no effect.
   *
   * <p>This can be used to pre-fetch the next page early to improve performance. For example, if
   * you want to start fetching the next page as soon as you reach the last 100 rows of the current
   * one, you can use:
   *
   * <pre>{@code
   * Iterator<Row> iterator = rs.iterator();
   * while (iterator.hasNext()) {
   *   if (rs.getAvailableWithoutFetching() == 100) {
   *     rs.fetchNextPage();
   *   }
   *   Row row = iterator.next();
   *   ... process the row ...
   * }
   * }</pre>
   *
   * <p>Note that, contrary to version 3.x of the driver, this method deliberately avoids returning
   * a future. If you want to iterate a multi-page result asynchronously with callbacks, use {@link
   * AsyncResultSet}.
   */
  void fetchNextPage();
}
