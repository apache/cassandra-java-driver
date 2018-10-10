/*
 * Copyright DataStax, Inc.
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

import com.datastax.oss.driver.api.core.CqlSession;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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

  /** @return the column definitions contained in this result set. */
  @NonNull
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
  @NonNull
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
  @NonNull
  List<ExecutionInfo> getExecutionInfos();

  /**
   * Returns the next row, or {@code null} if the result set is exhausted.
   *
   * <p>This is convenient for queries that are known to return exactly one row, for example count
   * queries.
   */
  @Nullable
  default Row one() {
    Iterator<Row> iterator = iterator();
    return iterator.hasNext() ? iterator.next() : null;
  }

  /**
   * Returns all the remaining rows as a list; <b>not recommended for queries that return a large
   * number of rows</b>.
   *
   * <p>Contrary to {@link #iterator()} or successive calls to {@link #one()}, this method forces
   * fetching the <b>full contents</b> of the result set at once; in particular, this means that a
   * large number of background queries might have to be run, and that all the data will be held in
   * memory locally. Therefore it is crucial to only call this method for queries that are known to
   * return a reasonable number of results.
   */
  @NonNull
  default List<Row> all() {
    Iterator<Row> iterator = iterator();
    if (!iterator.hasNext()) {
      return Collections.emptyList();
    }
    List<Row> result = new ArrayList<>();
    while (iterator.hasNext()) {
      result.add(iterator.next());
    }
    return result;
  }

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
