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
package com.datastax.dse.driver.api.core.cql.reactive;

import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A row produced by a {@linkplain ReactiveResultSet reactive result set}.
 *
 * <p>This is essentially an extension of the driver's {@link Row} object that also exposes useful
 * information about {@linkplain #getExecutionInfo() request execution} and {@linkplain
 * #getColumnDefinitions() query metadata} (note however that this information is also exposed at
 * result set level for convenience).
 *
 * @see ReactiveSession
 * @see ReactiveResultSet
 */
public interface ReactiveRow extends Row {

  /**
   * Returns the column definitions contained in this row.
   *
   * <p>This object is the same for all rows pertaining to the same result set.
   *
   * @return the column definitions contained in this row.
   * @see ReactiveResultSet#getColumnDefinitions()
   */
  @NonNull
  @Override
  ColumnDefinitions getColumnDefinitions();

  /**
   * The execution information for the paged request that produced this result.
   *
   * <p>This object is the same for two rows pertaining to the same page, but differs for rows
   * pertaining to different pages.
   *
   * @return the execution information for the paged request that produced this result.
   * @see ReactiveResultSet#getExecutionInfos()
   */
  @NonNull
  ExecutionInfo getExecutionInfo();

  /**
   * If the query that produced this result was a conditional update, indicates whether it was
   * successfully applied.
   *
   * <p>This is equivalent to calling:
   *
   * <pre>{@code
   * ReactiveRow row = ...
   * boolean wasApplied = row.getBoolean("[applied]");
   * }</pre>
   *
   * <p>For consistency, this method always returns {@code true} for non-conditional queries
   * (although there is no reason to call the method in that case). This is also the case for
   * conditional DDL statements ({@code CREATE KEYSPACE... IF NOT EXISTS}, {@code CREATE TABLE... IF
   * NOT EXISTS}), for which Cassandra doesn't return an {@code [applied]} column.
   *
   * <p>Note that, for versions of Cassandra strictly lower than 2.1.0-rc2, a server-side bug (<a
   * href="https://issues.apache.org/jira/browse/CASSANDRA-7337">CASSANDRA-7337</a>) causes this
   * method to always return {@code true} for batches containing conditional queries.
   *
   * <p>This method always return the same value for all results in the result set.
   *
   * @return {@code true} for non-conditional queries and for conditional queries that were
   *     successfully applied, {@code false} otherwise.
   */
  default boolean wasApplied() {
    return !getColumnDefinitions().contains("[applied]")
        || !getColumnDefinitions().get("[applied]").getType().equals(DataTypes.BOOLEAN)
        || getBoolean("[applied]");
  }
}
