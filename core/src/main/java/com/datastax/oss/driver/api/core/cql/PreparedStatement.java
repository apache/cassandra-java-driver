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
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * A query with bind variables that has been pre-parsed by the database.
 *
 * <p>Client applications create instances with {@link CqlSession#prepare(SimpleStatement)}. Then
 * they use {@link #bind(Object...)} to obtain an executable {@link BoundStatement}.
 *
 * <p>The default prepared statement implementation returned by the driver is <b>thread-safe</b>.
 * Client applications can -- and are expected to -- prepare each query once and store the result in
 * a place where it can be accessed concurrently by application threads (for example a final field).
 * Preparing the same query string twice is suboptimal and a bad practice, and will cause the driver
 * to log a warning.
 */
public interface PreparedStatement {

  /**
   * A unique identifier for this prepared statement.
   *
   * <p>Note: the returned buffer is read-only.
   */
  @NonNull
  ByteBuffer getId();

  @NonNull
  String getQuery();

  /** A description of the bind variables of this prepared statement. */
  @NonNull
  ColumnDefinitions getVariableDefinitions();

  /**
   * The indices of the variables in {@link #getVariableDefinitions()} that correspond to the target
   * table's partition key.
   *
   * <p>This is only present if all the partition key columns are expressed as bind variables.
   * Otherwise, the list will be empty. For example, given the following schema:
   *
   * <pre>
   *   CREATE TABLE foo (pk1 int, pk2 int, cc int, v int, PRIMARY KEY ((pk1, pk2), cc));
   * </pre>
   *
   * And the following definitions:
   *
   * <pre>
   * PreparedStatement ps1 = session.prepare("UPDATE foo SET v = ? WHERE pk1 = ? AND pk2 = ? AND v = ?");
   * PreparedStatement ps2 = session.prepare("UPDATE foo SET v = ? WHERE pk1 = 1 AND pk2 = ? AND v = ?");
   * </pre>
   *
   * Then {@code ps1.getPartitionKeyIndices()} contains 1 and 2, and {@code
   * ps2.getPartitionKeyIndices()} is empty (because one of the partition key components is
   * hard-coded in the query string).
   */
  @NonNull
  List<Integer> getPartitionKeyIndices();

  /**
   * A unique identifier for result metadata (essentially a hash of {@link
   * #getResultSetDefinitions()}).
   *
   * <p>This information is mostly for internal use: with protocol {@link DefaultProtocolVersion#V5}
   * or higher, the driver sends it with every execution of the prepared statement, to validate that
   * its result metadata is still up-to-date.
   *
   * <p>Note: this method returns {@code null} for protocol {@link DefaultProtocolVersion#V4} or
   * lower; otherwise, the returned buffer is read-only.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-10786">CASSANDRA-10786</a>
   */
  @Nullable
  ByteBuffer getResultMetadataId();

  /**
   * A description of the result set that will be returned when this prepared statement is bound and
   * executed.
   *
   * <p>This information is only present for {@code SELECT} queries, otherwise it is always empty.
   * Note that this is slightly incorrect for conditional updates (e.g. {@code INSERT ... IF NOT
   * EXISTS}), which do return columns; for those cases, use {@link
   * ResultSet#getColumnDefinitions()} on the result, not this method.
   */
  @NonNull
  ColumnDefinitions getResultSetDefinitions();

  /**
   * Updates {@link #getResultMetadataId()} and {@link #getResultSetDefinitions()} atomically.
   *
   * <p>This is for internal use by the driver. Calling this manually with incorrect information can
   * cause existing queries to fail.
   */
  void setResultMetadata(
      @NonNull ByteBuffer newResultMetadataId, @NonNull ColumnDefinitions newResultSetDefinitions);

  /**
   * Builds an executable statement that associates a set of values with the bind variables.
   *
   * <p>Note that the built-in bound statement implementation is immutable. If you need to set
   * multiple execution parameters on the bound statement (such as {@link
   * BoundStatement#setExecutionProfileName(String)}, {@link
   * BoundStatement#setPagingState(ByteBuffer)}, etc.), consider using {@link
   * #boundStatementBuilder(Object...)} instead to avoid unnecessary allocations.
   *
   * @param values the values of the bound variables in the statement. You can provide less values
   *     than the actual number of variables (or even none at all), in which case the remaining
   *     variables will be left unset. However, this method will throw an {@link
   *     IllegalArgumentException} if there are more values than variables. Individual values can be
   *     {@code null}, but the vararg array itself can't.
   */
  @NonNull
  BoundStatement bind(@NonNull Object... values);

  /**
   * Returns a builder to construct an executable statement.
   *
   * <p>Note that this builder is mutable and not thread-safe.
   *
   * @see #bind(Object...)
   */
  @NonNull
  BoundStatementBuilder boundStatementBuilder(@NonNull Object... values);
}
