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
  ByteBuffer getId();

  String getQuery();

  /** A description of the bind variables of this prepared statement. */
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
   * Then {@code ps1.getPrimaryKeyIndices()} contains 1 and 2, and {@code
   * ps2.getPrimaryKeyIndices()} is empty (because one of the partition key components is hard-coded
   * in the query string).
   */
  List<Integer> getPrimaryKeyIndices();

  /**
   * A description of the result set that will be returned when this prepared statement is bound and
   * executed.
   */
  ColumnDefinitions getResultSetDefinitions();

  /**
   * Builds an executable statement that associates a set of values with the bind variables.
   *
   * <p>You can provide less values than the actual number of variables (or even none at all), in
   * which case the remaining variables will be left unset. However, this method will throw an
   * {@link IllegalArgumentException} if there are more values than variables.
   *
   * <p>Note that the built-in bound statement implementation is immutable. If you need to set
   * multiple execution parameters on the bound statement (such as {@link
   * BoundStatement#setConfigProfileName(String)}, {@link
   * BoundStatement#setPagingState(ByteBuffer)}, etc.), consider using {@link
   * #boundStatementBuilder()} instead to avoid unnecessary allocations.
   */
  BoundStatement bind(Object... values);

  /**
   * Returns a builder to construct an executable statement.
   *
   * @see #bind(Object...)
   */
  BoundStatementBuilder boundStatementBuilder();
}
