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

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * A session that offers user-friendly methods to execute CQL requests asynchronously.
 *
 * @since 4.4.0
 */
public interface AsyncCqlSession extends Session {

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   *
   * @param statement the CQL query to execute (that can be any {@code Statement}).
   * @return a {@code CompletionStage} that, once complete, will produce the async result set.
   */
  @NonNull
  default CompletionStage<AsyncResultSet> executeAsync(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, Statement.ASYNC), "The CQL processor should never return a null result");
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   *
   * <p>This is an alias for {@link #executeAsync(Statement)}
   * executeAsync(SimpleStatement.newInstance(query))}.
   *
   * @param query the CQL query to execute.
   * @return a {@code CompletionStage} that, once complete, will produce the async result set.
   * @see SimpleStatement#newInstance(String)
   */
  @NonNull
  default CompletionStage<AsyncResultSet> executeAsync(@NonNull String query) {
    return executeAsync(SimpleStatement.newInstance(query));
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   *
   * <p>This is an alias for {@link #executeAsync(Statement)}
   * executeAsync(SimpleStatement.newInstance(query, values))}.
   *
   * @param query the CQL query to execute.
   * @param values the values for placeholders in the query string. Individual values can be {@code
   *     null}, but the vararg array itself can't.
   * @return a {@code CompletionStage} that, once complete, will produce the async result set.
   * @see SimpleStatement#newInstance(String, Object...)
   */
  @NonNull
  default CompletionStage<AsyncResultSet> executeAsync(
      @NonNull String query, @NonNull Object... values) {
    return executeAsync(SimpleStatement.newInstance(query, values));
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   *
   * <p>This is an alias for {@link #executeAsync(Statement)}
   * executeAsync(SimpleStatement.newInstance(query, values))}.
   *
   * @param query the CQL query to execute.
   * @param values the values for named placeholders in the query string. Individual values can be
   *     {@code null}, but the map itself can't.
   * @return a {@code CompletionStage} that, once complete, will produce the async result set.
   * @see SimpleStatement#newInstance(String, Map)
   */
  @NonNull
  default CompletionStage<AsyncResultSet> executeAsync(
      @NonNull String query, @NonNull Map<String, Object> values) {
    return executeAsync(SimpleStatement.newInstance(query, values));
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
   *
   * <p>Note that the bound statements created from the resulting prepared statement will inherit
   * some of the attributes of {@code query}; see {@link SyncCqlSession#prepare(SimpleStatement)}
   * for more details.
   *
   * <p>The result of this method is cached (see {@link SyncCqlSession#prepare(SimpleStatement)} for
   * more explanations).
   *
   * @param statement the CQL query to prepare (that can be any {@code SimpleStatement}).
   * @return a {@code CompletionStage} that, once complete, will produce the prepared statement.
   */
  @NonNull
  default CompletionStage<PreparedStatement> prepareAsync(@NonNull SimpleStatement statement) {
    return Objects.requireNonNull(
        execute(new DefaultPrepareRequest(statement), PrepareRequest.ASYNC),
        "The CQL prepare processor should never return a null result");
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
   *
   * <p>The result of this method is cached (see {@link SyncCqlSession#prepare(SimpleStatement)} for
   * more explanations).
   *
   * @param query the CQL query string to prepare.
   * @return a {@code CompletionStage} that, once complete, will produce the prepared statement.
   */
  @NonNull
  default CompletionStage<PreparedStatement> prepareAsync(@NonNull String query) {
    return Objects.requireNonNull(
        execute(new DefaultPrepareRequest(query), PrepareRequest.ASYNC),
        "The CQL prepare processor should never return a null result");
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
   *
   * <p>This variant is exposed in case you use an ad hoc {@link PrepareRequest} implementation to
   * customize how attributes are propagated when you prepare a {@link SimpleStatement} (see {@link
   * SyncCqlSession#prepare(SimpleStatement)} for more explanations). Otherwise, you should rarely
   * have to deal with {@link PrepareRequest} directly.
   *
   * <p>The result of this method is cached (see {@link SyncCqlSession#prepare(SimpleStatement)} for
   * more explanations).
   *
   * @param request the {@code PrepareRequest} to prepare.
   * @return a {@code CompletionStage} that, once complete, will produce the prepared statement.
   */
  @NonNull
  default CompletionStage<PreparedStatement> prepareAsync(PrepareRequest request) {
    return Objects.requireNonNull(
        execute(request, PrepareRequest.ASYNC),
        "The CQL prepare processor should never return a null result");
  }
}
