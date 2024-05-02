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
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.servererrors.QueryExecutionException;
import com.datastax.oss.driver.api.core.servererrors.QueryValidationException;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;

/**
 * A session that offers user-friendly methods to execute CQL requests synchronously.
 *
 * @since 4.4.0
 */
public interface SyncCqlSession extends Session {

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   *
   * @param statement the CQL query to execute (that can be any {@link Statement}).
   * @return the result of the query. That result will never be null but can be empty (and will be
   *     for any non SELECT query).
   * @throws AllNodesFailedException if no host in the cluster can be contacted successfully to
   *     execute this query.
   * @throws QueryExecutionException if the query triggered an execution exception, i.e. an
   *     exception thrown by Cassandra when it cannot execute the query with the requested
   *     consistency level successfully.
   * @throws QueryValidationException if the query is invalid (syntax error, unauthorized or any
   *     other validation problem).
   */
  @NonNull
  default ResultSet execute(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, Statement.SYNC), "The CQL processor should never return a null result");
  }

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   *
   * <p>This is an alias for {@link #execute(Statement)
   * execute(SimpleStatement.newInstance(query))}.
   *
   * @param query the CQL query to execute.
   * @return the result of the query. That result will never be null but can be empty (and will be
   *     for any non SELECT query).
   * @throws AllNodesFailedException if no host in the cluster can be contacted successfully to
   *     execute this query.
   * @throws QueryExecutionException if the query triggered an execution exception, i.e. an
   *     exception thrown by Cassandra when it cannot execute the query with the requested
   *     consistency level successfully.
   * @throws QueryValidationException if the query if invalid (syntax error, unauthorized or any
   *     other validation problem).
   * @see SimpleStatement#newInstance(String)
   */
  @NonNull
  default ResultSet execute(@NonNull String query) {
    return execute(SimpleStatement.newInstance(query));
  }

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   *
   * <p>This is an alias for {@link #execute(Statement) execute(SimpleStatement.newInstance(query,
   * values))}.
   *
   * @param query the CQL query to execute.
   * @param values the values for placeholders in the query string. Individual values can be {@code
   *     null}, but the vararg array itself can't.
   * @return the result of the query. That result will never be null but can be empty (and will be
   *     for any non SELECT query).
   * @throws AllNodesFailedException if no host in the cluster can be contacted successfully to
   *     execute this query.
   * @throws QueryExecutionException if the query triggered an execution exception, i.e. an
   *     exception thrown by Cassandra when it cannot execute the query with the requested
   *     consistency level successfully.
   * @throws QueryValidationException if the query if invalid (syntax error, unauthorized or any
   *     other validation problem).
   * @see SimpleStatement#newInstance(String, Object...)
   */
  @NonNull
  default ResultSet execute(@NonNull String query, @NonNull Object... values) {
    return execute(SimpleStatement.newInstance(query, values));
  }

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   *
   * <p>This is an alias for {@link #execute(Statement) execute(SimpleStatement.newInstance(query,
   * values))}.
   *
   * @param query the CQL query to execute.
   * @param values the values for named placeholders in the query string. Individual values can be
   *     {@code null}, but the map itself can't.
   * @return the result of the query. That result will never be null but can be empty (and will be
   *     for any non SELECT query).
   * @throws AllNodesFailedException if no host in the cluster can be contacted successfully to
   *     execute this query.
   * @throws QueryExecutionException if the query triggered an execution exception, i.e. an
   *     exception thrown by Cassandra when it cannot execute the query with the requested
   *     consistency level successfully.
   * @throws QueryValidationException if the query if invalid (syntax error, unauthorized or any
   *     other validation problem).
   * @see SimpleStatement#newInstance(String, Map)
   */
  @NonNull
  default ResultSet execute(@NonNull String query, @NonNull Map<String, Object> values) {
    return execute(SimpleStatement.newInstance(query, values));
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
   *
   * <p>Note that the bound statements created from the resulting prepared statement will inherit
   * some of the attributes of the provided simple statement. That is, given:
   *
   * <pre>{@code
   * SimpleStatement simpleStatement = SimpleStatement.newInstance("...");
   * PreparedStatement preparedStatement = session.prepare(simpleStatement);
   * BoundStatement boundStatement = preparedStatement.bind();
   * }</pre>
   *
   * Then:
   *
   * <ul>
   *   <li>the following methods return the same value as their counterpart on {@code
   *       simpleStatement}:
   *       <ul>
   *         <li>{@link Request#getExecutionProfileName() boundStatement.getExecutionProfileName()}
   *         <li>{@link Request#getExecutionProfile() boundStatement.getExecutionProfile()}
   *         <li>{@link Statement#getPagingState() boundStatement.getPagingState()}
   *         <li>{@link Request#getRoutingKey() boundStatement.getRoutingKey()}
   *         <li>{@link Request#getRoutingToken() boundStatement.getRoutingToken()}
   *         <li>{@link Request#getCustomPayload() boundStatement.getCustomPayload()}
   *         <li>{@link Request#isIdempotent() boundStatement.isIdempotent()}
   *         <li>{@link Request#getTimeout() boundStatement.getTimeout()}
   *         <li>{@link Statement#getPagingState() boundStatement.getPagingState()}
   *         <li>{@link Statement#getPageSize() boundStatement.getPageSize()}
   *         <li>{@link Statement#getConsistencyLevel() boundStatement.getConsistencyLevel()}
   *         <li>{@link Statement#getSerialConsistencyLevel()
   *             boundStatement.getSerialConsistencyLevel()}
   *         <li>{@link Statement#isTracing() boundStatement.isTracing()}
   *       </ul>
   *   <li>{@link Request#getRoutingKeyspace() boundStatement.getRoutingKeyspace()} is set from
   *       either {@link Request#getKeyspace() simpleStatement.getKeyspace()} (if it's not {@code
   *       null}), or {@code simpleStatement.getRoutingKeyspace()};
   *   <li>on the other hand, the following attributes are <b>not</b> propagated:
   *       <ul>
   *         <li>{@link Statement#getQueryTimestamp() boundStatement.getQueryTimestamp()} will be
   *             set to {@link Statement#NO_DEFAULT_TIMESTAMP}, meaning that the value will be
   *             assigned by the session's timestamp generator.
   *         <li>{@link Statement#getNode() boundStatement.getNode()} will always be {@code null}.
   *         <li>{@link Statement#getNowInSeconds()} boundStatement.getNowInSeconds()} will always
   *             be equal to {@link Statement#NO_NOW_IN_SECONDS}.
   *       </ul>
   * </ul>
   *
   * If you want to customize this behavior, you can write your own implementation of {@link
   * PrepareRequest} and pass it to {@link #prepare(PrepareRequest)}.
   *
   * <p>The result of this method is cached: if you call it twice with the same {@link
   * SimpleStatement}, you will get the same {@link PreparedStatement} instance. We still recommend
   * keeping a reference to it (for example by caching it as a field in a DAO); if that's not
   * possible (e.g. if query strings are generated dynamically), it's OK to call this method every
   * time: there will just be a small performance overhead to check the internal cache. Note that
   * caching is based on:
   *
   * <ul>
   *   <li>the query string exactly as you provided it: the driver does not perform any kind of
   *       trimming or sanitizing.
   *   <li>all other execution parameters: for example, preparing two statements with identical
   *       query strings but different {@linkplain SimpleStatement#getConsistencyLevel() consistency
   *       levels} will yield distinct prepared statements.
   * </ul>
   *
   * @param statement the CQL query to execute (that can be any {@link SimpleStatement}).
   * @return the prepared statement corresponding to {@code statement}.
   * @throws SyntaxError if the syntax of the query to prepare is not correct.
   */
  @NonNull
  default PreparedStatement prepare(@NonNull SimpleStatement statement) {
    return Objects.requireNonNull(
        execute(new DefaultPrepareRequest(statement), PrepareRequest.SYNC),
        "The CQL prepare processor should never return a null result");
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
   *
   * <p>The result of this method is cached (see {@link #prepare(SimpleStatement)} for more
   * explanations).
   *
   * @param query the CQL string query to execute.
   * @return the prepared statement corresponding to {@code query}.
   * @throws SyntaxError if the syntax of the query to prepare is not correct.
   */
  @NonNull
  default PreparedStatement prepare(@NonNull String query) {
    return Objects.requireNonNull(
        execute(new DefaultPrepareRequest(query), PrepareRequest.SYNC),
        "The CQL prepare processor should never return a null result");
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
   *
   * <p>This variant is exposed in case you use an ad hoc {@link PrepareRequest} implementation to
   * customize how attributes are propagated when you prepare a {@link SimpleStatement} (see {@link
   * #prepare(SimpleStatement)} for more explanations). Otherwise, you should rarely have to deal
   * with {@link PrepareRequest} directly.
   *
   * <p>The result of this method is cached (see {@link #prepare(SimpleStatement)} for more
   * explanations).
   *
   * @param request the {@code PrepareRequest} to execute.
   * @return the prepared statement corresponding to {@code request}.
   * @throws SyntaxError if the syntax of the query to prepare is not correct.
   */
  @NonNull
  default PreparedStatement prepare(@NonNull PrepareRequest request) {
    return Objects.requireNonNull(
        execute(request, PrepareRequest.SYNC),
        "The CQL prepare processor should never return a null result");
  }
}
