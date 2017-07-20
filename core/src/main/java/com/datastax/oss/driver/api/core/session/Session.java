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
package com.datastax.oss.driver.api.core.session;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import java.util.concurrent.CompletionStage;

/**
 * A nexus to send requests to a Cassandra cluster.
 *
 * <p>This is a high-level abstraction that can handle any kind of request (provided that you have
 * registered a custom request processor with the driver). However, for user-friendliness, we also
 * expose overrides for the standard CQL requests that are supported out of the box.
 */
public interface Session extends AsyncAutoCloseable {

  /**
   * The keyspace that this session is currently connected to.
   *
   * <p>There are two ways that this can be set:
   *
   * <ul>
   *   <li>during initialization, if the session was created with {@link
   *       Cluster#connect(CqlIdentifier)} or {@link Cluster#connectAsync(CqlIdentifier)};
   *   <li>at runtime, if the client issues a request that changes the keyspace (such as a CQL
   *       {@code USE} query). Note that this second method is inherently unsafe, since other
   *       requests expecting the old keyspace might be executing concurrently. Therefore it is
   *       highly discouraged, aside from trivial cases (such as a cqlsh-style program where
   *       requests are never concurrent).
   * </ul>
   */
  CqlIdentifier getKeyspace();

  /**
   * Executes a request, and blocks until the result is available.
   *
   * @return a synchronous result, that provides immediate access to the data as soon as the method
   *     returns.
   */
  <SyncResultT, AsyncResultT> SyncResultT execute(Request<SyncResultT, AsyncResultT> request);

  /**
   * Executes a request, returning as soon as it has been scheduled, but generally before the result
   * is available.
   *
   * @return an asynchronous result, that represents the future completion of the request. The
   *     client either wait, or schedule a callback to be executed on completion (this is
   *     implementation-specific).
   */
  <SyncResultT, AsyncResultT> AsyncResultT executeAsync(Request<SyncResultT, AsyncResultT> request);

  /**
   * Executes a CQL statement synchronously.
   *
   * <p>This is a convenience method that does the exact same thing as {@link #execute(Request)},
   * but exposes a more user-friendly signature reminiscent of the 3.x API.
   */
  default ResultSet execute(Statement<?> statement) {
    return execute((Request<ResultSet, CompletionStage<AsyncResultSet>>) statement);
  }

  /**
   * Executes a CQL statement synchronously.
   *
   * <p>This is a convenience method that builds a {@link SimpleStatement#newInstance(String)
   * SimpleStatement} and passes it to {@link #execute(Request)}.
   */
  default ResultSet execute(String query) {
    return execute(SimpleStatement.newInstance(query));
  }

  /**
   * Executes a CQL statement asynchronously.
   *
   * <p>This is a convenience method that does the exact same thing as {@link
   * #executeAsync(Statement)}, but exposes a more user-friendly signature reminiscent of the 3.x
   * API.
   */
  default CompletionStage<AsyncResultSet> executeAsync(Statement<?> statement) {
    return executeAsync((Request<ResultSet, CompletionStage<AsyncResultSet>>) statement);
  }

  /**
   * Executes a CQL statement asynchronously.
   *
   * <p>This is a convenience method that builds a {@link SimpleStatement#newInstance(String)
   * SimpleStatement} and passes it to {@link #executeAsync(Statement)}.
   */
  default CompletionStage<AsyncResultSet> executeAsync(String query) {
    return executeAsync(SimpleStatement.newInstance(query));
  }

  /**
   * Prepares a CQL statement synchronously.
   *
   * <p>This is a convenience method that builds a {@link PrepareRequest} and passes it to {@link
   * #execute(Request)}.
   */
  default PreparedStatement prepare(SimpleStatement query) {
    return execute(new DefaultPrepareRequest(query));
  }

  /**
   * Prepares a CQL statement synchronously.
   *
   * <p>This is a convenience method that builds a {@link PrepareRequest} and passes it to {@link
   * #execute(Request)}.
   */
  default PreparedStatement prepare(String query) {
    return execute(new DefaultPrepareRequest(query));
  }

  /**
   * Prepares a CQL statement asynchronously.
   *
   * <p>This is a convenience method that builds a {@link PrepareRequest} and passes it to {@link
   * #executeAsync(Request)}.
   */
  default CompletionStage<PreparedStatement> prepareAsync(String query) {
    return executeAsync(new DefaultPrepareRequest(query));
  }

  /**
   * Prepares a CQL statement asynchronously.
   *
   * <p>This is a convenience method that builds a {@link PrepareRequest} and passes it to {@link
   * #executeAsync(Request)}.
   */
  default CompletionStage<PreparedStatement> prepareAsync(SimpleStatement query) {
    return executeAsync(new DefaultPrepareRequest(query));
  }
}
