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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.concurrent.CompletionStage;

/** A specialized session with convenience methods to execute CQL statements. */
public interface CqlSession extends Session {

  /** Returns a builder to create a new instance. */
  @NonNull
  static CqlSessionBuilder builder() {
    return new CqlSessionBuilder();
  }

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   */
  @NonNull
  default ResultSet execute(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, Statement.SYNC), "The CQL processor should never return a null result");
  }

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   */
  @NonNull
  default ResultSet execute(@NonNull String query) {
    return execute(SimpleStatement.newInstance(query));
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   */
  @NonNull
  default CompletionStage<AsyncResultSet> executeAsync(@NonNull Statement<?> statement) {
    return Objects.requireNonNull(
        execute(statement, Statement.ASYNC), "The CQL processor should never return a null result");
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   */
  @NonNull
  default CompletionStage<AsyncResultSet> executeAsync(@NonNull String query) {
    return executeAsync(SimpleStatement.newInstance(query));
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
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
   */
  @NonNull
  default PreparedStatement prepare(@NonNull String query) {
    return Objects.requireNonNull(
        execute(new DefaultPrepareRequest(query), PrepareRequest.SYNC),
        "The CQL prepare processor should never return a null result");
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
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
   */
  @NonNull
  default CompletionStage<PreparedStatement> prepareAsync(@NonNull SimpleStatement statement) {
    return Objects.requireNonNull(
        execute(new DefaultPrepareRequest(statement), PrepareRequest.ASYNC),
        "The CQL prepare processor should never return a null result");
  }
}
