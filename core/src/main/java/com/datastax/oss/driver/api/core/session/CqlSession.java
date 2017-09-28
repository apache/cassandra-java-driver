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

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import java.util.concurrent.CompletionStage;

/** A specialized session with convenience methods to execute CQL statements. */
public interface CqlSession extends Session {

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   */
  default ResultSet execute(Statement<?> statement) {
    return execute(statement, Statement.SYNC);
  }

  /**
   * Executes a CQL statement synchronously (the calling thread blocks until the result becomes
   * available).
   */
  default ResultSet execute(String query) {
    return execute(SimpleStatement.newInstance(query));
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   */
  default CompletionStage<AsyncResultSet> executeAsync(Statement<?> statement) {
    return execute(statement, Statement.ASYNC);
  }

  /**
   * Executes a CQL statement asynchronously (the call returns as soon as the statement was sent,
   * generally before the result is available).
   */
  default CompletionStage<AsyncResultSet> executeAsync(String query) {
    return executeAsync(SimpleStatement.newInstance(query));
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
   */
  default PreparedStatement prepare(SimpleStatement query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.SYNC);
  }

  /**
   * Prepares a CQL statement synchronously (the calling thread blocks until the statement is
   * prepared).
   */
  default PreparedStatement prepare(String query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.SYNC);
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
   */
  default CompletionStage<PreparedStatement> prepareAsync(String query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.ASYNC);
  }

  /**
   * Prepares a CQL statement asynchronously (the call returns as soon as the prepare query was
   * sent, generally before the statement is prepared).
   */
  default CompletionStage<PreparedStatement> prepareAsync(SimpleStatement query) {
    return execute(new DefaultPrepareRequest(query), PrepareRequest.ASYNC);
  }
}
