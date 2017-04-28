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

import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement;
import java.util.Collections;
import java.util.concurrent.CompletionStage;

public interface CqlSession extends Session {

  // Not strictly needed, but it shows up as a more user-friendly signature in IDE completion
  default ResultSet execute(Statement statement) {
    return execute((Request<ResultSet, CompletionStage<AsyncResultSet>>) statement);
  }

  // Not strictly needed, but it shows up as a more user-friendly signature in IDE completion
  default CompletionStage<AsyncResultSet> executeAsync(Statement statement) {
    return executeAsync((Request<ResultSet, CompletionStage<AsyncResultSet>>) statement);
  }

  default ResultSet execute(String query) {
    return execute(new DefaultSimpleStatement(query, Collections.emptyList(), null));
  }

  default CompletionStage<AsyncResultSet> executeAsync(String query) {
    return executeAsync(new DefaultSimpleStatement(query, Collections.emptyList(), null));
  }

  default PreparedStatement prepare(String query) {
    return execute(PrepareRequest.from(query));
  }

  default PreparedStatement prepare(Statement query) {
    return execute(PrepareRequest.from(query));
  }

  default CompletionStage<PreparedStatement> prepareAsync(String query) {
    return executeAsync(PrepareRequest.from(query));
  }

  default CompletionStage<PreparedStatement> prepareAsync(Statement query) {
    return executeAsync(PrepareRequest.from(query));
  }
}
