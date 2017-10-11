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
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.cql.DefaultPrepareRequest;
import com.datastax.oss.driver.internal.core.session.SessionWrapper;
import com.google.common.util.concurrent.ListenableFuture;

public class GuavaSession extends SessionWrapper {

  static final GenericType<ListenableFuture<AsyncResultSet>> ASYNC =
      new GenericType<ListenableFuture<AsyncResultSet>>() {};

  static final GenericType<ListenableFuture<PreparedStatement>> ASYNC_PREPARED =
      new GenericType<ListenableFuture<PreparedStatement>>() {};

  GuavaSession(Session delegate) {
    super(delegate);
  }

  ListenableFuture<AsyncResultSet> executeAsync(Statement<?> statement) {
    return this.execute(statement, ASYNC);
  }

  ListenableFuture<AsyncResultSet> executeAsync(String statement) {
    return this.executeAsync(SimpleStatement.newInstance(statement));
  }

  ListenableFuture<PreparedStatement> prepareAsync(SimpleStatement statement) {
    return this.execute(new DefaultPrepareRequest(statement), ASYNC_PREPARED);
  }

  ListenableFuture<PreparedStatement> prepareAsync(String statement) {
    return this.prepareAsync(SimpleStatement.newInstance(statement));
  }
}
