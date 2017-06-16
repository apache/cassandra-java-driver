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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestHandler;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import java.util.concurrent.CompletionStage;

public class CqlRequestProcessor
    implements RequestProcessor<ResultSet, CompletionStage<AsyncResultSet>> {

  @Override
  public <T extends Request<?, ?>> boolean canProcess(T request) {
    return request instanceof Statement;
  }

  @Override
  public RequestHandler<ResultSet, CompletionStage<AsyncResultSet>> newHandler(
      Request<ResultSet, CompletionStage<AsyncResultSet>> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new CqlRequestHandler((Statement) request, session, context, sessionLogPrefix);
  }
}
