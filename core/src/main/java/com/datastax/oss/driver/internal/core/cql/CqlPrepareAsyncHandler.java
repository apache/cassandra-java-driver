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

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestHandler;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;

public class CqlPrepareAsyncHandler extends CqlPrepareHandlerBase
    implements RequestHandler<PrepareRequest, CompletionStage<PreparedStatement>> {

  CqlPrepareAsyncHandler(
      PrepareRequest request,
      ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    super(request, preparedStatementsCache, session, context, sessionLogPrefix);
  }

  @Override
  public CompletableFuture<PreparedStatement> handle() {
    return result;
  }
}
