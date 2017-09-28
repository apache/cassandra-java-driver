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
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestHandler;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;

public class CqlPrepareAsyncProcessor
    implements RequestProcessor<PrepareRequest, CompletionStage<PreparedStatement>> {

  private final ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache;

  public CqlPrepareAsyncProcessor(
      ConcurrentMap<ByteBuffer, DefaultPreparedStatement> preparedStatementsCache) {
    this.preparedStatementsCache = preparedStatementsCache;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof PrepareRequest && resultType.equals(PrepareRequest.ASYNC);
  }

  @Override
  public RequestHandler<PrepareRequest, CompletionStage<PreparedStatement>> newHandler(
      PrepareRequest request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new CqlPrepareAsyncHandler(
        request, preparedStatementsCache, session, context, sessionLogPrefix);
  }
}
