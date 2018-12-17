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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CqlRequestSyncProcessor implements RequestProcessor<Statement<?>, ResultSet> {

  private final CqlRequestAsyncProcessor asyncProcessor;

  public CqlRequestSyncProcessor(CqlRequestAsyncProcessor asyncProcessor) {
    this.asyncProcessor = asyncProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof Statement && resultType.equals(Statement.SYNC);
  }

  @Override
  public ResultSet process(
      Statement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    BlockingOperation.checkNotDriverThread();
    AsyncResultSet firstPage =
        CompletableFutures.getUninterruptibly(
            asyncProcessor.process(request, session, context, sessionLogPrefix));
    return ResultSets.newInstance(firstPage);
  }

  @Override
  public ResultSet newFailure(RuntimeException error) {
    throw error;
  }
}
