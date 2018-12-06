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

import com.datastax.oss.driver.api.core.cql.PrepareRequest;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.cache.Cache;
import java.util.concurrent.CompletableFuture;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class CqlPrepareSyncProcessor
    implements RequestProcessor<PrepareRequest, PreparedStatement> {

  private final CqlPrepareAsyncProcessor asyncProcessor;

  /**
   * Note: if you also register a {@link CqlPrepareAsyncProcessor} with your session, make sure that
   * you pass that same instance to this constructor. This is necessary for proper behavior of the
   * prepared statement cache.
   */
  public CqlPrepareSyncProcessor(CqlPrepareAsyncProcessor asyncProcessor) {
    this.asyncProcessor = asyncProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof PrepareRequest && resultType.equals(PrepareRequest.SYNC);
  }

  @Override
  public PreparedStatement process(
      PrepareRequest request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {

    BlockingOperation.checkNotDriverThread();
    return CompletableFutures.getUninterruptibly(
        asyncProcessor.process(request, session, context, sessionLogPrefix));
  }

  public Cache<PrepareRequest, CompletableFuture<PreparedStatement>> getCache() {
    return asyncProcessor.getCache();
  }

  @Override
  public PreparedStatement newFailure(RuntimeException error) {
    throw error;
  }
}
