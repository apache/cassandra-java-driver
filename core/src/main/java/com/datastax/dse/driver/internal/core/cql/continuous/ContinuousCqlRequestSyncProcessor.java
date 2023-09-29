/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.internal.core.cql.continuous;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousResultSet;
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
public class ContinuousCqlRequestSyncProcessor
    implements RequestProcessor<Statement<?>, ContinuousResultSet> {

  public static final GenericType<ContinuousResultSet> CONTINUOUS_RESULT_SYNC =
      GenericType.of(ContinuousResultSet.class);

  private final ContinuousCqlRequestAsyncProcessor asyncProcessor;

  public ContinuousCqlRequestSyncProcessor(ContinuousCqlRequestAsyncProcessor asyncProcessor) {
    this.asyncProcessor = asyncProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof Statement && resultType.equals(CONTINUOUS_RESULT_SYNC);
  }

  @Override
  public ContinuousResultSet process(
      Statement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    BlockingOperation.checkNotDriverThread();
    ContinuousAsyncResultSet firstPage =
        CompletableFutures.getUninterruptibly(
            asyncProcessor.process(request, session, context, sessionLogPrefix));
    return new DefaultContinuousResultSet(firstPage);
  }

  @Override
  public ContinuousResultSet newFailure(RuntimeException error) {
    throw error;
  }
}
