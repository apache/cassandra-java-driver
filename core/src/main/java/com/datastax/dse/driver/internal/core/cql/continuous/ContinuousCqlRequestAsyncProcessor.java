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
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ContinuousCqlRequestAsyncProcessor
    implements RequestProcessor<Statement<?>, CompletionStage<ContinuousAsyncResultSet>> {

  public static final GenericType<CompletionStage<ContinuousAsyncResultSet>>
      CONTINUOUS_RESULT_ASYNC = new GenericType<CompletionStage<ContinuousAsyncResultSet>>() {};

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof Statement && resultType.equals(CONTINUOUS_RESULT_ASYNC);
  }

  @Override
  public CompletionStage<ContinuousAsyncResultSet> process(
      Statement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new ContinuousCqlRequestHandler(request, session, context, sessionLogPrefix).handle();
  }

  @Override
  public CompletionStage<ContinuousAsyncResultSet> newFailure(RuntimeException error) {
    return CompletableFutures.failedFuture(error);
  }
}
