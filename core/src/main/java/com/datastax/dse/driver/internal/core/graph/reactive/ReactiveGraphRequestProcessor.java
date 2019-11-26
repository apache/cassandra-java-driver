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
package com.datastax.dse.driver.internal.core.graph.reactive;

import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphResultSet;
import com.datastax.dse.driver.internal.core.graph.GraphRequestAsyncProcessor;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ReactiveGraphRequestProcessor
    implements RequestProcessor<GraphStatement<?>, ReactiveGraphResultSet> {

  public static final GenericType<ReactiveGraphResultSet> REACTIVE_GRAPH_RESULT_SET =
      GenericType.of(ReactiveGraphResultSet.class);

  private final GraphRequestAsyncProcessor asyncGraphProcessor;

  public ReactiveGraphRequestProcessor(@NonNull GraphRequestAsyncProcessor asyncGraphProcessor) {
    this.asyncGraphProcessor = asyncGraphProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof GraphStatement && resultType.equals(REACTIVE_GRAPH_RESULT_SET);
  }

  @Override
  public ReactiveGraphResultSet process(
      GraphStatement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new DefaultReactiveGraphResultSet(
        () -> asyncGraphProcessor.process(request, session, context, sessionLogPrefix));
  }

  @Override
  public ReactiveGraphResultSet newFailure(RuntimeException error) {
    return new FailedReactiveGraphResultSet(error);
  }
}
