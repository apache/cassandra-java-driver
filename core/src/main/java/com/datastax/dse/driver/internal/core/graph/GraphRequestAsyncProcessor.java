/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class GraphRequestAsyncProcessor
    implements RequestProcessor<GraphStatement<?>, CompletionStage<AsyncGraphResultSet>> {

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof GraphStatement && resultType.equals(GraphStatement.ASYNC);
  }

  @Override
  public CompletionStage<AsyncGraphResultSet> process(
      GraphStatement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    return new GraphRequestHandler(request, session, context, sessionLogPrefix).handle();
  }

  @Override
  public CompletionStage<AsyncGraphResultSet> newFailure(RuntimeException error) {
    return CompletableFutures.failedFuture(error);
  }
}
