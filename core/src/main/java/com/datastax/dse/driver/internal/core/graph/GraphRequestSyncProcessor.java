/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class GraphRequestSyncProcessor
    implements RequestProcessor<GraphStatement<?>, GraphResultSet> {

  private final GraphRequestAsyncProcessor asyncProcessor;

  public GraphRequestSyncProcessor(GraphRequestAsyncProcessor asyncProcessor) {
    this.asyncProcessor = asyncProcessor;
  }

  @Override
  public boolean canProcess(Request request, GenericType<?> resultType) {
    return request instanceof GraphStatement && resultType.equals(GraphStatement.SYNC);
  }

  @Override
  public GraphResultSet process(
      GraphStatement<?> request,
      DefaultSession session,
      InternalDriverContext context,
      String sessionLogPrefix) {
    BlockingOperation.checkNotDriverThread();
    AsyncGraphResultSet firstPage =
        CompletableFutures.getUninterruptibly(
            asyncProcessor.process(request, session, context, sessionLogPrefix));
    return GraphResultSets.toSync(firstPage);
  }

  @Override
  public GraphResultSet newFailure(RuntimeException error) {
    throw error;
  }
}
