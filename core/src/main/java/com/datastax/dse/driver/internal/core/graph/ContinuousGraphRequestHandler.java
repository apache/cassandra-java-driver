/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphExecutionInfo;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.internal.core.ContinuousRequestHandlerBase;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.dse.protocol.internal.response.result.DseRowsMetadata;
import com.datastax.oss.driver.api.core.session.throttling.Throttled;
import com.datastax.oss.driver.internal.core.channel.ResponseCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.result.Rows;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import net.jcip.annotations.ThreadSafe;

/**
 * Handles a request that supports multiple response messages (a.k.a. continuous paging request).
 */
@ThreadSafe
public class ContinuousGraphRequestHandler
    extends ContinuousRequestHandlerBase<GraphStatement<?>, AsyncGraphResultSet, GraphExecutionInfo>
    implements ResponseCallback, GenericFutureListener<Future<java.lang.Void>>, Throttled {
  private final Message message;
  private final GraphProtocol subProtocol;
  private final GraphBinaryModule graphBinaryModule;

  ContinuousGraphRequestHandler(
      @NonNull GraphStatement<?> statement,
      @NonNull DefaultSession session,
      @NonNull InternalDriverContext context,
      @NonNull String sessionLogPrefix,
      @NonNull GraphBinaryModule graphBinaryModule) {
    super(statement, session, context, sessionLogPrefix, AsyncGraphResultSet.class);
    this.graphBinaryModule = graphBinaryModule;
    this.subProtocol = GraphConversions.inferSubProtocol(statement, executionProfile);
    this.message =
        GraphConversions.createContinuousMessageFromGraphStatement(
            statement, subProtocol, executionProfile, this.context, graphBinaryModule);
    throttler.register(this);
  }

  // MAIN LIFECYCLE

  @NonNull
  @Override
  protected Message getMessage() {
    return message;
  }

  @Override
  protected boolean isTracingEnabled() {
    return this.statement.isTracing();
  }

  @NonNull
  @Override
  protected Map<String, ByteBuffer> createPayload() {
    return GraphConversions.createCustomPayload(
        statement, subProtocol, executionProfile, context, graphBinaryModule);
  }

  @NonNull
  @Override
  protected AsyncGraphResultSet createEmptyResultSet(@NonNull GraphExecutionInfo executionInfo) {
    return ContinuousAsyncGraphResultSet.empty(executionInfo);
  }

  @NonNull
  @Override
  protected DefaultGraphExecutionInfo createExecutionInfo(
      @NonNull Result result, @Nullable Frame response) {
    return new DefaultGraphExecutionInfo(statement, node, 0, 0, errors, response);
  }

  @NonNull
  @Override
  protected ContinuousAsyncGraphResultSet createResultSet(
      @NonNull Rows rows, @NonNull GraphExecutionInfo executionInfo) throws IOException {

    Queue<GraphNode> graphNodes = new ArrayDeque<>();
    for (List<ByteBuffer> row : rows.getData()) {
      if (subProtocol.isGraphBinary()) {
        graphNodes.offer(GraphConversions.createGraphBinaryGraphNode(row, this.graphBinaryModule));
      } else {
        graphNodes.offer(GraphSONUtils.createGraphNode(row, subProtocol));
      }
    }

    DseRowsMetadata metadata = (DseRowsMetadata) rows.getMetadata();
    return new ContinuousAsyncGraphResultSet(
        executionInfo,
        graphNodes,
        metadata.continuousPageNumber,
        !metadata.isLastContinuousPage,
        this,
        subProtocol);
  }

  @Override
  protected int pageNumber(@NonNull AsyncGraphResultSet resultSet) {
    if (resultSet instanceof ContinuousAsyncGraphResultSet) {
      return ((ContinuousAsyncGraphResultSet) resultSet).pageNumber();
    } else { // otherwise the AsyncGraphResultSet is not a Continuous Paging Query
      return 1;
    }
  }
}
