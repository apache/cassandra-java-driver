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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousRequestHandlerBase;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.dse.protocol.internal.response.result.DseRowsMetadata;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.result.Rows;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;

/**
 * Handles a Graph request that supports multiple response messages (a.k.a. continuous paging
 * request).
 */
@ThreadSafe
public class ContinuousGraphRequestHandler
    extends ContinuousRequestHandlerBase<GraphStatement<?>, AsyncGraphResultSet> {

  private final GraphBinaryModule graphBinaryModule;
  private final GraphSupportChecker graphSupportChecker;
  private final Duration globalTimeout;

  ContinuousGraphRequestHandler(
      @Nonnull GraphStatement<?> statement,
      @Nonnull DefaultSession session,
      @Nonnull InternalDriverContext context,
      @Nonnull String sessionLogPrefix,
      @Nonnull GraphBinaryModule graphBinaryModule,
      @Nonnull GraphSupportChecker graphSupportChecker) {
    super(
        statement,
        session,
        context,
        sessionLogPrefix,
        AsyncGraphResultSet.class,
        true,
        DseSessionMetric.GRAPH_CLIENT_TIMEOUTS,
        DseSessionMetric.GRAPH_REQUESTS,
        DseNodeMetric.GRAPH_MESSAGES);
    this.graphBinaryModule = graphBinaryModule;
    this.graphSupportChecker = graphSupportChecker;
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    globalTimeout =
        MoreObjects.firstNonNull(
            statement.getTimeout(),
            executionProfile.getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO));
    // NOTE that ordering of the following statement matters.
    // We should register this request after all fields have been initialized.
    throttler.register(this);
  }

  @Nonnull
  @Override
  protected Duration getGlobalTimeout() {
    return globalTimeout;
  }

  @Nonnull
  @Override
  protected Duration getPageTimeout(@Nonnull GraphStatement<?> statement, int pageNumber) {
    return Duration.ZERO;
  }

  @Nonnull
  @Override
  protected Duration getReviseRequestTimeout(@Nonnull GraphStatement<?> statement) {
    return Duration.ZERO;
  }

  @Override
  protected int getMaxEnqueuedPages(@Nonnull GraphStatement<?> statement) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    return executionProfile.getInt(DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_ENQUEUED_PAGES);
  }

  @Override
  protected int getMaxPages(@Nonnull GraphStatement<?> statement) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    return executionProfile.getInt(DseDriverOption.GRAPH_CONTINUOUS_PAGING_MAX_PAGES);
  }

  @Nonnull
  @Override
  protected Message getMessage(@Nonnull GraphStatement<?> statement) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    GraphProtocol subProtocol =
        graphSupportChecker.inferGraphProtocol(statement, executionProfile, context);
    return GraphConversions.createContinuousMessageFromGraphStatement(
        statement, subProtocol, executionProfile, context, graphBinaryModule);
  }

  @Override
  protected boolean isTracingEnabled(@Nonnull GraphStatement<?> statement) {
    return statement.isTracing();
  }

  @Nonnull
  @Override
  protected Map<String, ByteBuffer> createPayload(@Nonnull GraphStatement<?> statement) {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    GraphProtocol subProtocol =
        graphSupportChecker.inferGraphProtocol(statement, executionProfile, context);
    return GraphConversions.createCustomPayload(
        statement, subProtocol, executionProfile, context, graphBinaryModule);
  }

  @Nonnull
  @Override
  protected AsyncGraphResultSet createEmptyResultSet(@Nonnull ExecutionInfo executionInfo) {
    return ContinuousAsyncGraphResultSet.empty(executionInfo);
  }

  @Nonnull
  @Override
  protected ContinuousAsyncGraphResultSet createResultSet(
      @Nonnull GraphStatement<?> statement,
      @Nonnull Rows rows,
      @Nonnull ExecutionInfo executionInfo,
      @Nonnull ColumnDefinitions columnDefinitions)
      throws IOException {
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(statement, context);
    GraphProtocol subProtocol =
        graphSupportChecker.inferGraphProtocol(statement, executionProfile, context);

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
  protected int pageNumber(@Nonnull AsyncGraphResultSet resultSet) {
    return ((ContinuousAsyncGraphResultSet) resultSet).pageNumber();
  }
}
