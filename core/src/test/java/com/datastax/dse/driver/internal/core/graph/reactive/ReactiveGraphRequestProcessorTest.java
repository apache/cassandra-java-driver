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
package com.datastax.dse.driver.internal.core.graph.reactive;

import static com.datastax.dse.driver.api.core.DseProtocolVersion.DSE_V1;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.createGraphBinaryModule;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.defaultDseFrameOf;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.tenGraphRows;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.DseTestDataProviders;
import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphNode;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphResultSet;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestHandlerTestBase;
import com.datastax.dse.driver.internal.core.graph.GraphProtocol;
import com.datastax.dse.driver.internal.core.graph.GraphRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.graph.GraphRequestHandlerTestHarness;
import com.datastax.dse.driver.internal.core.graph.GraphSupportChecker;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.PoolBehavior;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import io.reactivex.Flowable;
import java.io.IOException;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ReactiveGraphRequestProcessorTest extends ContinuousCqlRequestHandlerTestBase {

  private GraphRequestAsyncProcessor asyncProcessor;
  private GraphSupportChecker graphSupportChecker;

  @Before
  public void setUp() {
    DefaultDriverContext context = mock(DefaultDriverContext.class);
    graphSupportChecker = mock(GraphSupportChecker.class);
    asyncProcessor = Mockito.spy(new GraphRequestAsyncProcessor(context, graphSupportChecker));
  }

  @Test
  public void should_be_able_to_process_graph_reactive_result_set() {
    ReactiveGraphRequestProcessor processor = new ReactiveGraphRequestProcessor(asyncProcessor);
    assertThat(
            processor.canProcess(
                ScriptGraphStatement.newInstance("g.V()"),
                ReactiveGraphRequestProcessor.REACTIVE_GRAPH_RESULT_SET))
        .isTrue();
  }

  @Test
  public void should_create_reactive_result_set() {
    GraphRequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder().withProtocolVersion(DSE_V1);
    try (GraphRequestHandlerTestHarness harness = builder.build()) {
      ReactiveGraphRequestProcessor processor = new ReactiveGraphRequestProcessor(asyncProcessor);
      GraphStatement<?> graphStatement = ScriptGraphStatement.newInstance("g.V()");
      assertThat(
              processor.process(graphStatement, harness.getSession(), harness.getContext(), "test"))
          .isInstanceOf(DefaultReactiveGraphResultSet.class);
    }
  }

  @Test
  @UseDataProvider(
      value = "allDseProtocolVersionsAndSupportedGraphProtocols",
      location = DseTestDataProviders.class)
  public void should_complete_single_page_result(
      DseProtocolVersion version, GraphProtocol graphProtocol) throws IOException {
    when(graphSupportChecker.isPagingEnabled(any(), any())).thenReturn(false);
    when(graphSupportChecker.inferGraphProtocol(any(), any(), any())).thenReturn(graphProtocol);

    GraphRequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (GraphRequestHandlerTestHarness harness = builder.build()) {

      DefaultSession session = harness.getSession();
      DefaultDriverContext context = harness.getContext();
      GraphStatement<?> graphStatement = ScriptGraphStatement.newInstance("g.V()");

      GraphBinaryModule graphBinaryModule = createGraphBinaryModule(context);
      when(asyncProcessor.getGraphBinaryModule()).thenReturn(graphBinaryModule);

      ReactiveGraphResultSet publisher =
          new ReactiveGraphRequestProcessor(asyncProcessor)
              .process(graphStatement, session, context, "test");

      Flowable<ReactiveGraphNode> rowsPublisher = Flowable.fromPublisher(publisher).cache();
      rowsPublisher.subscribe();

      // emulate single page
      node1Behavior.setResponseSuccess(
          defaultDseFrameOf(tenGraphRows(graphProtocol, graphBinaryModule, 1, true)));

      List<ReactiveGraphNode> rows = rowsPublisher.toList().blockingGet();

      assertThat(rows).hasSize(10);
      checkResultSet(rows);

      Flowable<ExecutionInfo> execInfosFlowable =
          Flowable.fromPublisher(publisher.getExecutionInfos());
      assertThat(execInfosFlowable.toList().blockingGet())
          .hasSize(1)
          .containsExactly(rows.get(0).getExecutionInfo());
    }
  }

  @Test
  @UseDataProvider(
      value = "allDseProtocolVersionsAndSupportedGraphProtocols",
      location = DseTestDataProviders.class)
  public void should_complete_multi_page_result(
      DseProtocolVersion version, GraphProtocol graphProtocol) throws IOException {
    when(graphSupportChecker.isPagingEnabled(any(), any())).thenReturn(true);
    when(graphSupportChecker.inferGraphProtocol(any(), any(), any())).thenReturn(graphProtocol);

    GraphRequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder().withProtocolVersion(version);
    PoolBehavior node1Behavior = builder.customBehavior(node1);
    try (GraphRequestHandlerTestHarness harness = builder.build()) {

      DefaultSession session = harness.getSession();
      DefaultDriverContext context = harness.getContext();
      GraphStatement<?> graphStatement = ScriptGraphStatement.newInstance("g.V()");

      GraphBinaryModule graphBinaryModule = createGraphBinaryModule(context);
      when(asyncProcessor.getGraphBinaryModule()).thenReturn(graphBinaryModule);

      ReactiveGraphResultSet publisher =
          new ReactiveGraphRequestProcessor(asyncProcessor)
              .process(graphStatement, session, context, "test");

      Flowable<ReactiveGraphNode> rowsPublisher = Flowable.fromPublisher(publisher).cache();
      rowsPublisher.subscribe();

      // emulate page 1
      node1Behavior.setResponseSuccess(
          defaultDseFrameOf(tenGraphRows(graphProtocol, graphBinaryModule, 1, false)));
      // emulate page 2
      node1Behavior.setResponseSuccess(
          defaultDseFrameOf(tenGraphRows(graphProtocol, graphBinaryModule, 2, true)));

      List<ReactiveGraphNode> rows = rowsPublisher.toList().blockingGet();
      assertThat(rows).hasSize(20);
      checkResultSet(rows);

      Flowable<ExecutionInfo> execInfosFlowable =
          Flowable.fromPublisher(publisher.getExecutionInfos());
      assertThat(execInfosFlowable.toList().blockingGet())
          .hasSize(2)
          .containsExactly(rows.get(0).getExecutionInfo(), rows.get(10).getExecutionInfo());
    }
  }

  private void checkResultSet(List<ReactiveGraphNode> rows) {
    for (ReactiveGraphNode row : rows) {
      assertThat(row.isVertex()).isTrue();
      ExecutionInfo executionInfo = row.getExecutionInfo();
      assertThat(executionInfo.getCoordinator()).isEqualTo(node1);
      assertThat(executionInfo.getErrors()).isEmpty();
      assertThat(executionInfo.getIncomingPayload()).isEmpty();
      assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
      assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
      assertThat(executionInfo.getWarnings()).isEmpty();
    }
  }
}
