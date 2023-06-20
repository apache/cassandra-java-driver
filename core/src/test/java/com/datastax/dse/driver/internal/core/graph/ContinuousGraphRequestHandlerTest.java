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
package com.datastax.dse.driver.internal.core.graph;

import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.createGraphBinaryModule;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.defaultDseFrameOf;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.tenGraphRows;
import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.DseTestDataProviders;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.cql.PoolBehavior;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.util.concurrent.CapturingTimer.CapturedTimeout;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class ContinuousGraphRequestHandlerTest {

  @Mock DefaultDriverContext mockContext;
  @Mock DefaultNode node;
  @Mock NodeMetricUpdater nodeMetricUpdater1;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(node.getMetricUpdater()).thenReturn(nodeMetricUpdater1);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_return_paged_results(GraphProtocol graphProtocol) throws IOException {
    String profileName = "test-graph";
    when(nodeMetricUpdater1.isEnabled(DseNodeMetric.GRAPH_MESSAGES, profileName)).thenReturn(true);

    GraphBinaryModule module = createGraphBinaryModule(mockContext);

    GraphRequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder().withGraphProtocolForTestConfig(graphProtocol);
    PoolBehavior node1Behavior = builder.customBehavior(node);

    try (RequestHandlerTestHarness harness = builder.build()) {

      GraphStatement<?> graphStatement =
          ScriptGraphStatement.newInstance("mockQuery").setExecutionProfileName(profileName);

      ContinuousGraphRequestHandler handler =
          new ContinuousGraphRequestHandler(
              graphStatement,
              harness.getSession(),
              harness.getContext(),
              "test",
              module,
              new GraphSupportChecker());

      // send the initial request
      CompletionStage<AsyncGraphResultSet> page1Future = handler.handle();

      node1Behavior.setResponseSuccess(
          defaultDseFrameOf(tenGraphRows(graphProtocol, module, 1, false)));

      assertThatStage(page1Future)
          .isSuccess(
              page1 -> {
                assertThat(page1.hasMorePages()).isTrue();
                assertThat(page1.currentPage()).hasSize(10).allMatch(GraphNode::isVertex);
                ExecutionInfo executionInfo = page1.getRequestExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node);
                assertThat(executionInfo.getErrors()).isEmpty();
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();
              });

      AsyncGraphResultSet page1 = CompletableFutures.getCompleted(page1Future);
      CompletionStage<AsyncGraphResultSet> page2Future = page1.fetchNextPage();

      node1Behavior.setResponseSuccess(
          defaultDseFrameOf(tenGraphRows(graphProtocol, module, 2, true)));

      assertThatStage(page2Future)
          .isSuccess(
              page2 -> {
                assertThat(page2.hasMorePages()).isFalse();
                assertThat(page2.currentPage()).hasSize(10).allMatch(GraphNode::isVertex);
                ExecutionInfo executionInfo = page2.getRequestExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node);
                assertThat(executionInfo.getErrors()).isEmpty();
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();
              });

      validateMetrics(profileName, harness);
    }
  }

  @Test
  public void should_honor_default_timeout() throws Exception {
    // given
    GraphBinaryModule binaryModule = createGraphBinaryModule(mockContext);
    Duration defaultTimeout = Duration.ofSeconds(1);

    RequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder().withGraphTimeout(defaultTimeout);
    PoolBehavior node1Behavior = builder.customBehavior(node);

    try (RequestHandlerTestHarness harness = builder.build()) {

      DriverExecutionProfile profile = harness.getContext().getConfig().getDefaultProfile();
      when(profile.isDefined(DseDriverOption.GRAPH_SUB_PROTOCOL)).thenReturn(true);
      when(profile.getString(DseDriverOption.GRAPH_SUB_PROTOCOL))
          .thenReturn(GraphProtocol.GRAPH_BINARY_1_0.toInternalCode());

      GraphStatement<?> graphStatement = ScriptGraphStatement.newInstance("mockQuery");

      // when
      ContinuousGraphRequestHandler handler =
          new ContinuousGraphRequestHandler(
              graphStatement,
              harness.getSession(),
              harness.getContext(),
              "test",
              binaryModule,
              new GraphSupportChecker());

      // send the initial request
      CompletionStage<AsyncGraphResultSet> page1Future = handler.handle();

      // acknowledge the write, will set the global timeout
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();

      CapturedTimeout globalTimeout = harness.nextScheduledTimeout();
      assertThat(globalTimeout.getDelay(TimeUnit.NANOSECONDS)).isEqualTo(defaultTimeout.toNanos());

      // will trigger the global timeout and complete it exceptionally
      globalTimeout.task().run(globalTimeout);
      assertThat(page1Future.toCompletableFuture()).isCompletedExceptionally();

      assertThatThrownBy(() -> page1Future.toCompletableFuture().get())
          .hasRootCauseExactlyInstanceOf(DriverTimeoutException.class)
          .hasMessageContaining("Query timed out after " + defaultTimeout);
    }
  }

  @Test
  public void should_honor_statement_timeout() throws Exception {
    // given
    GraphBinaryModule binaryModule = createGraphBinaryModule(mockContext);
    Duration defaultTimeout = Duration.ofSeconds(1);
    Duration statementTimeout = Duration.ofSeconds(2);

    RequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder().withGraphTimeout(defaultTimeout);
    PoolBehavior node1Behavior = builder.customBehavior(node);

    try (RequestHandlerTestHarness harness = builder.build()) {

      DriverExecutionProfile profile = harness.getContext().getConfig().getDefaultProfile();
      when(profile.isDefined(DseDriverOption.GRAPH_SUB_PROTOCOL)).thenReturn(true);
      when(profile.getString(DseDriverOption.GRAPH_SUB_PROTOCOL))
          .thenReturn(GraphProtocol.GRAPH_BINARY_1_0.toInternalCode());

      GraphStatement<?> graphStatement =
          ScriptGraphStatement.newInstance("mockQuery").setTimeout(statementTimeout);

      // when
      ContinuousGraphRequestHandler handler =
          new ContinuousGraphRequestHandler(
              graphStatement,
              harness.getSession(),
              harness.getContext(),
              "test",
              binaryModule,
              new GraphSupportChecker());

      // send the initial request
      CompletionStage<AsyncGraphResultSet> page1Future = handler.handle();

      // acknowledge the write, will set the global timeout
      node1Behavior.verifyWrite();
      node1Behavior.setWriteSuccess();

      CapturedTimeout globalTimeout = harness.nextScheduledTimeout();
      assertThat(globalTimeout.getDelay(TimeUnit.NANOSECONDS))
          .isEqualTo(statementTimeout.toNanos());

      // will trigger the global timeout and complete it exceptionally
      globalTimeout.task().run(globalTimeout);
      assertThat(page1Future.toCompletableFuture()).isCompletedExceptionally();

      assertThatThrownBy(() -> page1Future.toCompletableFuture().get())
          .hasRootCauseExactlyInstanceOf(DriverTimeoutException.class)
          .hasMessageContaining("Query timed out after " + statementTimeout);
    }
  }

  private void validateMetrics(String profileName, RequestHandlerTestHarness harness) {
    // GRAPH_MESSAGES metrics update is invoked only for the first page
    verify(nodeMetricUpdater1, times(1))
        .updateTimer(
            eq(DseNodeMetric.GRAPH_MESSAGES), eq(profileName), anyLong(), eq(TimeUnit.NANOSECONDS));
    verifyNoMoreInteractions(nodeMetricUpdater1);

    verify(harness.getSession().getMetricUpdater())
        .updateTimer(
            eq(DseSessionMetric.GRAPH_REQUESTS), eq(null), anyLong(), eq(TimeUnit.NANOSECONDS));
    verifyNoMoreInteractions(harness.getSession().getMetricUpdater());
  }
}
