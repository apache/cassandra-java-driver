/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph;

import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.createGraphBinaryModule;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.defaultDseFrameOf;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.tenGraphRows;
import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.AsyncGraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphExecutionInfo;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.dse.driver.internal.core.graph.GraphRequestHandlerTestHarness.Builder;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.oss.driver.api.core.DriverTimeoutException;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
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

  @Mock DseDriverContext mockContext;
  @Mock DefaultNode node;
  @Mock NodeMetricUpdater nodeMetricUpdater1;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(node.getMetricUpdater()).thenReturn(nodeMetricUpdater1);
  }

  @Test
  @UseDataProvider(location = GraphTestUtils.class, value = "supportedGraphProtocols")
  public void should_return_paged_results(GraphProtocol graphProtocol) throws IOException {

    GraphBinaryModule module = createGraphBinaryModule(mockContext);

    Builder builder =
        GraphRequestHandlerTestHarness.builder()
            .withGraphProtocolForTestConfig(graphProtocol.toInternalCode());
    PoolBehavior node1Behavior = builder.customBehavior(node);

    try (RequestHandlerTestHarness harness = builder.build()) {

      GraphStatement graphStatement =
          ScriptGraphStatement.newInstance("mockQuery").setExecutionProfileName("test-graph");

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
                GraphExecutionInfo executionInfo = page1.getExecutionInfo();
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
                GraphExecutionInfo executionInfo = page2.getExecutionInfo();
                assertThat(executionInfo.getCoordinator()).isEqualTo(node);
                assertThat(executionInfo.getErrors()).isEmpty();
                assertThat(executionInfo.getIncomingPayload()).isEmpty();
                assertThat(executionInfo.getSpeculativeExecutionCount()).isEqualTo(0);
                assertThat(executionInfo.getSuccessfulExecutionIndex()).isEqualTo(0);
                assertThat(executionInfo.getWarnings()).isEmpty();
              });
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

      GraphStatement graphStatement = ScriptGraphStatement.newInstance("mockQuery");

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
      assertThat(page1Future.toCompletableFuture())
          .hasFailedWithThrowableThat()
          .isInstanceOf(DriverTimeoutException.class)
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

      GraphStatement graphStatement =
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
      assertThat(page1Future.toCompletableFuture())
          .hasFailedWithThrowableThat()
          .isInstanceOf(DriverTimeoutException.class)
          .hasMessageContaining("Query timed out after " + statementTimeout);
    }
  }
}
