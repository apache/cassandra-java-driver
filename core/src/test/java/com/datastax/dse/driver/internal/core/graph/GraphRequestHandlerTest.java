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

import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPHSON_1_0;
import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPHSON_2_0;
import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPH_BINARY_1_0;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.createGraphBinaryModule;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.defaultDseFrameOf;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.serialize;
import static com.datastax.dse.driver.internal.core.graph.GraphTestUtils.singleGraphRow;
import static com.datastax.oss.driver.api.core.type.codec.TypeCodecs.BIGINT;
import static com.datastax.oss.driver.api.core.type.codec.TypeCodecs.TEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.DseTestDataProviders;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.metrics.DseNodeMetric;
import com.datastax.dse.driver.api.core.metrics.DseSessionMetric;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.dse.protocol.internal.request.RawBytesQuery;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.internal.core.cql.Conversions;
import com.datastax.oss.driver.internal.core.cql.PoolBehavior;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Query;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(DataProviderRunner.class)
public class GraphRequestHandlerTest {

  private static final Pattern LOG_PREFIX_PER_REQUEST = Pattern.compile("test-graph\\|\\d+");

  @Mock DefaultNode node;

  @Mock protected NodeMetricUpdater nodeMetricUpdater1;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    when(node.getMetricUpdater()).thenReturn(nodeMetricUpdater1);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_create_query_message_from_script_statement(GraphProtocol graphProtocol)
      throws IOException {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    ScriptGraphStatement graphStatement =
        ScriptGraphStatement.newInstance("mockQuery")
            .setQueryParam("p1", 1L)
            .setQueryParam("p2", Uuids.random());

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement, graphProtocol, executionProfile, harness.getContext(), module);

    // checks
    assertThat(m).isInstanceOf(Query.class);
    Query q = ((Query) m);
    assertThat(q.query).isEqualTo("mockQuery");
    assertThat(q.options.positionalValues)
        .containsExactly(serialize(graphStatement.getQueryParams(), graphProtocol, module));
    assertThat(q.options.namedValues).isEmpty();
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_create_query_message_from_fluent_statement(GraphProtocol graphProtocol)
      throws IOException {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphTraversal<?, ?> traversalTest =
        DseGraph.g.V().has("person", "name", "marko").has("p1", 1L).has("p2", Uuids.random());
    GraphStatement<?> graphStatement = FluentGraphStatement.newInstance(traversalTest);

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement, graphProtocol, executionProfile, harness.getContext(), module);

    Map<String, ByteBuffer> createdCustomPayload =
        GraphConversions.createCustomPayload(
            graphStatement, graphProtocol, executionProfile, harness.getContext(), module);

    // checks
    assertThat(m).isInstanceOf(RawBytesQuery.class);
    testQueryRequestAndPayloadContents(
        ((RawBytesQuery) m),
        createdCustomPayload,
        GraphConversions.bytecodeToSerialize(graphStatement),
        graphProtocol,
        module);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_create_query_message_from_batch_statement(GraphProtocol graphProtocol)
      throws IOException {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    @SuppressWarnings("rawtypes")
    List<GraphTraversal> traversalsTest =
        ImmutableList.of(
            // randomly testing some complex data types. Complete suite of data types test is in
            // GraphDataTypesTest
            DseGraph.g
                .addV("person")
                .property("p1", 2.3f)
                .property("p2", LocalDateTime.now(ZoneOffset.UTC)),
            DseGraph.g
                .addV("software")
                .property("p3", new BigInteger("123456789123456789123456789123456789"))
                .property("p4", ImmutableList.of(Point.fromCoordinates(30.4, 25.63746284))));
    GraphStatement<?> graphStatement =
        BatchGraphStatement.builder().addTraversals(traversalsTest).build();

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement, graphProtocol, executionProfile, harness.getContext(), module);

    Map<String, ByteBuffer> createdCustomPayload =
        GraphConversions.createCustomPayload(
            graphStatement, graphProtocol, executionProfile, harness.getContext(), module);

    // checks
    assertThat(m).isInstanceOf(RawBytesQuery.class);
    testQueryRequestAndPayloadContents(
        ((RawBytesQuery) m),
        createdCustomPayload,
        GraphConversions.bytecodeToSerialize(graphStatement),
        graphProtocol,
        module);
  }

  private void testQueryRequestAndPayloadContents(
      RawBytesQuery q,
      Map<String, ByteBuffer> customPayload,
      Object traversalTest,
      GraphProtocol graphProtocol,
      GraphBinaryModule module)
      throws IOException {
    if (graphProtocol.isGraphBinary()) {
      assertThat(q.query).isEqualTo(GraphConversions.EMPTY_STRING_QUERY);
      assertThat(customPayload).containsKey(GraphConversions.GRAPH_BINARY_QUERY_OPTION_KEY);
      ByteBuffer encodedQuery = customPayload.get(GraphConversions.GRAPH_BINARY_QUERY_OPTION_KEY);
      assertThat(encodedQuery).isNotNull();
      assertThat(encodedQuery).isEqualTo(serialize(traversalTest, graphProtocol, module));
    } else {
      assertThat(q.query).isEqualTo(serialize(traversalTest, graphProtocol, module).array());
      assertThat(customPayload).doesNotContainKey(GraphConversions.GRAPH_BINARY_QUERY_OPTION_KEY);
    }
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_set_correct_query_options_from_graph_statement(GraphProtocol subProtocol)
      throws IOException {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.newInstance("mockQuery").setQueryParam("name", "value");

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());
    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement, subProtocol, executionProfile, harness.getContext(), module);

    // checks
    Query query = ((Query) m);
    DseQueryOptions options = ((DseQueryOptions) query.options);
    assertThat(options.consistency)
        .isEqualTo(
            DefaultConsistencyLevel.valueOf(
                    executionProfile.getString(DefaultDriverOption.REQUEST_CONSISTENCY))
                .getProtocolCode());
    // set by the mock timestamp generator
    assertThat(options.defaultTimestamp).isEqualTo(-9223372036854775808L);
    assertThat(options.positionalValues)
        .isEqualTo(
            ImmutableList.of(serialize(ImmutableMap.of("name", "value"), subProtocol, module)));

    m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement.setTimestamp(2L),
            subProtocol,
            executionProfile,
            harness.getContext(),
            module);
    query = ((Query) m);
    options = ((DseQueryOptions) query.options);
    assertThat(options.defaultTimestamp).isEqualTo(2L);
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_create_payload_from_config_options(GraphProtocol subProtocol) {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.newInstance("mockQuery").setExecutionProfileName("test-graph");

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Map<String, ByteBuffer> requestPayload =
        GraphConversions.createCustomPayload(
            graphStatement, subProtocol, executionProfile, harness.getContext(), module);

    // checks
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null);
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_NAME, null);
    Mockito.verify(executionProfile).getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false);
    Mockito.verify(executionProfile).getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO);
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null);
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null);

    assertThat(requestPayload.get(GraphConversions.GRAPH_SOURCE_OPTION_KEY))
        .isEqualTo(TEXT.encode("a", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_RESULTS_OPTION_KEY))
        .isEqualTo(
            TEXT.encode(subProtocol.toInternalCode(), harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_NAME_OPTION_KEY))
        .isEqualTo(TEXT.encode("mockGraph", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_LANG_OPTION_KEY))
        .isEqualTo(TEXT.encode("gremlin-groovy", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_TIMEOUT_OPTION_KEY))
        .isEqualTo(BIGINT.encode(2L, harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_READ_CONSISTENCY_LEVEL_OPTION_KEY))
        .isEqualTo(TEXT.encode("LOCAL_TWO", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_WRITE_CONSISTENCY_LEVEL_OPTION_KEY))
        .isEqualTo(TEXT.encode("LOCAL_THREE", harness.getContext().getProtocolVersion()));
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_create_payload_from_statement_options(GraphProtocol subProtocol) {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.builder("mockQuery")
            .setGraphName("mockGraph")
            .setTraversalSource("a")
            .setTimeout(Duration.ofMillis(2))
            .setReadConsistencyLevel(DefaultConsistencyLevel.TWO)
            .setWriteConsistencyLevel(DefaultConsistencyLevel.THREE)
            .setSystemQuery(false)
            .build();

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Map<String, ByteBuffer> requestPayload =
        GraphConversions.createCustomPayload(
            graphStatement, subProtocol, executionProfile, harness.getContext(), module);

    // checks
    Mockito.verify(executionProfile, never())
        .getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null);
    Mockito.verify(executionProfile, never()).getString(DseDriverOption.GRAPH_NAME, null);
    Mockito.verify(executionProfile, never())
        .getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false);
    Mockito.verify(executionProfile, never())
        .getDuration(DseDriverOption.GRAPH_TIMEOUT, Duration.ZERO);
    Mockito.verify(executionProfile, never())
        .getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null);
    Mockito.verify(executionProfile, never())
        .getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null);

    assertThat(requestPayload.get(GraphConversions.GRAPH_SOURCE_OPTION_KEY))
        .isEqualTo(TEXT.encode("a", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_RESULTS_OPTION_KEY))
        .isEqualTo(
            TEXT.encode(subProtocol.toInternalCode(), harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_NAME_OPTION_KEY))
        .isEqualTo(TEXT.encode("mockGraph", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_LANG_OPTION_KEY))
        .isEqualTo(TEXT.encode("gremlin-groovy", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_TIMEOUT_OPTION_KEY))
        .isEqualTo(BIGINT.encode(2L, harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_READ_CONSISTENCY_LEVEL_OPTION_KEY))
        .isEqualTo(TEXT.encode("TWO", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_WRITE_CONSISTENCY_LEVEL_OPTION_KEY))
        .isEqualTo(TEXT.encode("THREE", harness.getContext().getProtocolVersion()));
  }

  @Test
  @UseDataProvider(location = DseTestDataProviders.class, value = "supportedGraphProtocols")
  public void should_not_set_graph_name_on_system_queries(GraphProtocol subProtocol) {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.newInstance("mockQuery").setSystemQuery(true);

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Map<String, ByteBuffer> requestPayload =
        GraphConversions.createCustomPayload(
            graphStatement, subProtocol, executionProfile, harness.getContext(), module);

    // checks
    assertThat(requestPayload.get(GraphConversions.GRAPH_NAME_OPTION_KEY)).isNull();
    assertThat(requestPayload.get(GraphConversions.GRAPH_SOURCE_OPTION_KEY)).isNull();
  }

  @Test
  @UseDataProvider("supportedGraphProtocolsWithDseVersions")
  public void should_return_results_for_statements(GraphProtocol graphProtocol, Version dseVersion)
      throws IOException {

    GraphRequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder()
            .withGraphProtocolForTestConfig(graphProtocol)
            .withDseVersionInMetadata(dseVersion);
    PoolBehavior node1Behavior = builder.customBehavior(node);
    GraphRequestHandlerTestHarness harness = builder.build();

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // ideally we would be able to provide a function here to
    // produce results instead of a static predefined response.
    // Function to which we would pass the harness instance or a (mocked)DriverContext.
    // Since that's not possible in the RequestHandlerTestHarness API at the moment, we
    // have to use another DseDriverContext and GraphBinaryModule here,
    // instead of reusing the one in the harness' DriverContext
    node1Behavior.setResponseSuccess(defaultDseFrameOf(singleGraphRow(graphProtocol, module)));

    GraphSupportChecker graphSupportChecker = mock(GraphSupportChecker.class);
    when(graphSupportChecker.isPagingEnabled(any(), any())).thenReturn(false);
    when(graphSupportChecker.inferGraphProtocol(any(), any(), any())).thenReturn(graphProtocol);

    GraphRequestAsyncProcessor p =
        Mockito.spy(new GraphRequestAsyncProcessor(harness.getContext(), graphSupportChecker));
    when(p.getGraphBinaryModule()).thenReturn(module);

    GraphStatement<?> graphStatement =
        ScriptGraphStatement.newInstance("mockQuery").setExecutionProfileName("test-graph");
    GraphResultSet grs =
        new GraphRequestSyncProcessor(p)
            .process(graphStatement, harness.getSession(), harness.getContext(), "test-graph");

    List<GraphNode> nodes = grs.all();
    assertThat(nodes.size()).isEqualTo(1);

    GraphNode graphNode = nodes.get(0);
    assertThat(graphNode.isVertex()).isTrue();

    Vertex vRead = graphNode.asVertex();
    assertThat(vRead.label()).isEqualTo("person");
    assertThat(vRead.id()).isEqualTo(1);
    if (!graphProtocol.isGraphBinary()) {
      // GraphBinary does not encode properties regardless of whether they are present in the
      // parent element or not :/
      assertThat(vRead.property("name").id()).isEqualTo(11);
      assertThat(vRead.property("name").value()).isEqualTo("marko");
    }
  }

  @DataProvider
  public static Object[][] supportedGraphProtocolsWithDseVersions() {
    return new Object[][] {
      {GRAPHSON_1_0, Version.parse("6.7.0")},
      {GRAPHSON_1_0, Version.parse("6.8.0")},
      {GRAPHSON_2_0, Version.parse("6.7.0")},
      {GRAPHSON_2_0, Version.parse("6.8.0")},
      {GRAPH_BINARY_1_0, Version.parse("6.7.0")},
      {GRAPH_BINARY_1_0, Version.parse("6.8.0")},
    };
  }

  @Test
  @UseDataProvider("dseVersionsWithDefaultGraphProtocol")
  public void should_invoke_request_tracker_and_update_metrics(
      GraphProtocol graphProtocol, Version dseVersion) throws IOException {
    when(nodeMetricUpdater1.isEnabled(
            DseNodeMetric.GRAPH_MESSAGES, DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(true);

    GraphRequestHandlerTestHarness.Builder builder =
        GraphRequestHandlerTestHarness.builder()
            .withGraphProtocolForTestConfig(graphProtocol)
            .withDseVersionInMetadata(dseVersion);
    PoolBehavior node1Behavior = builder.customBehavior(node);
    GraphRequestHandlerTestHarness harness = builder.build();

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    GraphSupportChecker graphSupportChecker = mock(GraphSupportChecker.class);
    when(graphSupportChecker.isPagingEnabled(any(), any())).thenReturn(false);
    when(graphSupportChecker.inferGraphProtocol(any(), any(), any())).thenReturn(graphProtocol);

    GraphRequestAsyncProcessor p =
        Mockito.spy(new GraphRequestAsyncProcessor(harness.getContext(), graphSupportChecker));
    when(p.getGraphBinaryModule()).thenReturn(module);

    RequestTracker requestTracker = mock(RequestTracker.class);
    when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

    GraphStatement<?> graphStatement = ScriptGraphStatement.newInstance("mockQuery");

    node1Behavior.setResponseSuccess(defaultDseFrameOf(singleGraphRow(graphProtocol, module)));

    GraphResultSet grs =
        new GraphRequestSyncProcessor(
                new GraphRequestAsyncProcessor(harness.getContext(), graphSupportChecker))
            .process(graphStatement, harness.getSession(), harness.getContext(), "test-graph");

    List<GraphNode> nodes = grs.all();
    assertThat(nodes.size()).isEqualTo(1);

    GraphNode graphNode = nodes.get(0);
    assertThat(graphNode.isVertex()).isTrue();

    Vertex actual = graphNode.asVertex();
    assertThat(actual.label()).isEqualTo("person");
    assertThat(actual.id()).isEqualTo(1);
    if (!graphProtocol.isGraphBinary()) {
      // GraphBinary does not encode properties regardless of whether they are present in the
      // parent element or not :/
      assertThat(actual.property("name").id()).isEqualTo(11);
      assertThat(actual.property("name").value()).isEqualTo("marko");
    }

    verify(requestTracker)
        .onSuccess(
            eq(graphStatement),
            anyLong(),
            any(DriverExecutionProfile.class),
            eq(node),
            matches(LOG_PREFIX_PER_REQUEST));
    verify(requestTracker)
        .onNodeSuccess(
            eq(graphStatement),
            anyLong(),
            any(DriverExecutionProfile.class),
            eq(node),
            matches(LOG_PREFIX_PER_REQUEST));
    verifyNoMoreInteractions(requestTracker);

    verify(nodeMetricUpdater1)
        .isEnabled(DseNodeMetric.GRAPH_MESSAGES, DriverExecutionProfile.DEFAULT_NAME);
    verify(nodeMetricUpdater1)
        .updateTimer(
            eq(DseNodeMetric.GRAPH_MESSAGES),
            eq(DriverExecutionProfile.DEFAULT_NAME),
            anyLong(),
            eq(TimeUnit.NANOSECONDS));
    verifyNoMoreInteractions(nodeMetricUpdater1);

    verify(harness.getSession().getMetricUpdater())
        .isEnabled(DseSessionMetric.GRAPH_REQUESTS, DriverExecutionProfile.DEFAULT_NAME);
    verify(harness.getSession().getMetricUpdater())
        .updateTimer(
            eq(DseSessionMetric.GRAPH_REQUESTS),
            eq(DriverExecutionProfile.DEFAULT_NAME),
            anyLong(),
            eq(TimeUnit.NANOSECONDS));
    verifyNoMoreInteractions(harness.getSession().getMetricUpdater());
  }

  @Test
  public void should_honor_statement_consistency_level() {
    // initialization
    GraphRequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    ScriptGraphStatement graphStatement =
        ScriptGraphStatement.builder("mockScript")
            .setConsistencyLevel(DefaultConsistencyLevel.THREE)
            .build();

    GraphBinaryModule module = createGraphBinaryModule(harness.getContext());

    // when
    DriverExecutionProfile executionProfile =
        Conversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement, GRAPH_BINARY_1_0, executionProfile, harness.getContext(), module);

    // checks
    assertThat(m).isInstanceOf(Query.class);
    Query q = ((Query) m);
    assertThat(q.options.consistency).isEqualTo(DefaultConsistencyLevel.THREE.getProtocolCode());
  }

  @DataProvider
  public static Object[][] dseVersionsWithDefaultGraphProtocol() {
    // Default GraphSON sub protocol version differs based on DSE version, so test with a version
    // less than DSE 6.8 as well as DSE 6.8.
    return new Object[][] {
      {GRAPHSON_2_0, Version.parse("6.7.0")},
      {GRAPH_BINARY_1_0, Version.parse("6.8.0")},
    };
  }
}
