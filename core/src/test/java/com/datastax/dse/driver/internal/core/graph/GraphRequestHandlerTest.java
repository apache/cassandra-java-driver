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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.api.core.type.codec.TypeCodecs.BIGINT;
import static com.datastax.oss.driver.api.core.type.codec.TypeCodecs.TEXT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.GraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.protocol.internal.request.RawBytesQuery;
import com.datastax.dse.protocol.internal.request.query.DseQueryOptions;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.cql.RequestHandlerTestHarness;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

// TODO subProtocol is hard-coded to graphson-2.0 everywhere, we could parameterize the tests
public class GraphRequestHandlerTest {

  private static final Pattern LOG_PREFIX_PER_REQUEST = Pattern.compile("test-graph\\|\\d*\\|\\d*");

  @Mock DefaultNode node;

  @Mock protected NodeMetricUpdater nodeMetricUpdater1;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    Mockito.when(node.getMetricUpdater()).thenReturn(nodeMetricUpdater1);
  }

  @Test
  public void should_create_query_message_from_script_statement() {
    // initialization
    RequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement = ScriptGraphStatement.newInstance("mockQuery");
    String subProtocol = "graphson-2.0";

    // when
    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement,
            subProtocol,
            GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext()),
            harness.getContext());

    // checks
    assertThat(m).isInstanceOf(Query.class);
    assertThat(((Query) m).query).isEqualTo("mockQuery");
  }

  @Test
  public void should_create_query_message_from_fluent_statement() throws IOException {
    // initialization
    RequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphTraversal traversalTest = DseGraph.g.V().has("name", "marko");
    GraphStatement<?> graphStatement = FluentGraphStatement.newInstance(traversalTest);
    String subProtocol = "graphson-2.0";

    // when
    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement,
            subProtocol,
            GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext()),
            harness.getContext());

    // checks
    assertThat(m).isInstanceOf(RawBytesQuery.class);
    assertThat(((RawBytesQuery) m).query)
        .isEqualTo(GraphSONUtils.serializeToBytes(traversalTest, subProtocol));
  }

  @Test
  public void should_create_query_message_from_batch_statement() throws IOException {
    // initialization
    RequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    List<GraphTraversal> traversalsTest =
        ImmutableList.of(
            DseGraph.g.addV("person").property("key1", "value1"),
            DseGraph.g.addV("software").property("key2", "value2"));
    GraphStatement<?> graphStatement =
        BatchGraphStatement.builder().addTraversals(traversalsTest).build();
    String subProtocol = "graphson-2.0";

    // when
    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement,
            subProtocol,
            GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext()),
            harness.getContext());

    // checks
    assertThat(m).isInstanceOf(RawBytesQuery.class);
    assertThat(((RawBytesQuery) m).query)
        .isEqualTo(GraphSONUtils.serializeToBytes(traversalsTest, subProtocol));
  }

  @Test
  public void should_set_correct_query_options_from_graph_statement() throws IOException {
    // initialization
    RequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.newInstance("mockQuery").setQueryParam("name", "value");
    String subProtocol = "graphson-2.0";

    // when
    DriverExecutionProfile executionProfile =
        GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext());
    Message m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement, subProtocol, executionProfile, harness.getContext());

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
            ImmutableList.of(
                GraphSONUtils.serializeToByteBuffer(
                    ImmutableMap.of("name", "value"), subProtocol)));

    m =
        GraphConversions.createMessageFromGraphStatement(
            graphStatement.setTimestamp(2L),
            subProtocol,
            GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext()),
            harness.getContext());
    query = ((Query) m);
    options = ((DseQueryOptions) query.options);
    assertThat(options.defaultTimestamp).isEqualTo(2L);
  }

  @Test
  public void should_create_payload_from_config_options() {
    // initialization
    RequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.newInstance("mockQuery").setExecutionProfileName("test-graph");
    String subProtocol = "graphson-2.0";

    // when
    DriverExecutionProfile executionProfile =
        GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Map<String, ByteBuffer> requestPayload =
        GraphConversions.createCustomPayload(
            graphStatement, subProtocol, executionProfile, harness.getContext());

    // checks
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null);
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_NAME, null);
    Mockito.verify(executionProfile).getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false);
    Mockito.verify(executionProfile).getDuration(DseDriverOption.GRAPH_TIMEOUT, null);
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null);
    Mockito.verify(executionProfile).getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null);

    assertThat(requestPayload.get(GraphConversions.GRAPH_SOURCE_OPTION_KEY))
        .isEqualTo(TEXT.encode("a", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_RESULTS_OPTION_KEY))
        .isEqualTo(TEXT.encode(subProtocol, harness.getContext().getProtocolVersion()));
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
  public void should_create_payload_from_statement_options() {
    // initialization
    RequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.builder("mockQuery")
            .setGraphName("mockGraph")
            .setTraversalSource("a")
            .setTimeout(Duration.ofMillis(2))
            .setReadConsistencyLevel(DefaultConsistencyLevel.TWO)
            .setWriteConsistencyLevel(DefaultConsistencyLevel.THREE)
            .setSystemQuery(false)
            .build();
    String subProtocol = "graphson-2.0";

    // when
    DriverExecutionProfile executionProfile =
        GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Map<String, ByteBuffer> requestPayload =
        GraphConversions.createCustomPayload(
            graphStatement, subProtocol, executionProfile, harness.getContext());

    // checks
    Mockito.verify(executionProfile, never())
        .getString(DseDriverOption.GRAPH_TRAVERSAL_SOURCE, null);
    Mockito.verify(executionProfile, never()).getString(DseDriverOption.GRAPH_NAME, null);
    Mockito.verify(executionProfile, never())
        .getBoolean(DseDriverOption.GRAPH_IS_SYSTEM_QUERY, false);
    Mockito.verify(executionProfile, never()).getDuration(DseDriverOption.GRAPH_TIMEOUT, null);
    Mockito.verify(executionProfile, never())
        .getString(DseDriverOption.GRAPH_READ_CONSISTENCY_LEVEL, null);
    Mockito.verify(executionProfile, never())
        .getString(DseDriverOption.GRAPH_WRITE_CONSISTENCY_LEVEL, null);

    assertThat(requestPayload.get(GraphConversions.GRAPH_SOURCE_OPTION_KEY))
        .isEqualTo(TEXT.encode("a", harness.getContext().getProtocolVersion()));
    assertThat(requestPayload.get(GraphConversions.GRAPH_RESULTS_OPTION_KEY))
        .isEqualTo(TEXT.encode(subProtocol, harness.getContext().getProtocolVersion()));
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
  public void should_not_set_graph_name_on_system_queries() {
    // initialization
    RequestHandlerTestHarness harness = GraphRequestHandlerTestHarness.builder().build();
    GraphStatement<?> graphStatement =
        ScriptGraphStatement.newInstance("mockQuery").setSystemQuery(true);
    String subProtocol = "graphson-2.0";

    // when
    DriverExecutionProfile executionProfile =
        GraphConversions.resolveExecutionProfile(graphStatement, harness.getContext());

    Map<String, ByteBuffer> requestPayload =
        GraphConversions.createCustomPayload(
            graphStatement, subProtocol, executionProfile, harness.getContext());

    // checks
    assertThat(requestPayload.get(GraphConversions.GRAPH_NAME_OPTION_KEY)).isNull();
    assertThat(requestPayload.get(GraphConversions.GRAPH_SOURCE_OPTION_KEY)).isNull();
  }

  @Test
  public void should_return_results_for_statements()
      throws IOException, ExecutionException, InterruptedException {
    RequestHandlerTestHarness harness =
        GraphRequestHandlerTestHarness.builder()
            .withResponse(node, defaultDseFrameOf(singleGraphRow()))
            .build();

    GraphStatement graphStatement = ScriptGraphStatement.newInstance("mockQuery");
    GraphResultSet grs =
        new GraphRequestSyncProcessor(new GraphRequestAsyncProcessor())
            .process(graphStatement, harness.getSession(), harness.getContext(), "test-graph");

    List<GraphNode> nodes = grs.all();
    assertThat(nodes.size()).isEqualTo(1);

    GraphNode node = nodes.get(0);
    assertThat(node.isVertex()).isTrue();

    Vertex v = node.asVertex();
    assertThat(v.label()).isEqualTo("person");
    assertThat(v.id()).isEqualTo(1);
    assertThat(v.property("name").id()).isEqualTo(11);
    assertThat(v.property("name").value()).isEqualTo("marko");
  }

  @Test
  public void should_invoke_request_tracker()
      throws IOException, ExecutionException, InterruptedException {
    RequestHandlerTestHarness harness =
        GraphRequestHandlerTestHarness.builder()
            .withResponse(node, defaultDseFrameOf(singleGraphRow()))
            .build();

    RequestTracker requestTracker = mock(RequestTracker.class);
    when(harness.getContext().getRequestTracker()).thenReturn(requestTracker);

    GraphStatement graphStatement = ScriptGraphStatement.newInstance("mockQuery");
    GraphResultSet grs =
        new GraphRequestSyncProcessor(new GraphRequestAsyncProcessor())
            .process(graphStatement, harness.getSession(), harness.getContext(), "test-graph");

    List<GraphNode> nodes = grs.all();
    assertThat(nodes.size()).isEqualTo(1);

    GraphNode graphNode = nodes.get(0);
    assertThat(graphNode.isVertex()).isTrue();

    Vertex v = graphNode.asVertex();
    assertThat(v.label()).isEqualTo("person");
    assertThat(v.id()).isEqualTo(1);
    assertThat(v.property("name").id()).isEqualTo(11);
    assertThat(v.property("name").value()).isEqualTo("marko");

    verify(requestTracker)
        .onSuccess(
            eq(graphStatement),
            anyLong(),
            any(DriverExecutionProfile.class),
            eq(node),
            matches(LOG_PREFIX_PER_REQUEST));
    verifyNoMoreInteractions(requestTracker);
  }

  private static Frame defaultDseFrameOf(Message responseMessage) {
    return Frame.forResponse(
        DseProtocolVersion.DSE_V2.getCode(),
        0,
        null,
        Frame.NO_PAYLOAD,
        Collections.emptyList(),
        responseMessage);
  }

  // Returns a single row, with a single "message" column with the value "hello, world"
  private static Message singleGraphRow() throws IOException {
    RowsMetadata metadata =
        new RowsMetadata(
            ImmutableList.of(
                new ColumnSpec(
                    "ks",
                    "table",
                    "gremlin",
                    0,
                    RawType.PRIMITIVES.get(ProtocolConstants.DataType.VARCHAR))),
            null,
            new int[] {},
            null);
    Queue<List<ByteBuffer>> data = new ArrayDeque<>();
    data.add(
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(
                ImmutableMap.of(
                    "result",
                    DetachedVertex.build()
                        .setId(1)
                        .setLabel("person")
                        .addProperty(
                            DetachedVertexProperty.build()
                                .setId(11)
                                .setLabel("name")
                                .setValue("marko")
                                .create())
                        .create()),
                "graphson-2.0")));
    return new DefaultRows(metadata, data);
  }
}
