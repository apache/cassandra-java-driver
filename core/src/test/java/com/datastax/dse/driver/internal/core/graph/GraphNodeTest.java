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

import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPHSON_1_0;
import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPHSON_2_0;
import static com.datastax.dse.driver.internal.core.graph.GraphProtocol.GRAPH_BINARY_1_0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.internal.core.graph.binary.GraphBinaryModule;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class GraphNodeTest {

  private GraphBinaryModule graphBinaryModule;

  @Before
  public void setup() {
    DefaultDriverContext dseDriverContext = mock(DefaultDriverContext.class);
    when(dseDriverContext.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
    when(dseDriverContext.getProtocolVersion()).thenReturn(DseProtocolVersion.DSE_V2);

    TypeSerializerRegistry registry =
        GraphBinaryModule.createDseTypeSerializerRegistry(dseDriverContext);
    graphBinaryModule =
        new GraphBinaryModule(new GraphBinaryReader(registry), new GraphBinaryWriter(registry));
  }

  @Test
  public void should_not_support_set_for_graphson_2_0() throws IOException {
    // when
    GraphNode graphNode = serdeAndCreateGraphNode(ImmutableSet.of("value"), GRAPHSON_2_0);

    // then
    assertThat(graphNode.isSet()).isFalse();
  }

  @Test
  public void should_throw_for_set_for_graphson_1_0() throws IOException {
    // when
    GraphNode graphNode = serdeAndCreateGraphNode(ImmutableSet.of("value"), GRAPHSON_1_0);

    // then
    assertThat(graphNode.isSet()).isFalse();
    assertThatThrownBy(graphNode::asSet).isExactlyInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @UseDataProvider(value = "allGraphProtocols")
  public void should_create_graph_node_for_list(GraphProtocol graphVersion) throws IOException {
    // when
    GraphNode graphNode = serdeAndCreateGraphNode(ImmutableList.of("value"), graphVersion);

    // then
    assertThat(graphNode.isList()).isTrue();
    List<String> result = graphNode.asList();
    assertThat(result).isEqualTo(ImmutableList.of("value"));
  }

  @Test
  @UseDataProvider("allGraphProtocols")
  public void should_create_graph_node_for_map(GraphProtocol graphProtocol) throws IOException {
    // when
    GraphNode graphNode = serdeAndCreateGraphNode(ImmutableMap.of("value", 1234), graphProtocol);

    // then
    assertThat(graphNode.isMap()).isTrue();
    Map<String, Integer> result = graphNode.asMap();
    assertThat(result).isEqualTo(ImmutableMap.of("value", 1234));
  }

  @Test
  @UseDataProvider("graphson1_0and2_0")
  public void should_create_graph_node_for_map_for_non_string_key(GraphProtocol graphProtocol)
      throws IOException {
    // when
    GraphNode graphNode = serdeAndCreateGraphNode(ImmutableMap.of(12, 1234), graphProtocol);

    // then
    assertThat(graphNode.isMap()).isTrue();
    Map<String, Integer> result = graphNode.asMap();
    assertThat(result).isEqualTo(ImmutableMap.of("12", 1234));
  }

  @Test
  @UseDataProvider(value = "allGraphProtocols")
  public void should_calculate_size_of_collection_types(GraphProtocol graphProtocol)
      throws IOException {
    // when
    GraphNode mapNode = serdeAndCreateGraphNode(ImmutableMap.of(12, 1234), graphProtocol);
    GraphNode setNode = serdeAndCreateGraphNode(ImmutableSet.of(12, 1234), graphProtocol);
    GraphNode listNode = serdeAndCreateGraphNode(ImmutableList.of(12, 1234, 99999), graphProtocol);

    // then
    assertThat(mapNode.size()).isEqualTo(1);
    assertThat(setNode.size()).isEqualTo(2);
    assertThat(listNode.size()).isEqualTo(3);
  }

  @Test
  @UseDataProvider(value = "allGraphProtocols")
  public void should_return_is_value_only_for_scalar_value(GraphProtocol graphProtocol)
      throws IOException {
    // when
    GraphNode mapNode = serdeAndCreateGraphNode(ImmutableMap.of(12, 1234), graphProtocol);
    GraphNode setNode = serdeAndCreateGraphNode(ImmutableMap.of(12, 1234), graphProtocol);
    GraphNode listNode = serdeAndCreateGraphNode(ImmutableMap.of(12, 1234), graphProtocol);
    GraphNode vertexNode =
        serdeAndCreateGraphNode(new DetachedVertex("a", "l", null), graphProtocol);
    GraphNode edgeNode =
        serdeAndCreateGraphNode(
            new DetachedEdge("a", "l", Collections.emptyMap(), "v1", "l1", "v2", "l2"),
            graphProtocol);
    GraphNode pathNode = serdeAndCreateGraphNode(EmptyPath.instance(), graphProtocol);
    GraphNode propertyNode = serdeAndCreateGraphNode(new DetachedProperty<>("a", 1), graphProtocol);
    GraphNode vertexPropertyNode =
        serdeAndCreateGraphNode(
            new DetachedVertexProperty<>("id", "l", "v", null, new DetachedVertex("a", "l", null)),
            graphProtocol);
    GraphNode scalarValueNode = serdeAndCreateGraphNode(true, graphProtocol);

    // then
    assertThat(mapNode.isValue()).isFalse();
    assertThat(setNode.isValue()).isFalse();
    assertThat(listNode.isValue()).isFalse();
    assertThat(vertexNode.isValue()).isFalse();
    assertThat(edgeNode.isValue()).isFalse();
    assertThat(pathNode.isValue()).isFalse();
    assertThat(propertyNode.isValue()).isFalse();
    assertThat(vertexPropertyNode.isValue()).isFalse();
    assertThat(scalarValueNode.isValue()).isTrue();
  }

  @Test
  @UseDataProvider("objectGraphNodeProtocols")
  public void should_check_if_node_is_property_not_map(GraphProtocol graphProtocol)
      throws IOException {
    // when
    GraphNode propertyNode = serdeAndCreateGraphNode(new DetachedProperty<>("a", 1), graphProtocol);

    // then
    assertThat(propertyNode.isProperty()).isTrue();
    assertThat(propertyNode.isMap()).isFalse();
    assertThat(propertyNode.asProperty()).isNotNull();
  }

  @Test
  public void should_check_if_node_is_property_or_map_for_1_0() throws IOException {
    // when
    GraphNode propertyNode = serdeAndCreateGraphNode(new DetachedProperty<>("a", 1), GRAPHSON_1_0);

    // then
    assertThat(propertyNode.isProperty()).isTrue();
    assertThat(propertyNode.isMap()).isTrue();
    assertThat(propertyNode.asProperty()).isNotNull();
  }

  @Test
  @UseDataProvider("allGraphProtocols")
  public void should_check_if_node_is_vertex_property(GraphProtocol graphProtocol)
      throws IOException {
    // when
    GraphNode vertexPropertyNode =
        serdeAndCreateGraphNode(
            new DetachedVertexProperty<>("id", "l", "v", null, new DetachedVertex("a", "l", null)),
            graphProtocol);

    // then
    assertThat(vertexPropertyNode.isVertexProperty()).isTrue();
    assertThat(vertexPropertyNode.isVertexProperty()).isNotNull();
  }

  @Test
  public void should_check_if_node_is_path_for_graphson_1_0() throws IOException {
    // when
    GraphNode pathNode = serdeAndCreateGraphNode(EmptyPath.instance(), GRAPHSON_1_0);

    // then
    assertThat(pathNode.isPath()).isFalse();
    assertThatThrownBy(pathNode::asPath).isExactlyInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @UseDataProvider("objectGraphNodeProtocols")
  public void should_check_if_node_is_path(GraphProtocol graphProtocol) throws IOException {
    // when
    GraphNode pathNode = serdeAndCreateGraphNode(EmptyPath.instance(), graphProtocol);

    // then
    assertThat(pathNode.isPath()).isTrue();
    assertThat(pathNode.asPath()).isNotNull();
  }

  @Test
  @UseDataProvider("allGraphProtocols")
  public void should_check_if_node_is_vertex(GraphProtocol graphProtocol) throws IOException {
    // when
    GraphNode vertexNode =
        serdeAndCreateGraphNode(new DetachedVertex("a", "l", null), graphProtocol);

    // then
    assertThat(vertexNode.isVertex()).isTrue();
    assertThat(vertexNode.asVertex()).isNotNull();
  }

  @Test
  @UseDataProvider("allGraphProtocols")
  public void should_check_if_node_is_edge(GraphProtocol graphProtocol) throws IOException {
    // when
    GraphNode edgeNode =
        serdeAndCreateGraphNode(
            new DetachedEdge("a", "l", Collections.emptyMap(), "v1", "l1", "v2", "l2"),
            graphProtocol);

    // then
    assertThat(edgeNode.isEdge()).isTrue();
    assertThat(edgeNode.asEdge()).isNotNull();
  }

  private GraphNode serdeAndCreateGraphNode(Object inputValue, GraphProtocol graphProtocol)
      throws IOException {
    if (graphProtocol.isGraphBinary()) {
      Buffer tinkerBuf = graphBinaryModule.serialize(new DefaultRemoteTraverser<>(inputValue, 0L));
      ByteBuffer nioBuffer = tinkerBuf.nioBuffer();
      tinkerBuf.release();
      return new ObjectGraphNode(
          GraphConversions.createGraphBinaryGraphNode(
                  ImmutableList.of(nioBuffer), graphBinaryModule)
              .as(Traverser.class)
              .get());
    } else {
      return GraphSONUtils.createGraphNode(
          ImmutableList.of(GraphSONUtils.serializeToByteBuffer(inputValue, graphProtocol)),
          graphProtocol);
    }
  }

  @DataProvider
  public static Object[][] allGraphProtocols() {
    return new Object[][] {{GRAPHSON_1_0}, {GRAPHSON_2_0}, {GRAPH_BINARY_1_0}};
  }

  @DataProvider
  public static Object[][] graphson1_0and2_0() {
    return new Object[][] {{GRAPHSON_1_0}, {GRAPHSON_2_0}};
  }

  @DataProvider
  public static Object[][] objectGraphNodeProtocols() {
    return new Object[][] {{GRAPHSON_2_0}, {GRAPH_BINARY_1_0}};
  }
}
