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

import static com.datastax.dse.driver.internal.core.graph.GraphSONUtils.GRAPHSON_1_0;
import static com.datastax.dse.driver.internal.core.graph.GraphSONUtils.GRAPHSON_2_0;
import static com.datastax.dse.driver.internal.core.graph.GraphSONUtils.GRAPHSON_3_0;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dse.driver.api.core.graph.GraphNode;
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
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public class GraphNodeTest {

  @Test
  public void should_create_graph_node_for_set_for_graphson_3_0() throws IOException {
    // given
    ImmutableList<ByteBuffer> bytes =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableSet.of("value"), GRAPHSON_3_0));

    // when
    GraphNode graphNode = GraphSONUtils.createGraphNode(bytes, GRAPHSON_3_0);

    // then
    assertThat(graphNode.isSet()).isTrue();
    Set<String> set = graphNode.asSet();
    assertThat(set).isEqualTo(ImmutableSet.of("value"));
  }

  @Test
  public void should_not_support_set_for_graphson_2_0() throws IOException {
    // given
    ImmutableList<ByteBuffer> bytes =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableSet.of("value"), GRAPHSON_2_0));

    // when
    GraphNode graphNode = GraphSONUtils.createGraphNode(bytes, GRAPHSON_2_0);

    // then
    assertThat(graphNode.isSet()).isFalse();
  }

  @Test
  public void should_throw_for_set_for_graphson_1_0() throws IOException {
    // given
    ImmutableList<ByteBuffer> bytes =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableSet.of("value"), GRAPHSON_1_0));

    // when
    GraphNode graphNode = GraphSONUtils.createGraphNode(bytes, GRAPHSON_1_0);

    // then
    assertThat(graphNode.isSet()).isFalse();
    assertThatThrownBy(graphNode::asSet).isExactlyInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @UseDataProvider(value = "graphsonAllVersions")
  public void should_create_graph_node_for_list(String graphVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> bytes =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableList.of("value"), graphVersion));

    // when
    GraphNode graphNode = GraphSONUtils.createGraphNode(bytes, graphVersion);

    // then
    assertThat(graphNode.isList()).isTrue();
    List<String> result = graphNode.asList();
    assertThat(result).isEqualTo(ImmutableList.of("value"));
  }

  @Test
  public void should_create_graph_node_for_map_for_graphson_3_0() throws IOException {
    // given
    ImmutableList<ByteBuffer> bytes =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableMap.of(12, 1234), GRAPHSON_3_0));

    // when
    GraphNode graphNode = GraphSONUtils.createGraphNode(bytes, GRAPHSON_3_0);

    // then
    assertThat(graphNode.isMap()).isTrue();
    Map<String, Integer> result = graphNode.asMap();
    assertThat(result).isEqualTo(ImmutableMap.of(12, 1234));
  }

  @Test
  @UseDataProvider("graphsonAllVersions")
  public void should_create_graph_node_for_map(String graphsonVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> bytes =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableMap.of("value", 1234), graphsonVersion));

    // when
    GraphNode graphNode = GraphSONUtils.createGraphNode(bytes, graphsonVersion);

    // then
    assertThat(graphNode.isMap()).isTrue();
    Map<String, Integer> result = graphNode.asMap();
    assertThat(result).isEqualTo(ImmutableMap.of("value", 1234));
  }

  @Test
  @UseDataProvider("graphson1_0and2_0")
  public void should_create_graph_node_for_map_for_non_string_key(String graphsonVersion)
      throws IOException {
    // given
    ImmutableList<ByteBuffer> bytes =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableMap.of(12, 1234), graphsonVersion));

    // when
    GraphNode graphNode = GraphSONUtils.createGraphNode(bytes, graphsonVersion);

    // then
    assertThat(graphNode.isMap()).isTrue();
    Map<String, Integer> result = graphNode.asMap();
    assertThat(result).isEqualTo(ImmutableMap.of("12", 1234));
  }

  @Test
  @UseDataProvider(value = "graphsonAllVersions")
  public void should_calculate_size_of_collection_types(String graphVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> map =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableMap.of(12, 1234), graphVersion));

    ImmutableList<ByteBuffer> set =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableSet.of(12, 1234), graphVersion));

    ImmutableList<ByteBuffer> list =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableList.of(12, 1234, 99999), graphVersion));

    // when
    GraphNode mapNode = GraphSONUtils.createGraphNode(map, graphVersion);
    GraphNode setNode = GraphSONUtils.createGraphNode(set, graphVersion);
    GraphNode listNode = GraphSONUtils.createGraphNode(list, graphVersion);

    // then
    assertThat(mapNode.size()).isEqualTo(1);
    assertThat(setNode.size()).isEqualTo(2);
    assertThat(listNode.size()).isEqualTo(3);
  }

  @Test
  @UseDataProvider(value = "graphsonAllVersions")
  public void should_return_is_value_only_for_scalar_value(String graphVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> map =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableMap.of(12, 1234), graphVersion));

    ImmutableList<ByteBuffer> set =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableSet.of(12, 1234), graphVersion));

    ImmutableList<ByteBuffer> list =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(ImmutableList.of(12, 1234, 99999), graphVersion));

    ImmutableList<ByteBuffer> vertex =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(new DetachedVertex("a", "l", null), graphVersion));

    ImmutableList<ByteBuffer> edge =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(
                new DetachedEdge("a", "l", Collections.emptyMap(), "v1", "l1", "v2", "l2"),
                graphVersion));

    ImmutableList<ByteBuffer> path =
        ImmutableList.of(GraphSONUtils.serializeToByteBuffer(EmptyPath.instance(), graphVersion));

    ImmutableList<ByteBuffer> property =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(new DetachedProperty<>("a", 1), graphVersion));

    ImmutableList<ByteBuffer> vertexProperty =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(
                new DetachedVertexProperty<>(
                    "id", "l", "v", null, new DetachedVertex("a", "l", null)),
                graphVersion));

    ImmutableList<ByteBuffer> scalarValue =
        ImmutableList.of(GraphSONUtils.serializeToByteBuffer(true, graphVersion));

    // when
    GraphNode mapNode = GraphSONUtils.createGraphNode(map, graphVersion);
    GraphNode setNode = GraphSONUtils.createGraphNode(set, graphVersion);
    GraphNode listNode = GraphSONUtils.createGraphNode(list, graphVersion);
    GraphNode vertexNode = GraphSONUtils.createGraphNode(vertex, graphVersion);
    GraphNode edgeNode = GraphSONUtils.createGraphNode(edge, graphVersion);
    GraphNode pathNode = GraphSONUtils.createGraphNode(path, graphVersion);
    GraphNode propertyNode = GraphSONUtils.createGraphNode(property, graphVersion);
    GraphNode vertexPropertyNode = GraphSONUtils.createGraphNode(vertexProperty, graphVersion);
    GraphNode scalarValueNode = GraphSONUtils.createGraphNode(scalarValue, graphVersion);

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
  @UseDataProvider("graphson2_0and3_0")
  public void should_check_if_node_is_property_not_map(String graphVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> property =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(new DetachedProperty<>("a", 1), graphVersion));

    // when
    GraphNode propertyNode = GraphSONUtils.createGraphNode(property, graphVersion);

    // then
    assertThat(propertyNode.isProperty()).isTrue();
    assertThat(propertyNode.isMap()).isFalse();
    assertThat(propertyNode.asProperty()).isNotNull();
  }

  @Test
  public void should_check_if_node_is_property_or_map_for_1_0() throws IOException {
    // given
    ImmutableList<ByteBuffer> property =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(new DetachedProperty<>("a", 1), GRAPHSON_1_0));

    // when
    GraphNode propertyNode = GraphSONUtils.createGraphNode(property, GRAPHSON_1_0);

    // then
    assertThat(propertyNode.isProperty()).isTrue();
    assertThat(propertyNode.isMap()).isTrue();
    assertThat(propertyNode.asProperty()).isNotNull();
  }

  @Test
  @UseDataProvider("graphsonAllVersions")
  public void should_check_if_node_is_vertex_property(String graphVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> vertexProperty =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(
                new DetachedVertexProperty<>(
                    "id", "l", "v", null, new DetachedVertex("a", "l", null)),
                graphVersion));

    // when
    GraphNode vertexPropertyNode = GraphSONUtils.createGraphNode(vertexProperty, graphVersion);

    // then
    assertThat(vertexPropertyNode.isVertexProperty()).isTrue();
    assertThat(vertexPropertyNode.isVertexProperty()).isNotNull();
  }

  @Test
  public void should_check_if_node_is_path_for_graphson_1_0() throws IOException {
    // given
    ImmutableList<ByteBuffer> path =
        ImmutableList.of(GraphSONUtils.serializeToByteBuffer(EmptyPath.instance(), GRAPHSON_1_0));

    // when
    GraphNode vertexPropertyNode = GraphSONUtils.createGraphNode(path, GRAPHSON_1_0);

    // then
    assertThat(vertexPropertyNode.isPath()).isFalse();
    assertThatThrownBy(vertexPropertyNode::asPath)
        .isExactlyInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  @UseDataProvider("graphson2_0and3_0")
  public void should_check_if_node_is_path(String graphsonVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> path =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(EmptyPath.instance(), graphsonVersion));

    // when
    GraphNode vertexPropertyNode = GraphSONUtils.createGraphNode(path, graphsonVersion);

    // then
    assertThat(vertexPropertyNode.isPath()).isTrue();
    assertThat(vertexPropertyNode.asPath()).isNotNull();
  }

  @Test
  @UseDataProvider("graphsonAllVersions")
  public void should_check_if_node_is_vertex(String graphsonVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> vertex =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(
                new DetachedVertex("a", "l", null), graphsonVersion));

    // when
    GraphNode vertexNode = GraphSONUtils.createGraphNode(vertex, graphsonVersion);

    // then
    assertThat(vertexNode.isVertex()).isTrue();
    assertThat(vertexNode.asVertex()).isNotNull();
  }

  @Test
  @UseDataProvider("graphsonAllVersions")
  public void should_check_if_node_is_edge(String graphsonVersion) throws IOException {
    // given
    ImmutableList<ByteBuffer> edge =
        ImmutableList.of(
            GraphSONUtils.serializeToByteBuffer(
                new DetachedEdge("a", "l", Collections.emptyMap(), "v1", "l1", "v2", "l2"),
                graphsonVersion));

    // when
    GraphNode edgeNode = GraphSONUtils.createGraphNode(edge, graphsonVersion);

    // then
    assertThat(edgeNode.isEdge()).isTrue();
    assertThat(edgeNode.asEdge()).isNotNull();
  }

  @DataProvider
  public static Object[][] graphsonAllVersions() {
    return new Object[][] {{GRAPHSON_1_0}, {GRAPHSON_2_0}, {GRAPHSON_3_0}};
  }

  @DataProvider
  public static Object[][] graphson1_0and2_0() {
    return new Object[][] {{GRAPHSON_1_0}, {GRAPHSON_2_0}};
  }

  @DataProvider
  public static Object[][] graphson2_0and3_0() {
    return new Object[][] {{GRAPHSON_2_0}, {GRAPHSON_3_0}};
  }
}
