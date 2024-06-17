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

import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.reactive.ReactiveGraphNode;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.NotThreadSafe;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

@NotThreadSafe
class DefaultReactiveGraphNode implements ReactiveGraphNode {

  private final GraphNode graphNode;
  private final ExecutionInfo executionInfo;

  DefaultReactiveGraphNode(@NonNull GraphNode graphNode, @NonNull ExecutionInfo executionInfo) {
    this.graphNode = graphNode;
    this.executionInfo = executionInfo;
  }

  @NonNull
  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  public boolean isNull() {
    return graphNode.isNull();
  }

  @Override
  public boolean isMap() {
    return graphNode.isMap();
  }

  @Override
  public Iterable<?> keys() {
    return graphNode.keys();
  }

  @Override
  public GraphNode getByKey(Object key) {
    return graphNode.getByKey(key);
  }

  @Override
  public <K, V> Map<K, V> asMap() {
    return graphNode.asMap();
  }

  @Override
  public boolean isList() {
    return graphNode.isList();
  }

  @Override
  public int size() {
    return graphNode.size();
  }

  @Override
  public GraphNode getByIndex(int index) {
    return graphNode.getByIndex(index);
  }

  @Override
  public <T> List<T> asList() {
    return graphNode.asList();
  }

  @Override
  public boolean isValue() {
    return graphNode.isValue();
  }

  @Override
  public int asInt() {
    return graphNode.asInt();
  }

  @Override
  public boolean asBoolean() {
    return graphNode.asBoolean();
  }

  @Override
  public long asLong() {
    return graphNode.asLong();
  }

  @Override
  public double asDouble() {
    return graphNode.asDouble();
  }

  @Override
  public String asString() {
    return graphNode.asString();
  }

  @Override
  public <ResultT> ResultT as(Class<ResultT> clazz) {
    return graphNode.as(clazz);
  }

  @Override
  public <ResultT> ResultT as(GenericType<ResultT> type) {
    return graphNode.as(type);
  }

  @Override
  public boolean isVertex() {
    return graphNode.isVertex();
  }

  @Override
  public Vertex asVertex() {
    return graphNode.asVertex();
  }

  @Override
  public boolean isEdge() {
    return graphNode.isEdge();
  }

  @Override
  public Edge asEdge() {
    return graphNode.asEdge();
  }

  @Override
  public boolean isPath() {
    return graphNode.isPath();
  }

  @Override
  public Path asPath() {
    return graphNode.asPath();
  }

  @Override
  public boolean isProperty() {
    return graphNode.isProperty();
  }

  @Override
  public <T> Property<T> asProperty() {
    return graphNode.asProperty();
  }

  @Override
  public boolean isVertexProperty() {
    return graphNode.isVertexProperty();
  }

  @Override
  public <T> VertexProperty<T> asVertexProperty() {
    return graphNode.asVertexProperty();
  }

  @Override
  public boolean isSet() {
    return graphNode.isSet();
  }

  @Override
  public <T> Set<T> asSet() {
    return graphNode.asSet();
  }

  @Override
  public String toString() {
    return "DefaultReactiveGraphNode{graphNode="
        + graphNode
        + ", executionInfo="
        + executionInfo
        + '}';
  }
}
