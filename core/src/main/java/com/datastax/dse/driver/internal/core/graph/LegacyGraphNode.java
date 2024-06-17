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

import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Objects;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.Immutable;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

/**
 * Legacy implementation for GraphSON 1 results.
 *
 * <p>The server returns plain JSON with no type information. The driver works with the JSON
 * representation directly.
 */
@Immutable
public class LegacyGraphNode implements GraphNode {
  private static final String TYPE = "type";
  private static final String VERTEX_TYPE = "vertex";
  private static final String EDGE_TYPE = "edge";

  private static final GenericType<List<Object>> LIST_TYPE = GenericType.listOf(Object.class);
  private static final GenericType<Map<String, Object>> MAP_TYPE =
      GenericType.mapOf(String.class, Object.class);

  private final JsonNode delegate;
  private final ObjectMapper objectMapper;

  public LegacyGraphNode(JsonNode delegate, ObjectMapper objectMapper) {
    Preconditions.checkNotNull(delegate);
    Preconditions.checkNotNull(objectMapper);
    this.delegate = delegate;
    this.objectMapper = objectMapper;
  }

  /**
   * The underlying JSON representation.
   *
   * <p>This is an implementation detail, it's only exposed through the internal API.
   */
  public JsonNode getDelegate() {
    return delegate;
  }

  /**
   * The object mapper used to deserialize results in {@link #as(Class)} and {@link
   * #as(GenericType)}.
   *
   * <p>This is an implementation detail, it's only exposed through the internal API.
   */
  public ObjectMapper getObjectMapper() {
    return objectMapper;
  }

  @Override
  public boolean isNull() {
    return delegate.isNull();
  }

  @Override
  public boolean isMap() {
    return delegate.isObject();
  }

  @Override
  public Iterable<?> keys() {
    return (Iterable<String>) delegate::fieldNames;
  }

  @Override
  public LegacyGraphNode getByKey(Object key) {
    if (!(key instanceof String)) {
      return null;
    }
    JsonNode node = delegate.get(((String) key));
    if (node == null) {
      return null;
    }
    return new LegacyGraphNode(node, objectMapper);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> Map<K, V> asMap() {
    return (Map<K, V>) as(MAP_TYPE);
  }

  @Override
  public boolean isList() {
    return delegate.isArray();
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public LegacyGraphNode getByIndex(int index) {
    JsonNode node = delegate.get(index);
    if (node == null) {
      return null;
    }
    return new LegacyGraphNode(node, objectMapper);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> asList() {
    return (List<T>) as(LIST_TYPE);
  }

  @Override
  public boolean isValue() {
    return delegate.isValueNode();
  }

  @Override
  public int asInt() {
    return delegate.asInt();
  }

  @Override
  public boolean asBoolean() {
    return delegate.asBoolean();
  }

  @Override
  public long asLong() {
    return delegate.asLong();
  }

  @Override
  public double asDouble() {
    return delegate.asDouble();
  }

  @Override
  public String asString() {
    return delegate.asText();
  }

  @Override
  public boolean isVertex() {
    return isType(VERTEX_TYPE);
  }

  @Override
  public Vertex asVertex() {
    try {
      return GraphSONUtils.GRAPHSON1_READER
          .get()
          .readVertex(
              new ByteArrayInputStream(delegate.toString().getBytes(StandardCharsets.UTF_8)),
              null,
              null,
              null);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not deserialize node as Vertex.", e);
    }
  }

  @Override
  public boolean isEdge() {
    return isType(EDGE_TYPE);
  }

  @Override
  public Edge asEdge() {
    try {
      return GraphSONUtils.GRAPHSON1_READER
          .get()
          .readEdge(
              new ByteArrayInputStream(delegate.toString().getBytes(StandardCharsets.UTF_8)),
              Attachable::get);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not deserialize node as Edge.", e);
    }
  }

  @Override
  public boolean isPath() {
    return false;
  }

  @Override
  public Path asPath() {
    throw new UnsupportedOperationException(
        "GraphSON1 does not support Path, use another Graph sub-protocol such as GraphSON2.");
  }

  @Override
  public boolean isProperty() {
    return delegate.has(GraphSONTokens.KEY) && delegate.has(GraphSONTokens.VALUE);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Property<T> asProperty() {
    try {
      return GraphSONUtils.GRAPHSON1_READER
          .get()
          .readProperty(
              new ByteArrayInputStream(delegate.toString().getBytes(StandardCharsets.UTF_8)),
              Attachable::get);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not deserialize node as Property.", e);
    }
  }

  @Override
  public boolean isVertexProperty() {
    return delegate.has(GraphSONTokens.ID)
        && delegate.has(GraphSONTokens.VALUE)
        && delegate.has(GraphSONTokens.LABEL);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> VertexProperty<T> asVertexProperty() {
    try {
      return GraphSONUtils.GRAPHSON1_READER
          .get()
          .readVertexProperty(
              new ByteArrayInputStream(delegate.toString().getBytes(StandardCharsets.UTF_8)),
              Attachable::get);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not deserialize node as VertexProperty.", e);
    }
  }

  @Override
  public boolean isSet() {
    return false;
  }

  @Override
  public <T> Set<T> asSet() {
    throw new UnsupportedOperationException(
        "GraphSON1 does not support Set, use another Graph sub-protocol such as GraphSON2.");
  }

  @Override
  public <ResultT> ResultT as(Class<ResultT> clazz) {
    try {
      return objectMapper.treeToValue(delegate, clazz);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not deserialize node as: " + clazz, e);
    }
  }

  @Override
  public <ResultT> ResultT as(GenericType<ResultT> type) {
    try {
      JsonParser parser = objectMapper.treeAsTokens(delegate);
      JavaType javaType = objectMapper.constructType(type.__getToken().getType());
      return objectMapper.readValue(parser, javaType);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not deserialize node as: " + type, e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LegacyGraphNode)) {
      return false;
    }
    LegacyGraphNode that = (LegacyGraphNode) o;
    return Objects.equal(delegate, that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(delegate);
  }

  @Override
  public String toString() {
    return delegate.toString();
  }

  private boolean isType(String expectedTypeName) {
    JsonNode type = delegate.get(TYPE);
    return type != null && expectedTypeName.equals(type.asText());
  }
}
