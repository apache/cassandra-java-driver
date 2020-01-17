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

import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Objects;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.Immutable;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Modern implementation for GraphSON 2+ results.
 *
 * <p>The server returns results with type information. The driver works with the decoded objects
 * directly.
 */
@Immutable
public class ObjectGraphNode implements GraphNode {

  private final Object delegate;

  public ObjectGraphNode(Object delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean isNull() {
    return delegate == null;
  }

  @Override
  public boolean isMap() {
    return delegate instanceof Map;
  }

  @Override
  public Iterable<?> keys() {
    return ((Map<?, ?>) delegate).keySet();
  }

  @Override
  public GraphNode getByKey(Object key) {
    if (!isMap()) {
      return null;
    }
    Map<?, Object> map = asMap();
    if (map.containsKey(key)) {
      return new ObjectGraphNode(map.get(key));
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> Map<K, V> asMap() {
    return (Map<K, V>) delegate;
  }

  @Override
  public boolean isList() {
    return delegate instanceof List;
  }

  @Override
  public int size() {
    if (isList()) {
      return asList().size();
    } else if (isMap()) {
      return asMap().size();
    } else if (isSet()) {
      return asSet().size();
    } else {
      return 0;
    }
  }

  @Override
  public GraphNode getByIndex(int index) {
    if (!isList() || index < 0 || index >= size()) {
      return null;
    }
    return new ObjectGraphNode(asList().get(index));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> List<T> asList() {
    return (List<T>) delegate;
  }

  @Override
  public boolean isValue() {
    return !(isList()
        || isMap()
        || isSet()
        || isVertex()
        || isEdge()
        || isPath()
        || isProperty()
        || isVertexProperty());
  }

  @Override
  public boolean isVertexProperty() {
    return delegate instanceof VertexProperty;
  }

  @Override
  public boolean isProperty() {
    return delegate instanceof Property;
  }

  @Override
  public boolean isPath() {
    return delegate instanceof Path;
  }

  @Override
  public int asInt() {
    return (Integer) delegate;
  }

  @Override
  public boolean asBoolean() {
    return (Boolean) delegate;
  }

  @Override
  public long asLong() {
    return (Long) delegate;
  }

  @Override
  public double asDouble() {
    return (Double) delegate;
  }

  @Override
  public String asString() {
    return (String) delegate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(Class<T> clazz) {
    return (T) delegate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T as(GenericType<T> type) {
    return (T) delegate;
  }

  @Override
  public boolean isVertex() {
    return delegate instanceof Vertex;
  }

  @Override
  public Vertex asVertex() {
    return (Vertex) delegate;
  }

  @Override
  public boolean isEdge() {
    return delegate instanceof Edge;
  }

  @Override
  public Edge asEdge() {
    return (Edge) delegate;
  }

  @Override
  public Path asPath() {
    return (Path) delegate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Property<T> asProperty() {
    return (Property<T>) delegate;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> VertexProperty<T> asVertexProperty() {
    return (VertexProperty<T>) delegate;
  }

  @Override
  public boolean isSet() {
    return delegate instanceof Set;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> Set<T> asSet() {
    return (Set<T>) delegate;
  }

  @Override
  public String toString() {
    return this.delegate.toString();
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    // Compare each others' delegates.
    return other instanceof ObjectGraphNode
        && Objects.equal(this.delegate, ((ObjectGraphNode) other).delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(delegate);
  }
}
