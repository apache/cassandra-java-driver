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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * A node in a tree-like structure representing a Graph or a Graph component.
 *
 * <p>It can be:
 *
 * <ul>
 *   <li>a scalar value of a primitive type (boolean, string, int, long, double);
 *   <li>a graph element (vertex, edge, path or property);
 *   <li>a list of nodes;
 *   <li>a set of nodes;
 *   <li>a map of nodes.
 * </ul>
 *
 * This interface provides test methods to find out what a node represents, and conversion methods
 * to cast it to a particular Java type. Two generic methods {@link #as(Class)} and {@link
 * #as(GenericType)} can produce any arbitrary Java type, provided that the underlying serialization
 * runtime has been correctly configured to support the requested conversion.
 */
public interface GraphNode {

  /** Whether this node represents a {@code null} value. */
  boolean isNull();

  /**
   * Returns {@code true} if this node is a {@link Map}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, you can convert this node with {@link #asMap()}, or use
   * {@link #keys()} and {@link #getByKey(Object)} to access the individual fields (note that
   * entries are not ordered, so {@link #getByIndex(int)} does not work).
   */
  boolean isMap();

  /** The keys of this map node, or an empty iterator if it is not a map. */
  Iterable<?> keys();

  /**
   * Returns the value for the given key as a node.
   *
   * <p>If this node is not a map, or does not contain the specified key, {@code null} is returned.
   *
   * <p>If the property value has been explicitly set to {@code null}, implementors may return a
   * special "null node" instead of {@code null}.
   */
  GraphNode getByKey(Object key);

  /** Deserializes and returns this node as a {@link Map}. */
  <K, V> Map<K, V> asMap();

  /**
   * Returns {@code true} if this node is a {@link List}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, you can convert this node with {@link #asList()}, or
   * use {@link #size()} and {@link #getByIndex(int)} to access the individual fields.
   */
  boolean isList();

  /** The size of the current node, if it is a list or map, or {@code 0} otherwise. */
  int size();

  /**
   * Returns the element at the given index as a node.
   *
   * <p>If this node is not a list, or {@code index} is out of bounds (i.e. less than zero or {@code
   * >= size()}, {@code null} is returned; no exception will be thrown.
   *
   * <p>If the requested element has been explicitly set to {@code null}, implementors may return a
   * special "null node" instead of {@code null}.
   */
  GraphNode getByIndex(int index);

  /** Deserializes and returns this node as a {@link List}. */
  <T> List<T> asList();

  /**
   * Returns {@code true} if this node is a simple scalar value, (i.e., string, boolean or number),
   * and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, you can convert this node with {@link #asString()},
   * {@link #asBoolean()}, {@link #asInt()}, {@link #asLong()} or {@link #asDouble()}.
   */
  boolean isValue();

  /**
   * Returns this node as an integer.
   *
   * <p>If the underlying object is not convertible to integer, implementors may choose to either
   * throw {@link ClassCastException} or return [null | empty | some default value], whichever is
   * deemed more appropriate.
   */
  int asInt();

  /**
   * Returns this node as a boolean.
   *
   * <p>If the underlying object is not convertible to boolean, implementors may choose to either
   * throw {@link ClassCastException} or return [null | empty | some default value], whichever is
   * deemed more appropriate.
   */
  boolean asBoolean();

  /**
   * Returns this node as a long integer.
   *
   * <p>If the underlying object is not convertible to long, implementors may choose to either throw
   * {@link ClassCastException} or return [null | empty | some default value], whichever is deemed
   * more appropriate.
   */
  long asLong();

  /**
   * Returns this node as a long integer.
   *
   * <p>If the underlying object is not convertible to double, implementors may choose to either
   * throw {@link ClassCastException} or return [null | empty | some default value], whichever is
   * deemed more appropriate.
   */
  double asDouble();

  /**
   * A valid string representation of this node.
   *
   * <p>If the underlying object is not convertible to a string, implementors may choose to either
   * throw {@link ClassCastException} or return an empty string, whichever is deemed more
   * appropriate.
   */
  String asString();

  /**
   * Deserializes and returns this node as an instance of {@code clazz}.
   *
   * <p>Before attempting such a conversion, there must be an appropriate converter configured on
   * the underlying serialization runtime.
   */
  <ResultT> ResultT as(Class<ResultT> clazz);

  /**
   * Deserializes and returns this node as an instance of the given {@link GenericType type}.
   *
   * <p>Before attempting such a conversion, there must be an appropriate converter configured on
   * the underlying serialization runtime.
   */
  <ResultT> ResultT as(GenericType<ResultT> type);

  /**
   * Returns {@code true} if this node is a {@link Vertex}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, then {@link #asVertex()} can be safely called.
   */
  boolean isVertex();

  /** Returns this node as a Tinkerpop {@link Vertex}. */
  Vertex asVertex();

  /**
   * Returns {@code true} if this node is a {@link Edge}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, then {@link #asEdge()} can be safely called.
   */
  boolean isEdge();

  /** Returns this node as a Tinkerpop {@link Edge}. */
  Edge asEdge();

  /**
   * Returns {@code true} if this node is a {@link Path}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, then {@link #asPath()} can be safely called.
   */
  boolean isPath();

  /** Returns this node as a Tinkerpop {@link Path}. */
  Path asPath();

  /**
   * Returns {@code true} if this node is a {@link Property}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, then {@link #asProperty()} can be safely called.
   */
  boolean isProperty();

  /** Returns this node as a Tinkerpop {@link Property}. */
  <T> Property<T> asProperty();

  /**
   * Returns {@code true} if this node is a {@link VertexProperty}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, then {@link #asVertexProperty()} ()} can be safely
   * called.
   */
  boolean isVertexProperty();

  /** Returns this node as a Tinkerpop {@link VertexProperty}. */
  <T> VertexProperty<T> asVertexProperty();

  /**
   * Returns {@code true} if this node is a {@link Set}, and {@code false} otherwise.
   *
   * <p>If this method returns {@code true}, you can convert this node with {@link #asSet()}, or use
   * {@link #size()}.
   */
  boolean isSet();

  /** Deserializes and returns this node as a {@link Set}. */
  <T> Set<T> asSet();
}
