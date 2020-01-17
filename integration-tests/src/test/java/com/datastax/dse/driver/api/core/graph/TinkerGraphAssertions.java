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
package com.datastax.dse.driver.api.core.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

public class TinkerGraphAssertions extends com.datastax.oss.driver.assertions.Assertions {

  public static TinkerEdgeAssert assertThat(Edge edge) {
    return new TinkerEdgeAssert(edge);
  }

  public static TinkerVertexAssert assertThat(Vertex vertex) {
    return new TinkerVertexAssert(vertex);
  }

  public static <T> TinkerVertexPropertyAssert<T> assertThat(VertexProperty<T> vertexProperty) {
    return new TinkerVertexPropertyAssert<T>(vertexProperty);
  }

  public static TinkerPathAssert assertThat(Path path) {
    return new TinkerPathAssert(path);
  }

  public static <T> TinkerTreeAssert<T> assertThat(Tree<T> tree) {
    return new TinkerTreeAssert<>(tree);
  }
}
