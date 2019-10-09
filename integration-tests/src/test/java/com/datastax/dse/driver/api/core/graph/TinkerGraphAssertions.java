/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
