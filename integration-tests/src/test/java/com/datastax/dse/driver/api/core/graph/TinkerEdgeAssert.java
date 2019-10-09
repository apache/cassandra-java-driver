/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.assertions.Assertions;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class TinkerEdgeAssert extends TinkerElementAssert<TinkerEdgeAssert, Edge> {

  public TinkerEdgeAssert(Edge actual) {
    super(actual, TinkerEdgeAssert.class);
  }

  public TinkerEdgeAssert hasInVLabel(String label) {
    Assertions.assertThat(actual.inVertex().label()).isEqualTo(label);
    return myself;
  }

  public TinkerEdgeAssert hasOutVLabel(String label) {
    Assertions.assertThat(actual.outVertex().label()).isEqualTo(label);
    return myself;
  }

  public TinkerEdgeAssert hasOutV(Vertex vertex) {
    Assertions.assertThat(actual.outVertex()).isEqualTo(vertex);
    return myself;
  }

  public TinkerEdgeAssert hasInV(Vertex vertex) {
    Assertions.assertThat(actual.inVertex()).isEqualTo(vertex);
    return myself;
  }
}
