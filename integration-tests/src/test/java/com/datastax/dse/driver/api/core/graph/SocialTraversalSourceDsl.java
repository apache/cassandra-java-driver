/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class SocialTraversalSourceDsl extends GraphTraversalSource {

  public SocialTraversalSourceDsl(
      final Graph graph, final TraversalStrategies traversalStrategies) {
    super(graph, traversalStrategies);
  }

  public SocialTraversalSourceDsl(final Graph graph) {
    super(graph);
  }

  public GraphTraversal<Vertex, Vertex> persons(String... names) {
    GraphTraversalSource clone = this.clone();

    // Manually add a "start" step for the traversal in this case the equivalent of V(). GraphStep
    // is marked
    // as a "start" step by passing "true" in the constructor.
    clone.getBytecode().addStep(GraphTraversal.Symbols.V);
    GraphTraversal<Vertex, Vertex> traversal = new DefaultGraphTraversal<>(clone);
    traversal.asAdmin().addStep(new GraphStep(traversal.asAdmin(), Vertex.class, true));

    traversal = traversal.hasLabel("person");
    if (names.length > 0) traversal = traversal.has("name", P.within(names));

    return traversal;
  }
}
