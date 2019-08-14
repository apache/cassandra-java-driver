/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.remote;

import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.NativeGraphDataTypeITBase;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@DseRequirement(min = "6.8.0", description = "DSE 6.8.0 required for Native graph support")
@RunWith(DataProviderRunner.class)
@Category(IsolatedTests.class)
public class NativeGraphDataTypeRemoteIT extends NativeGraphDataTypeITBase {

  private final GraphTraversalSource g =
      DseGraph.g.withRemote(DseGraph.remoteConnectionBuilder(session()).build());

  @Override
  public Map<Object, Object> insertVertexThenReadProperties(
      Map<String, Object> properties, int vertexID, String vertexLabel) {
    GraphTraversal<Vertex, Vertex> traversal = g.addV(vertexLabel).property("id", vertexID);

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String typeDefinition = entry.getKey();
      String propName = formatPropertyName(typeDefinition);
      Object value = entry.getValue();
      traversal = traversal.property(propName, value);
    }

    // insert vertex
    traversal.iterate();

    // query properties
    return g.V().has(vertexLabel, "id", vertexID).valueMap().next();
  }
}
