/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.statement;

import static com.datastax.dse.driver.api.core.graph.DseGraph.g;

import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.NativeGraphDataTypeITBase;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.categories.IsolatedTests;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@DseRequirement(min = "6.8.0", description = "DSE 6.8.0 required for Native graph support")
@RunWith(DataProviderRunner.class)
@Category(IsolatedTests.class)
public class NativeGraphDataTypeFluentIT extends NativeGraphDataTypeITBase {

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

    session().execute(FluentGraphStatement.newInstance(traversal));

    return session()
        .execute(
            FluentGraphStatement.newInstance(g.V().has(vertexLabel, "id", vertexID).valueMap()))
        .one()
        .asMap();
  }
}
