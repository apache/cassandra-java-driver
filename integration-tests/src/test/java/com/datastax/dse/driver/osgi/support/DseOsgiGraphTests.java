/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.osgi.support;

import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.testinfra.DseSessionBuilderInstantiator;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import java.util.List;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface DseOsgiGraphTests extends DseOsgiSimpleTests {

  String CREATE_GRAPH = "system.graph('%s').ifNotExists().create()";

  String GRAPH_SCHEMA =
      "schema.propertyKey('name').Text().ifNotExists().create();"
          + "schema.vertexLabel('person').properties('name').ifNotExists().create();";

  String GRAPH_DATA = "g.addV('person').property('name', 'alice').next();";

  String ALLOW_SCANS = "schema.config().option('graph.allow_scan').set('true');";

  @Override
  default ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return DseSessionBuilderInstantiator.configLoaderBuilder()
        .withString(DseDriverOption.GRAPH_NAME, "test_osgi_graph");
  }

  /**
   * Ensures a session can be established and a query using DSE Graph can be made when running in an
   * OSGi container.
   */
  default void connectAndQueryGraph() {

    try (DseSession session = sessionBuilder().build()) {

      // Test that Graph + Tinkerpop is available
      session.execute(
          ScriptGraphStatement.newInstance(String.format(CREATE_GRAPH, "test_osgi_graph"))
              .setSystemQuery(true));
      session.execute(ScriptGraphStatement.newInstance(GRAPH_SCHEMA));
      session.execute(ScriptGraphStatement.newInstance(GRAPH_DATA));
      session.execute(ScriptGraphStatement.newInstance(ALLOW_SCANS));

      GraphResultSet resultSet =
          session.execute(
              FluentGraphStatement.newInstance(g.V().hasLabel("person").has("name", "alice")));
      List<GraphNode> results = resultSet.all();
      assertThat(results.size()).isEqualTo(1);
      Vertex actual = results.get(0).asVertex();
      assertThat(actual.properties("name"))
          .toIterable()
          .extracting(Property::value)
          .contains("alice");
    }
  }
}
