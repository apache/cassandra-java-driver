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
package com.datastax.oss.driver.osgi.support;

import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.GraphNode;
import com.datastax.dse.driver.api.core.graph.GraphResultSet;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import java.util.List;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface OsgiGraphTests extends OsgiSimpleTests {

  String CREATE_GRAPH = "system.graph('%s').ifNotExists().create()";

  String GRAPH_SCHEMA =
      "schema.propertyKey('name').Text().ifNotExists().create();"
          + "schema.vertexLabel('person').properties('name').ifNotExists().create();";

  String GRAPH_DATA = "g.addV('person').property('name', 'alice').next();";

  String ALLOW_SCANS = "schema.config().option('graph.allow_scan').set('true');";

  @Override
  default ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return DriverConfigLoader.programmaticBuilder()
        .withString(DseDriverOption.GRAPH_NAME, "test_osgi_graph");
  }

  /**
   * Ensures a session can be established and a query using DSE Graph can be made when running in an
   * OSGi container.
   */
  default void connectAndQueryGraph() {

    try (CqlSession session = sessionBuilder().build()) {

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
