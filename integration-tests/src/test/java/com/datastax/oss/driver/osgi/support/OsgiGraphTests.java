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
import com.datastax.dse.driver.internal.core.graph.GraphProtocol;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import java.util.List;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public interface OsgiGraphTests extends OsgiSimpleTests {

  @Override
  default ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder() {
    return DriverConfigLoader.programmaticBuilder()
        .withString(DseDriverOption.GRAPH_NAME, "test_osgi_graph")
        .withString(
            DseDriverOption.GRAPH_SUB_PROTOCOL,
            getDseVersion().compareTo(Version.parse("6.8.0")) >= 0
                ? GraphProtocol.GRAPH_BINARY_1_0.toInternalCode()
                : GraphProtocol.GRAPHSON_2_0.toInternalCode());
  }

  Version getDseVersion();

  /**
   * Ensures a session can be established and a query using DSE Graph can be made when running in an
   * OSGi container.
   */
  default void connectAndQueryGraph() {

    try (CqlSession session = sessionBuilder().build()) {

      // Test that Graph + Tinkerpop is available
      session.execute(
          ScriptGraphStatement.newInstance("system.graph('test_osgi_graph').ifNotExists().create()")
              .setSystemQuery(true));

      if (getDseVersion().compareTo(Version.parse("6.8.0")) >= 0) {
        setUpCoreEngineGraph(session);
      } else {
        setUpClassicEngineGraph(session);
      }

      GraphResultSet resultSet =
          session.execute(FluentGraphStatement.newInstance(g.V().hasLabel("person")));
      List<GraphNode> results = resultSet.all();
      assertThat(results.size()).isEqualTo(1);
      Vertex actual = results.get(0).asVertex();
      assertThat(actual.label()).isEqualTo("person");
    }
  }

  default void setUpCoreEngineGraph(CqlSession session) {
    session.execute(
        ScriptGraphStatement.newInstance(
            "schema.vertexLabel('person').ifNotExists().partitionBy('pk', Int)"
                + ".clusterBy('cc', Int).property('name', Text).create();"));
    session.execute(
        ScriptGraphStatement.newInstance(
            "g.addV('person').property('pk',0).property('cc',0).property('name', 'alice');"));
  }

  default void setUpClassicEngineGraph(CqlSession session) {
    session.execute(
        ScriptGraphStatement.newInstance(
            "schema.propertyKey('name').Text().ifNotExists().create();"
                + "schema.vertexLabel('person').properties('name').ifNotExists().create();"));
    session.execute(
        ScriptGraphStatement.newInstance("g.addV('person').property('name', 'alice').next();"));
    session.execute(
        ScriptGraphStatement.newInstance(
            "schema.config().option('graph.allow_scan').set('true');"));
  }
}
