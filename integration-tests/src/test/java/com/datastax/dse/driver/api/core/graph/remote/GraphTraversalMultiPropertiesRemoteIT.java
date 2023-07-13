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
package com.datastax.dse.driver.api.core.graph.remote;

import static com.datastax.dse.driver.api.core.graph.SampleGraphScripts.ALLOW_SCANS;
import static com.datastax.dse.driver.api.core.graph.SampleGraphScripts.MAKE_STRICT;
import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;

import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import java.util.Iterator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "5.0.3", description = "DSE 5.0.3 required for remote TinkerPop support")
public class GraphTraversalMultiPropertiesRemoteIT {

  private static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule).withCreateGraph().build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  private final GraphTraversalSource g =
      DseGraph.g.withRemote(DseGraph.remoteConnectionBuilder(sessionRule.session()).build());

  /** Builds a simple schema that provides for a vertex with a multi-cardinality property. */
  public static final String multiProps =
      MAKE_STRICT
          + ALLOW_SCANS
          + "schema.propertyKey('multi_prop').Text().multiple().create()\n"
          + "schema.vertexLabel('multi_v').properties('multi_prop').create()\n";

  /**
   * Ensures that a traversal that yields a vertex with a property name that is present multiple
   * times that the properties are parsed and made accessible via {@link
   * Vertex#properties(String...)}.
   *
   * @test_category dse:graph
   */
  @Test
  public void should_parse_multiple_cardinality_properties() {
    // given a schema that defines multiple cardinality properties.
    sessionRule.session().execute(ScriptGraphStatement.newInstance(multiProps));

    // when adding a vertex with a multiple cardinality property
    Vertex v =
        g.addV("multi_v")
            .property("multi_prop", "Hello")
            .property("multi_prop", "Sweet")
            .property("multi_prop", "World")
            .next();

    // then the created vertex should have the multi-cardinality property present with its values.
    assertThat(v).hasProperty("multi_prop");
    Iterator<VertexProperty<String>> multiProp = v.properties("multi_prop");
    assertThat(multiProp)
        .toIterable()
        .extractingResultOf("value")
        .containsExactly("Hello", "Sweet", "World");
  }
}
