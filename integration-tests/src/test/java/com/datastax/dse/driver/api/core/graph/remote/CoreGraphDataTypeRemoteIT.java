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

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

import com.datastax.dse.driver.api.core.graph.CoreGraphDataTypeITBase;
import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.GraphTestSupport;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "6.8.0",
    description = "DSE 6.8.0 required for Core graph support")
@RunWith(DataProviderRunner.class)
public class CoreGraphDataTypeRemoteIT extends CoreGraphDataTypeITBase {

  private static final CustomCcmRule CCM_RULE = GraphTestSupport.CCM_BUILDER_WITH_GRAPH.build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      GraphTestSupport.getCoreGraphSessionBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @Override
  protected CqlSession session() {
    return SESSION_RULE.session();
  }

  @Override
  protected String graphName() {
    return SESSION_RULE.getGraphName();
  }

  private final GraphTraversalSource g =
      AnonymousTraversalSource.traversal()
          .withRemote(DseGraph.remoteConnectionBuilder(session()).build());

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
    return g.V().has(vertexLabel, "id", vertexID).valueMap().by(unfold()).next();
  }
}
