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

import com.datastax.dse.driver.api.core.graph.ClassicGraphDataTypeITBase;
import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.GraphTestSupport;
import com.datastax.dse.driver.api.core.graph.SampleGraphScripts;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "5.0.3",
    description = "DSE 5.0.3 required for remote TinkerPop support")
public class ClassicGraphDataTypeRemoteIT extends ClassicGraphDataTypeITBase {

  private static final CustomCcmRule CCM_RULE = GraphTestSupport.GRAPH_CCM_RULE_BUILDER.build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      GraphTestSupport.getClassicGraphSessionBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void setupSchema() {
    SESSION_RULE
        .session()
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.ALLOW_SCANS));
    SESSION_RULE
        .session()
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.MAKE_STRICT));
  }

  @Override
  public CqlSession session() {
    return SESSION_RULE.session();
  }

  private final GraphTraversalSource g =
      AnonymousTraversalSource.traversal()
          .withRemote(DseGraph.remoteConnectionBuilder(SESSION_RULE.session()).build());

  @Override
  public Vertex insertVertexAndReturn(String vertexLabel, String propertyName, Object value) {
    return g.addV(vertexLabel).property(propertyName, value).next();
  }
}
