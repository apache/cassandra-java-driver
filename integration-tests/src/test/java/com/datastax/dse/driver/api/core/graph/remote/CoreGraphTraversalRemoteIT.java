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

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.GraphTestSupport;
import com.datastax.dse.driver.api.core.graph.SampleGraphScripts;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.SocialTraversalSource;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "6.8",
    description = "DSE 6.8 required for Core graph support")
public class CoreGraphTraversalRemoteIT extends GraphTraversalRemoteITBase {

  private static final CustomCcmRule CCM_RULE = GraphTestSupport.GRAPH_CCM_RULE_BUILDER.build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      GraphTestSupport.getCoreGraphSessionBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void setupSchema() {
    SESSION_RULE.session().execute(ScriptGraphStatement.newInstance(SampleGraphScripts.CORE_GRAPH));
  }

  @Override
  protected CqlSession session() {
    return SESSION_RULE.session();
  }

  @Override
  protected boolean isGraphBinary() {
    return true;
  }

  @Override
  protected GraphTraversalSource graphTraversalSource() {
    return traversal()
        .withRemote(DseGraph.remoteConnectionBuilder(session()).build())
        .with("allow-filtering");
  }

  @Override
  protected SocialTraversalSource socialTraversalSource() {
    return traversal(SocialTraversalSource.class)
        .withRemote(DseGraph.remoteConnectionBuilder(session()).build())
        .with("allow-filtering");
  }

  @Override
  protected CustomCcmRule ccmRule() {
    return CCM_RULE;
  }
}
