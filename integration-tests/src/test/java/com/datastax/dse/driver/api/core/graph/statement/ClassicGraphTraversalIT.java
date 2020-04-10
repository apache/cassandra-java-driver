/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.api.core.graph.statement;

import com.datastax.dse.driver.api.core.graph.GraphTestSupport;
import com.datastax.dse.driver.api.core.graph.SampleGraphScripts;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.SocialTraversalSource;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.BaseCcmRule;
import com.datastax.oss.driver.api.testinfra.ccm.CcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(
    min = "5.0.9",
    description = "DSE 5.0.9 required for inserting edges and vertices script.")
@Category(ParallelizableTests.class)
public class ClassicGraphTraversalIT extends GraphTraversalITBase {

  private static final CcmRule CCM_RULE = CcmRule.getInstance();

  private static final SessionRule<CqlSession> SESSION_RULE =
      GraphTestSupport.getClassicGraphSessionBuilder(CCM_RULE).build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private final GraphTraversalSource graphTraversalSource = EmptyGraph.instance().traversal();
  private final SocialTraversalSource socialTraversal =
      EmptyGraph.instance().traversal(SocialTraversalSource.class);

  @BeforeClass
  public static void setupSchema() {
    SESSION_RULE
        .session()
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.ALLOW_SCANS));
    SESSION_RULE
        .session()
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.CLASSIC_GRAPH));
    SESSION_RULE
        .session()
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.MAKE_STRICT));
  }

  @Override
  protected CqlSession session() {
    return SESSION_RULE.session();
  }

  @Override
  protected boolean isGraphBinary() {
    return false;
  }

  @Override
  protected BaseCcmRule ccmRule() {
    return CCM_RULE;
  }

  @Override
  protected GraphTraversalSource graphTraversalSource() {
    return graphTraversalSource;
  }

  @Override
  protected SocialTraversalSource socialTraversalSource() {
    return socialTraversal;
  }
}
