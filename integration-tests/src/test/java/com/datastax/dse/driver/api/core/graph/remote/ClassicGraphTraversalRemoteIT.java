/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph.remote;

import com.datastax.dse.driver.api.core.DseSession;
import com.datastax.dse.driver.api.core.graph.DseGraph;
import com.datastax.dse.driver.api.core.graph.GraphTestSupport;
import com.datastax.dse.driver.api.core.graph.SampleGraphScripts;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.SocialTraversalSource;
import com.datastax.dse.driver.api.testinfra.session.DseSessionRule;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(
    min = "5.0.9",
    description = "DSE 5.0.9 required for inserting edges and vertices script.")
public class ClassicGraphTraversalRemoteIT extends GraphTraversalRemoteITBase {

  private static final CustomCcmRule CCM_RULE = GraphTestSupport.GRAPH_CCM_RULE_BUILDER.build();

  private static final DseSessionRule SESSION_RULE =
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
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.CLASSIC_GRAPH));
    SESSION_RULE
        .session()
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.MAKE_STRICT));
  }

  @Override
  protected DseSession session() {
    return SESSION_RULE.session();
  }

  @Override
  protected boolean isGraphBinary() {
    return false;
  }

  @Override
  protected GraphTraversalSource graphTraversalSource() {
    return AnonymousTraversalSource.traversal()
        .withRemote(DseGraph.remoteConnectionBuilder(session()).build());
  }

  @Override
  protected SocialTraversalSource socialTraversalSource() {
    return EmptyGraph.instance()
        .traversal(SocialTraversalSource.class)
        .withRemote(DseGraph.remoteConnectionBuilder(session()).build());
  }

  @Override
  protected CustomCcmRule ccmRule() {
    return CCM_RULE;
  }
}
