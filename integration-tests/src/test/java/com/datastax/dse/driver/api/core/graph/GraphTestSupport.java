/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.api.testinfra.session.DseSessionRuleBuilder;
import com.datastax.dse.driver.internal.core.graph.GraphProtocol;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;

/** Utility for creating commonly used Rule builders for tests. */
public class GraphTestSupport {

  /** CCM Rule builder for Graph Data Type tests. */
  public static final CustomCcmRule.Builder CCM_BUILDER_WITH_GRAPH =
      CustomCcmRule.builder()
          .withDseWorkloads("graph")
          .withDseConfiguration("graph.max_query_params", 32)
          .withDseConfiguration(
              "graph.gremlin_server.scriptEngines.gremlin-groovy.config.sandbox_enabled", "false");

  /** CCM Rule builder for general Graph workload tests. */
  public static final CustomCcmRule.Builder GRAPH_CCM_RULE_BUILDER =
      CustomCcmRule.builder().withDseWorkloads("graph");

  /**
   * Creates a session rule builder for Classic Graph workloads with the default Graph protocol. The
   * default GraphProtocol for Classic Graph: GraphSON 2.0.
   *
   * @param ccmRule CustomCcmRule configured for Graph workloads
   * @return A Session rule builder configured for Classic Graph workloads
   */
  public static DseSessionRuleBuilder getClassicGraphSessionBuilder(CustomCcmRule ccmRule) {
    return new DseSessionRuleBuilder(ccmRule)
        .withCreateGraph()
        .withGraphProtocol(GraphProtocol.GRAPHSON_2_0.toInternalCode());
  }

  /**
   * Creates a session rule builder for Core Graph workloads with the default Graph protocol. The
   * default GraphProtocol for Core Graph: Graph Binary 1.0.
   *
   * @param ccmRule CustomCcmRule configured for Graph workloads
   * @return A Session rule builder configured for Core Graph workloads
   */
  public static DseSessionRuleBuilder getCoreGraphSessionBuilder(CustomCcmRule ccmRule) {
    return new DseSessionRuleBuilder(ccmRule)
        .withCreateGraph()
        .withCoreEngine()
        .withGraphProtocol(GraphProtocol.GRAPH_BINARY_1_0.toInternalCode());
  }
}
