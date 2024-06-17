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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.dse.driver.internal.core.graph.GraphProtocol;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.CqlSessionRuleBuilder;

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
  public static CqlSessionRuleBuilder getClassicGraphSessionBuilder(CustomCcmRule ccmRule) {
    return new CqlSessionRuleBuilder(ccmRule)
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
  public static CqlSessionRuleBuilder getCoreGraphSessionBuilder(CustomCcmRule ccmRule) {
    return new CqlSessionRuleBuilder(ccmRule)
        .withCreateGraph()
        .withCoreEngine()
        .withGraphProtocol(GraphProtocol.GRAPH_BINARY_1_0.toInternalCode());
  }
}
