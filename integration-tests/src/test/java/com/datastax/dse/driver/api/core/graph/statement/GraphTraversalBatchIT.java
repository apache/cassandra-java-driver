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
package com.datastax.dse.driver.api.core.graph.statement;

import static com.datastax.dse.driver.api.core.graph.DseGraph.g;
import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.dse.driver.api.core.graph.BatchGraphStatement;
import com.datastax.dse.driver.api.core.graph.FluentGraphStatement;
import com.datastax.dse.driver.api.core.graph.SampleGraphScripts;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "6.0")
public class GraphTraversalBatchIT {

  private static CustomCcmRule ccmRule = CustomCcmRule.builder().withDseWorkloads("graph").build();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule).withCreateGraph().build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  @BeforeClass
  public static void setupSchema() {
    sessionRule.session().execute(ScriptGraphStatement.newInstance(SampleGraphScripts.ALLOW_SCANS));
    sessionRule
        .session()
        .execute(ScriptGraphStatement.newInstance(SampleGraphScripts.MAKE_NOT_STRICT));
  }

  @Test
  public void should_allow_vertex_and_edge_insertions_in_batch() {
    BatchGraphStatement batch =
        BatchGraphStatement.builder()
            .addTraversals(
                ImmutableList.of(
                    addV("person").property("name", "batch1").property("age", 1),
                    addV("person").property("name", "batch2").property("age", 2)))
            .build();

    BatchGraphStatement batch2 =
        BatchGraphStatement.builder()
            .addTraversals(batch)
            .addTraversal(
                addE("knows")
                    .from(__.<Edge>V().has("name", "batch1"))
                    .to(__.<Edge>V().has("name", "batch2"))
                    .property("weight", 2.3f))
            .build();

    assertThat(batch.size()).isEqualTo(2);
    assertThat(batch2.size()).isEqualTo(3);

    sessionRule.session().execute(batch2);

    assertThat(
            sessionRule
                .session()
                .execute(FluentGraphStatement.newInstance(g.V().has("name", "batch1")))
                .one()
                .asVertex())
        .hasProperty("age", 1);

    assertThat(
            sessionRule
                .session()
                .execute(FluentGraphStatement.newInstance(g.V().has("name", "batch2")))
                .one()
                .asVertex())
        .hasProperty("age", 2);

    assertThat(
            sessionRule
                .session()
                .execute(FluentGraphStatement.newInstance(g.V().has("name", "batch1").bothE()))
                .one()
                .asEdge())
        .hasProperty("weight", 2.3f)
        .hasOutVLabel("person")
        .hasInVLabel("person");
  }

  @Test
  public void should_fail_if_no_bytecode_in_batch() {
    BatchGraphStatement batch =
        BatchGraphStatement.builder().addTraversals(ImmutableList.of()).build();
    assertThat(batch.size()).isEqualTo(0);
    try {
      sessionRule.session().execute(batch);
      fail(
          "Should have thrown InvalidQueryException because batch does not contain any traversals.");
    } catch (InvalidQueryException e) {
      assertThat(e.getMessage())
          .contains(
              "Could not read the traversal from the request sent.",
              "The batch statement sent does not contain any traversal.");
    }
  }
}
