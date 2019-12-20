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

import com.datastax.dse.driver.api.core.graph.CoreGraphDataTypeITBase;
import com.datastax.dse.driver.api.core.graph.GraphTestSupport;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatement;
import com.datastax.dse.driver.api.core.graph.ScriptGraphStatementBuilder;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.util.Map;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@DseRequirement(min = "6.8.0", description = "DSE 6.8.0 required for Core graph support")
@RunWith(DataProviderRunner.class)
public class CoreGraphDataTypeScriptIT extends CoreGraphDataTypeITBase {

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

  @Override
  protected Map<Object, Object> insertVertexThenReadProperties(
      Map<String, Object> properties, int vertexID, String vertexLabel) {
    StringBuilder insert = new StringBuilder("g.addV(vertexLabel).property('id', vertexID)");

    ScriptGraphStatementBuilder statementBuilder =
        new ScriptGraphStatementBuilder()
            .setQueryParam("vertexID", vertexID)
            .setQueryParam("vertexLabel", vertexLabel);

    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      String typeDefinition = entry.getKey();
      String propName = formatPropertyName(typeDefinition);
      Object value = entry.getValue();

      insert.append(String.format(".property('%s', %s)", propName, propName));
      statementBuilder = statementBuilder.setQueryParam(propName, value);
    }

    session().execute(statementBuilder.setScript(insert.toString()).build());

    return session()
        .execute(
            ScriptGraphStatement.newInstance(
                    "g.V().has(vertexLabel, 'id', vertexID).valueMap().by(unfold())")
                .setQueryParam("vertexID", vertexID)
                .setQueryParam("vertexLabel", vertexLabel))
        .one()
        .asMap();
  }
}
