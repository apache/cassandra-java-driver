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
package com.datastax.dse.driver.api.core.graph;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Collection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "6.8.0", description = "DSE 6.8.0 required for Core graph support")
public class CoreGraphTextSearchIndexIT extends GraphTextSearchIndexITBase {

  private static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder().withDseWorkloads("graph", "solr").build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      GraphTestSupport.getCoreGraphSessionBuilder(CCM_RULE).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private final GraphTraversalSource g =
      AnonymousTraversalSource.traversal()
          .withRemote(DseGraph.remoteConnectionBuilder(SESSION_RULE.session()).build())
          .with("allow-filtering");

  @Override
  protected boolean isGraphBinary() {
    return true;
  }

  @Override
  protected GraphTraversalSource graphTraversalSource() {
    return g;
  }

  /**
   * A schema representing an address book with 3 properties (full_name_*, description_*, alias_*)
   * created for each type of index (search, secondary, materialized).
   */
  public static Collection<String> textIndices() {
    Object[][] providerIndexTypes = indexTypes();
    String[] indexTypes = new String[providerIndexTypes.length];
    for (int i = 0; i < providerIndexTypes.length; i++) {
      indexTypes[i] = (String) providerIndexTypes[i][0];
    }

    StringBuilder schema = new StringBuilder("schema.vertexLabel('user')");
    StringBuilder propertyKeys = new StringBuilder();
    StringBuilder indices = new StringBuilder();
    StringBuilder vertex0 = new StringBuilder("g.addV('user')");
    StringBuilder vertex1 = new StringBuilder("g.addV('user')");
    StringBuilder vertex2 = new StringBuilder("g.addV('user')");
    StringBuilder vertex3 = new StringBuilder("g.addV('user')");

    for (String indexType : indexTypes) {
      propertyKeys.append(
          String.format(
              ".partitionBy('full_name_%s', Text)"
                  + ".property('description_%s', Text)"
                  + ".property('alias_%s', Text)\n",
              indexType, indexType, indexType));

      if (indexType.equals("search")) {
        indices.append(
            "schema.vertexLabel('user').searchIndex().by('full_name_search').asString().by('description_search').asText().by('alias_search').asString().create()\n");
      } else {
        throw new UnsupportedOperationException("IndexType other than search is not supported.");
      }

      vertex0.append(
          String.format(
              ".property('full_name_%s', 'Paul Thomas Joe').property('description_%s', 'Lives by the hospital').property('alias_%s', 'mario')",
              indexType, indexType, indexType));
      vertex1.append(
          String.format(
              ".property('full_name_%s', 'George Bill Steve').property('description_%s', 'A cold dude').property('alias_%s', 'wario')",
              indexType, indexType, indexType));
      vertex2.append(
          String.format(
              ".property('full_name_%s', 'James Paul Joe').property('description_%s', 'Likes to hang out').property('alias_%s', 'bowser')",
              indexType, indexType, indexType));
      vertex3.append(
          String.format(
              ".property('full_name_%s', 'Jill Alice').property('description_%s', 'Enjoys a very nice cold coca cola').property('alias_%s', 'peach')",
              indexType, indexType, indexType));
    }

    schema.append(propertyKeys).append(".create();\n").append(indices);

    return Lists.newArrayList(
        schema.toString(),
        vertex0.toString(),
        vertex1.toString(),
        vertex2.toString(),
        vertex3.toString());
  }

  @BeforeClass
  public static void setup() {
    for (String setupQuery : textIndices()) {
      SESSION_RULE.session().execute(ScriptGraphStatement.newInstance(setupQuery));
    }

    CCM_RULE.getCcmBridge().reloadCore(1, SESSION_RULE.getGraphName(), "user", true);
  }
}
