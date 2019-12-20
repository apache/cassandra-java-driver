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
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@DseRequirement(min = "5.1", description = "DSE 5.1 required for graph geo indexing")
public class ClassicGraphTextSearchIndexIT extends GraphTextSearchIndexITBase {
  private static final CustomCcmRule CCM_RULE =
      CustomCcmRule.builder().withDseWorkloads("graph", "solr").build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      GraphTestSupport.getClassicGraphSessionBuilder(CCM_RULE).build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  private final GraphTraversalSource g =
      AnonymousTraversalSource.traversal()
          .withRemote(DseGraph.remoteConnectionBuilder(SESSION_RULE.session()).build());

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

    StringBuilder schema = new StringBuilder();
    StringBuilder propertyKeys = new StringBuilder();
    StringBuilder vertexLabel = new StringBuilder("schema.vertexLabel('user').properties(");
    StringBuilder indices = new StringBuilder();
    StringBuilder vertex0 = new StringBuilder("g.addV('user')");
    StringBuilder vertex1 = new StringBuilder("g.addV('user')");
    StringBuilder vertex2 = new StringBuilder("g.addV('user')");
    StringBuilder vertex3 = new StringBuilder("g.addV('user')");

    ArrayList<String> propertyNames = new ArrayList<>();
    for (String indexType : indexTypes) {
      propertyKeys.append(
          String.format(
              "schema.propertyKey('full_name_%s').Text().create()\n"
                  + "schema.propertyKey('description_%s').Text().create()\n"
                  + "schema.propertyKey('alias_%s').Text().create()\n",
              indexType, indexType, indexType));

      propertyNames.add("'full_name_" + indexType + "'");
      propertyNames.add("'description_" + indexType + "'");
      propertyNames.add("'alias_" + indexType + "'");

      if (indexType.equals("search")) {
        indices.append(
            "schema.vertexLabel('user').index('search').search().by('full_name_search').asString().by('description_search').asText().by('alias_search').asString().add()\n");
      } else {
        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_full_name_%s').%s().by('full_name_%s').add()\n",
                indexType, indexType, indexType));
        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_description_%s').%s().by('description_%s').add()\n",
                indexType, indexType, indexType));
        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_alias_name_%s').%s().by('alias_%s').add()\n",
                indexType, indexType, indexType));
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

    vertexLabel.append(Joiner.on(", ").join(propertyNames));
    vertexLabel.append(").create()\n");

    schema.append(propertyKeys).append(vertexLabel).append(indices);

    return Lists.newArrayList(
        SampleGraphScripts.MAKE_STRICT,
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

    CCM_RULE.getCcmBridge().reloadCore(1, SESSION_RULE.getGraphName(), "user_p", true);
  }

  @Override
  protected boolean isGraphBinary() {
    return false;
  }

  @Override
  protected GraphTraversalSource graphTraversalSource() {
    return g;
  }
}
