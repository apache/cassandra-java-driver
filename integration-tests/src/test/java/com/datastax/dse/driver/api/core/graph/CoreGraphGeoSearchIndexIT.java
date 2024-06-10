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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.requirement.BackendRequirement;
import com.datastax.oss.driver.api.testinfra.requirement.BackendType;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.Collection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@BackendRequirement(
    type = BackendType.DSE,
    minInclusive = "6.8.0",
    description = "DSE 6.8.0 required for Core graph support")
public class CoreGraphGeoSearchIndexIT extends GraphGeoSearchIndexITBase {

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

  @BeforeClass
  public static void setup() {
    for (String setupQuery : geoIndices()) {
      SESSION_RULE.session().execute(ScriptGraphStatement.newInstance(setupQuery));
    }

    CCM_RULE.getCcmBridge().reloadCore(1, SESSION_RULE.getGraphName(), "user", true);
  }

  /**
   * A schema representing an address book with search enabled on name, description, and
   * coordinates.
   */
  public static Collection<String> geoIndices() {
    Object[][] providerIndexTypes = indexTypes();
    String[] indexTypes = new String[providerIndexTypes.length];
    for (int i = 0; i < providerIndexTypes.length; i++) {
      indexTypes[i] = (String) providerIndexTypes[i][0];
    }

    StringBuilder schema =
        new StringBuilder("schema.vertexLabel('user').partitionBy('full_name', Text)");
    StringBuilder propertyKeys = new StringBuilder();
    StringBuilder indices = new StringBuilder();
    StringBuilder vertex0 =
        new StringBuilder("g.addV('user').property('full_name', 'Paul Thomas Joe')");
    StringBuilder vertex1 =
        new StringBuilder("g.addV('user').property('full_name', 'George Bill Steve')");
    String vertex2 = "g.addV('user').property('full_name', 'James Paul Joe')";
    StringBuilder vertex3 = new StringBuilder("g.addV('user').property('full_name', 'Jill Alice')");

    for (String indexType : indexTypes) {
      propertyKeys.append(String.format(".property('pointPropWithBounds_%s', Point)\n", indexType));

      propertyKeys.append(
          String.format(".property('pointPropWithGeoBounds_%s', Point)\n", indexType));

      if (indexType.equals("search")) {
        indices.append(
            String.format(
                "schema.vertexLabel('user').searchIndex().by('pointPropWithBounds_%s').by('pointPropWithGeoBounds_%s').create()\n",
                indexType, indexType));

      } else {
        throw new UnsupportedOperationException("IndexType other than search is not supported.");
      }

      vertex0.append(
          String.format(
              ".property('pointPropWithBounds_%s', point(40.0001,40)).property('pointPropWithGeoBounds_%s', point(40.0001,40))",
              indexType, indexType));
      vertex1.append(
          String.format(
              ".property('pointPropWithBounds_%s', point(40,40)).property('pointPropWithGeoBounds_%s', point(40,40))",
              indexType, indexType));
      vertex3.append(
          String.format(
              ".property('pointPropWithBounds_%s', point(30,30)).property('pointPropWithGeoBounds_%s', point(30,30))",
              indexType, indexType));
    }

    schema.append(propertyKeys).append(".create();\n").append(indices);

    return Lists.newArrayList(
        schema.toString(), vertex0.toString(), vertex1.toString(), vertex2, vertex3.toString());
  }
}
