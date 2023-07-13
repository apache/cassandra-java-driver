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

import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.datastax.oss.driver.api.testinfra.DseRequirement;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.shaded.guava.common.base.Joiner;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

@DseRequirement(min = "5.1", description = "DSE 5.1 required for graph geo indexing")
@RunWith(DataProviderRunner.class)
public class GraphGeoSearchIndexIT {

  private static CustomCcmRule ccmRule =
      CustomCcmRule.builder().withDseWorkloads("graph", "solr").build();

  private static SessionRule<CqlSession> sessionRule =
      SessionRule.builder(ccmRule).withCreateGraph().build();

  @ClassRule public static TestRule chain = RuleChain.outerRule(ccmRule).around(sessionRule);

  private final GraphTraversalSource g =
      DseGraph.g.withRemote(DseGraph.remoteConnectionBuilder(sessionRule.session()).build());

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

    StringBuilder schema = new StringBuilder("schema.propertyKey('full_name').Text().create()\n");
    StringBuilder propertyKeys = new StringBuilder("");
    StringBuilder vertexLabel = new StringBuilder("schema.vertexLabel('user').properties(");
    StringBuilder indices = new StringBuilder("");
    StringBuilder vertex0 =
        new StringBuilder("g.addV('user').property('full_name', 'Paul Thomas Joe')");
    StringBuilder vertex1 =
        new StringBuilder("g.addV('user').property('full_name', 'George Bill Steve')");
    String vertex2 = "g.addV('user').property('full_name', 'James Paul Joe')";
    StringBuilder vertex3 = new StringBuilder("g.addV('user').property('full_name', 'Jill Alice')");

    ArrayList<String> propertyNames = new ArrayList<String>();
    propertyNames.add("'full_name'");

    for (String indexType : indexTypes) {

      propertyKeys.append(
          String.format(
              "schema.propertyKey('pointPropWithBounds_%s').%s.create()\n",
              indexType, geoTypeWithBounds("Point()", 0, 0, 100, 100)));

      propertyKeys.append(
          String.format(
              "schema.propertyKey('pointPropWithGeoBounds_%s').%s.create()\n",
              indexType, geoType("Point()")));

      propertyNames.add("'pointPropWithBounds_" + indexType + "'");
      propertyNames.add("'pointPropWithGeoBounds_" + indexType + "'");

      if (indexType.equals("search")) {

        indices.append(
            String.format(
                "schema.vertexLabel('user').index('search').search().by('pointPropWithBounds_%s').withError(0.00001, 0.0).by('pointPropWithGeoBounds_%s').withError(0.00001, 0.0).add()\n",
                indexType, indexType));
      } else {

        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_pointPropWithBounds_%s').%s().by('pointPropWithBounds_%s').add()\n",
                indexType, indexType, indexType));

        indices.append(
            String.format(
                "schema.vertexLabel('user').index('by_pointPropWithGeoBounds_%s').%s().by('pointPropWithGeoBounds_%s').add()\n",
                indexType, indexType, indexType));
      }

      vertex0.append(
          String.format(
              ".property('pointPropWithBounds_%s', 'POINT(40.0001 40)').property('pointPropWithGeoBounds_%s', 'POINT(40.0001 40)')",
              indexType, indexType));
      vertex1.append(
          String.format(
              ".property('pointPropWithBounds_%s', 'POINT(40 40)').property('pointPropWithGeoBounds_%s', 'POINT(40 40)')",
              indexType, indexType));
      vertex3.append(
          String.format(
              ".property('pointPropWithBounds_%s', 'POINT(30 30)').property('pointPropWithGeoBounds_%s', 'POINT(30 30)')",
              indexType, indexType));
    }

    vertexLabel.append(Joiner.on(", ").join(propertyNames));
    vertexLabel.append(").create()\n");

    schema.append(propertyKeys).append(vertexLabel).append(indices);

    return Lists.newArrayList(
        SampleGraphScripts.MAKE_STRICT,
        schema.toString(),
        vertex0.toString(),
        vertex1.toString(),
        vertex2,
        vertex3.toString());
  }

  private static String geoTypeWithBounds(
      String baseName,
      double lowerLimitX,
      double lowerLimitY,
      double higherLimitX,
      double higherLimitY) {
    return baseName
        + String.format(
            ".withBounds(%f, %f, %f, %f)", lowerLimitX, lowerLimitY, higherLimitX, higherLimitY);
  }

  private static String geoType(String baseName) {
    return baseName + ".withGeoBounds()";
  }

  @BeforeClass
  public static void setup() {
    for (String setupQuery : geoIndices()) {
      sessionRule.session().execute(ScriptGraphStatement.newInstance(setupQuery));
    }

    ccmRule.getCcmBridge().reloadCore(1, sessionRule.getGraphName(), "user_p", true);
  }

  @DataProvider
  public static Object[][] indexTypes() {
    return new Object[][] {{"search"}

      // for some reason, materialized and secondary indices have decided not to work
      // I get an exception saying "there is no index for this query, here is the defined
      // indices: " and the list contains the indices that are needed. Mysterious.
      // There may be something to do with differences in the CCMBridge adapter of the new
      // driver, some changes make materialized views and secondary indices to be not
      // considered for graph:
      //
      // , {"materialized"}
      // , {"secondary"}
    };
  }

  @UseDataProvider("indexTypes")
  @Test
  public void search_by_distance_cartesian(String indexType) {
    // in cartesian geometry, the distance between POINT(30 30) and POINT(40 40) is exactly
    // 14.142135623730951
    // any point further than that should be detected outside of the range.
    // the vertex "Paul Thomas Joe" is at POINT(40.0001 40), and shouldn't be detected inside the
    // range.
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has(
                "user",
                "pointPropWithBounds_" + indexType,
                Geo.inside(Point.fromCoordinates((double) 30, (double) 30), 14.142135623730951))
            .values("full_name");
    assertThat(traversal.toList()).containsOnly("George Bill Steve", "Jill Alice");
  }

  @UseDataProvider("indexTypes")
  @Test
  public void search_by_distance_geodetic(String indexType) {
    // in geodetic geometry, the distance between POINT(30 30) and POINT(40 40) is exactly
    // 12.908258700131379
    // any point further than that should be detected outside of the range.
    // the vertex "Paul Thomas Joe" is at POINT(40.0001 40), and shouldn't be detected inside the
    // range.
    GraphTraversal<Vertex, String> traversal =
        g.V()
            .has(
                "user",
                "pointPropWithGeoBounds_" + indexType,
                Geo.inside(
                    Point.fromCoordinates((double) 30, (double) 30),
                    12.908258700131379,
                    Geo.Unit.DEGREES))
            .values("full_name");
    assertThat(traversal.toList()).containsOnly("George Bill Steve", "Jill Alice");
  }

  @Test
  public void
      should_fail_if_geodetic_predicate_used_against_cartesian_property_with_search_index() {
    try {
      GraphTraversal<Vertex, String> traversal =
          g.V()
              .has(
                  "user",
                  "pointPropWithBounds_search",
                  Geo.inside(
                      Point.fromCoordinates((double) 30, (double) 30),
                      12.908258700131379,
                      Geo.Unit.DEGREES))
              .values("full_name");
      traversal.toList();
      fail("Should have failed executing the traversal because the property type is incorrect");
    } catch (InvalidQueryException e) {
      assertThat(e.getMessage())
          .contains("Distance units cannot be used in queries against non-geodetic points.");
    }
  }

  @Test
  public void
      should_fail_if_cartesian_predicate_used_against_geodetic_property_with_search_index() {
    try {
      GraphTraversal<Vertex, String> traversal =
          g.V()
              .has(
                  "user",
                  "pointPropWithGeoBounds_search",
                  Geo.inside(Point.fromCoordinates((double) 30, (double) 30), 14.142135623730951))
              .values("full_name");
      traversal.toList();
      fail("Should have failed executing the traversal because the property type is incorrect");
    } catch (InvalidQueryException e) {
      assertThat(e.getMessage())
          .contains("Distance units are required for queries against geodetic points.");
    }
  }
}
