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

import static com.datastax.dse.driver.api.core.graph.TinkerGraphAssertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatCode;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.oss.driver.api.core.servererrors.InvalidQueryException;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.assertj.core.api.Assumptions;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DataProviderRunner.class)
public abstract class GraphGeoSearchIndexITBase {

  protected abstract boolean isGraphBinary();

  protected abstract GraphTraversalSource graphTraversalSource();

  @DataProvider
  public static Object[][] indexTypes() {
    return new Object[][] {{"search"}

      // FIXME for some reason, materialized and secondary indices have decided not to work
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
  public void search_by_distance_cartesian_graphson(String indexType) {
    // cartesian is not supported by graph_binary
    Assumptions.assumeThat(isGraphBinary()).isFalse();
    // in cartesian geometry, the distance between POINT(30 30) and POINT(40 40) is exactly
    // 14.142135623730951
    // any point further than that should be detected outside of the range.
    // the vertex "Paul Thomas Joe" is at POINT(40.0001 40), and shouldn't be detected inside the
    // range for classic.

    GraphTraversal<Vertex, String> traversal =
        graphTraversalSource()
            .V()
            .has(
                "user",
                "pointPropWithBounds_" + indexType,
                Geo.inside(Point.fromCoordinates(30, 30), 14.142135623730951))
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
        graphTraversalSource()
            .V()
            .has(
                "user",
                "pointPropWithGeoBounds_" + indexType,
                Geo.inside(Point.fromCoordinates(30, 30), 12.908258700131379, Geo.Unit.DEGREES))
            .values("full_name");
    assertThat(traversal.toList()).containsOnly("George Bill Steve", "Jill Alice");
  }

  @Test
  public void
      should_fail_if_geodetic_predicate_used_against_cartesian_property_with_search_index() {

    // for graph_binary cartesian properties are not supported, thus it does not fail
    if (isGraphBinary()) {
      assertThatCode(
              () -> {
                GraphTraversal<Vertex, String> traversal =
                    graphTraversalSource()
                        .V()
                        .has(
                            "user",
                            "pointPropWithBounds_search",
                            Geo.inside(
                                Point.fromCoordinates(30, 30),
                                12.908258700131379,
                                Geo.Unit.DEGREES))
                        .values("full_name");
                traversal.toList();
              })
          .doesNotThrowAnyException();
    } else {
      try {
        GraphTraversal<Vertex, String> traversal =
            graphTraversalSource()
                .V()
                .has(
                    "user",
                    "pointPropWithBounds_search",
                    Geo.inside(Point.fromCoordinates(30, 30), 12.908258700131379, Geo.Unit.DEGREES))
                .values("full_name");
        traversal.toList();
        fail("Should have failed executing the traversal because the property type is incorrect");
      } catch (InvalidQueryException e) {
        assertThat(e.getMessage())
            .contains("Distance units cannot be used in queries against non-geodetic points.");
      }
    }
  }

  @Test
  public void
      should_fail_if_cartesian_predicate_used_against_geodetic_property_with_search_index() {

    if (isGraphBinary()) {
      try {
        GraphTraversal<Vertex, String> traversal =
            graphTraversalSource()
                .V()
                .has(
                    "user",
                    "pointPropWithGeoBounds_search",
                    Geo.inside(Point.fromCoordinates(30, 30), 14.142135623730951))
                .values("full_name");
        traversal.toList();
        fail("Should have failed executing the traversal because the property type is incorrect");
      } catch (InvalidQueryException e) {
        assertThat(e.getMessage())
            .contains("Predicate 'insideCartesian' is not supported on property");
      }
    } else {
      try {
        GraphTraversal<Vertex, String> traversal =
            graphTraversalSource()
                .V()
                .has(
                    "user",
                    "pointPropWithGeoBounds_search",
                    Geo.inside(Point.fromCoordinates(30, 30), 14.142135623730951))
                .values("full_name");
        traversal.toList();
        fail("Should have failed executing the traversal because the property type is incorrect");
      } catch (InvalidQueryException e) {
        assertThat(e.getMessage())
            .contains("Distance units are required for queries against geodetic points.");
      }
    }
  }
}
