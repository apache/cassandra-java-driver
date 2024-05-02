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
package com.datastax.dse.driver.api.core.graph.predicates;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.junit.Test;

public class GeoTest {

  @Test
  public void should_convert_units_to_degrees() {
    assertThat(Geo.Unit.DEGREES.toDegrees(100.0)).isEqualTo(100.0);
    assertThat(Geo.Unit.MILES.toDegrees(68.9722)).isEqualTo(0.9982455747535043);
    assertThat(Geo.Unit.KILOMETERS.toDegrees(111.0)).isEqualTo(0.9982456082154465);
    assertThat(Geo.Unit.METERS.toDegrees(111000.0)).isEqualTo(0.9982456082154464);
  }

  @Test
  public void should_test_if_point_is_inside_circle_with_cartesian_coordinates() {
    P<Object> inside = Geo.inside(Point.fromCoordinates(30, 30), 14.142135623730951);
    assertThat(inside.test(Point.fromCoordinates(40, 40))).isTrue();
    assertThat(inside.test(Point.fromCoordinates(40.1, 40))).isFalse();
  }

  @Test
  public void should_test_if_point_is_inside_circle_with_geo_coordinates() {
    P<Object> inside =
        Geo.inside(Point.fromCoordinates(30, 30), 12.908258700131379, Geo.Unit.DEGREES);
    assertThat(inside.test(Point.fromCoordinates(40, 40))).isTrue();
    assertThat(inside.test(Point.fromCoordinates(40.1, 40))).isFalse();
  }

  @Test
  public void should_test_if_point_is_inside_polygon() {
    P<Object> inside =
        Geo.inside(
            Polygon.builder()
                .addRing(
                    Point.fromCoordinates(30, 30),
                    Point.fromCoordinates(40, 40),
                    Point.fromCoordinates(40, 30))
                .build());
    assertThat(inside.test(Point.fromCoordinates(35, 32))).isTrue();
    assertThat(inside.test(Point.fromCoordinates(33, 37))).isFalse();
  }

  @Test
  public void should_build_line_string_from_coordinates() {
    LineString lineString = Geo.lineString(1, 2, 3, 4, 5, 6);
    assertThat(lineString.getPoints())
        .hasSize(3)
        .contains(Point.fromCoordinates(1, 2))
        .contains(Point.fromCoordinates(3, 4))
        .contains(Point.fromCoordinates(5, 6));
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_build_line_string_if_not_enough_coordinates() {
    Geo.lineString(1, 2);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_build_line_string_if_uneven_number_of_coordinates() {
    Geo.lineString(1, 2, 3, 4, 5);
  }

  @Test
  public void should_build_polygon_from_coordinates() {
    Polygon polygon = Geo.polygon(1, 2, 3, 4, 5, 6, 7, 8);
    assertThat(polygon.getExteriorRing())
        .hasSize(4)
        .contains(Point.fromCoordinates(1, 2))
        .contains(Point.fromCoordinates(3, 4))
        .contains(Point.fromCoordinates(5, 6))
        .contains(Point.fromCoordinates(7, 8));
    assertThat(polygon.getInteriorRings()).isEmpty();
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_build_polygon_if_not_enough_coordinates() {
    Geo.polygon(1, 2, 3, 4);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_build_polygon_if_uneven_number_of_coordinates() {
    Geo.polygon(1, 2, 3, 4, 5, 6, 7);
  }
}
