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
package com.datastax.dse.driver.internal.core.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import org.junit.Test;

public class DistanceTest {

  private final Point point = Point.fromCoordinates(1.1, 2.2);
  private final Distance distance = new Distance(point, 7.0);
  private final String wkt = "DISTANCE((1.1 2.2) 7.0)";

  @Test
  public void should_parse_valid_well_known_text() {
    Distance fromWkt = Distance.fromWellKnownText(wkt);
    assertThat(fromWkt.getRadius()).isEqualTo(7.0);
    assertThat(fromWkt.getCenter()).isEqualTo(point);
    assertThat(Distance.fromWellKnownText(wkt)).isEqualTo(distance);
    // whitespace doesn't matter between distance and spec.
    assertThat(Distance.fromWellKnownText("DISTANCE ((1.1 2.2) 7.0)")).isEqualTo(distance);
    // case doesn't matter.
    assertThat(Distance.fromWellKnownText("distance((1.1 2.2) 7.0)")).isEqualTo(distance);
  }

  @Test(expected = IllegalArgumentException.class)
  public void should_fail_to_parse_invalid_well_known_text() {
    Distance.fromWellKnownText("dist((1.1 2.2) 3.3)");
  }

  @Test
  public void should_convert_to_well_known_text() {
    assertThat(distance.asWellKnownText()).isEqualTo(wkt);
  }

  @Test
  public void should_contain_point() {
    assertThat(distance.contains(Point.fromCoordinates(2.0, 3.0))).isTrue();
  }

  @Test
  public void should_not_contain_point() {
    // y axis falls outside of distance
    assertThat(distance.contains(Point.fromCoordinates(2.0, 9.3))).isFalse();
  }

  @Test
  public void should_contain_linestring() {
    assertThat(
            distance.contains(
                LineString.fromPoints(
                    Point.fromCoordinates(2.0, 3.0),
                    Point.fromCoordinates(3.1, 6.2),
                    Point.fromCoordinates(-1.0, -2.0))))
        .isTrue();
  }

  @Test
  public void should_not_contain_linestring() {
    // second point falls outside of distance at y axis.
    assertThat(
            distance.contains(
                LineString.fromPoints(
                    Point.fromCoordinates(2.0, 3.0),
                    Point.fromCoordinates(3.1, 9.2),
                    Point.fromCoordinates(-1.0, -2.0))))
        .isFalse();
  }

  @Test
  public void should_contain_polygon() {
    Polygon polygon =
        Polygon.fromPoints(
            Point.fromCoordinates(3, 1),
            Point.fromCoordinates(1, 2),
            Point.fromCoordinates(2, 4),
            Point.fromCoordinates(4, 4));
    assertThat(distance.contains(polygon)).isTrue();
  }

  @Test
  public void should_not_contain_polygon() {
    Polygon polygon =
        Polygon.fromPoints(
            Point.fromCoordinates(3, 1),
            Point.fromCoordinates(1, 2),
            Point.fromCoordinates(2, 4),
            Point.fromCoordinates(10, 4));
    // final point falls outside of distance at x axis.
    assertThat(distance.contains(polygon)).isFalse();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void should_fail_to_convert_to_ogc() {
    distance.getOgcGeometry();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void should_fail_to_convert_to_wkb() {
    distance.asWellKnownBinary();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void should_fail_to_convert_to_geo_json() {
    distance.asGeoJson();
  }

  @Test
  public void should_serialize_and_deserialize() throws Exception {
    assertThat(SerializationUtils.serializeAndDeserialize(distance)).isEqualTo(distance);
  }
}
