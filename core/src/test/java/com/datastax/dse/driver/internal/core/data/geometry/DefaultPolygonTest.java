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
import static org.assertj.core.api.Fail.fail;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.esri.core.geometry.ogc.OGCPolygon;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;

public class DefaultPolygonTest {

  private Polygon polygon =
      Polygon.fromPoints(
          Point.fromCoordinates(30, 10),
          Point.fromCoordinates(10, 20),
          Point.fromCoordinates(20, 40),
          Point.fromCoordinates(40, 40));

  private String wkt = "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))";

  private String json =
      "{\"type\":\"Polygon\",\"coordinates\":[[[30.0,10.0],[10.0,20.0],[20.0,40.0],[40.0,40.0],[30.0,10.0]]]}";

  @Test
  public void should_parse_valid_well_known_text() {
    assertThat(Polygon.fromWellKnownText(wkt)).isEqualTo(polygon);
  }

  @Test
  public void should_fail_to_parse_invalid_well_known_text() {
    assertInvalidWkt("polygon(())"); // malformed
    assertInvalidWkt("polygon((30 10 1, 40 40 1, 20 40 1, 10 20 1, 30 10 1))"); // 3d
    assertInvalidWkt("polygon((0 0, 1 1, 0 1, 1 0, 0 0))"); // crosses itself
    assertInvalidWkt("polygon123((30 10, 40 40, 20 40, 10 20, 30 10))"); // malformed
  }

  @Test
  public void should_convert_to_well_known_binary() {
    ByteBuffer actual = polygon.asWellKnownBinary();

    ByteBuffer expected = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
    expected.position(0);
    expected.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
    expected.putInt(3); // type
    expected.putInt(1); // num rings
    expected.putInt(5); // num polygons (ring 1/1)
    expected.putDouble(30); // x1
    expected.putDouble(10); // y1
    expected.putDouble(40); // x2
    expected.putDouble(40); // y2
    expected.putDouble(20); // x3
    expected.putDouble(40); // y3
    expected.putDouble(10); // x4
    expected.putDouble(20); // y4
    expected.putDouble(30); // x5
    expected.putDouble(10); // y5
    expected.flip();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_load_from_well_known_binary() {
    ByteBuffer bb = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
    bb.position(0);
    bb.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
    bb.putInt(3); // type
    bb.putInt(1); // num rings
    bb.putInt(5); // num polygons (ring 1/1)
    bb.putDouble(30); // x1
    bb.putDouble(10); // y1
    bb.putDouble(40); // x2
    bb.putDouble(40); // y2
    bb.putDouble(20); // x3
    bb.putDouble(40); // y3
    bb.putDouble(10); // x4
    bb.putDouble(20); // y4
    bb.putDouble(30); // x5
    bb.putDouble(10); // y5
    bb.flip();

    assertThat(Polygon.fromWellKnownBinary(bb)).isEqualTo(polygon);
  }

  @Test
  public void should_parse_valid_geo_json() {
    assertThat(Polygon.fromGeoJson(json)).isEqualTo(polygon);
  }

  @Test
  public void should_convert_to_geo_json() {
    assertThat(polygon.asGeoJson()).isEqualTo(json);
  }

  @Test
  public void should_convert_to_ogc_polygon() {
    assertThat(((DefaultPolygon) polygon).getOgcGeometry()).isInstanceOf(OGCPolygon.class);
  }

  @Test
  public void should_produce_same_hashCode_for_equal_objects() {
    Polygon polygon1 =
        Polygon.fromPoints(
            Point.fromCoordinates(30, 10),
            Point.fromCoordinates(10, 20),
            Point.fromCoordinates(20, 40),
            Point.fromCoordinates(40, 40));
    Polygon polygon2 = Polygon.fromWellKnownText(wkt);
    assertThat(polygon1).isEqualTo(polygon2);
    assertThat(polygon1.hashCode()).isEqualTo(polygon2.hashCode());
  }

  @Test
  public void should_build_with_constructor_without_checking_orientation() {
    // By default, OGC requires outer rings to be clockwise and inner rings to be counterclockwise.
    // We disable that in our constructors.
    // This polygon has a single outer ring that is counterclockwise.
    Polygon polygon =
        Polygon.fromPoints(
            Point.fromCoordinates(5, 0),
            Point.fromCoordinates(5, 3),
            Point.fromCoordinates(0, 3),
            Point.fromCoordinates(0, 0));
    assertThat(polygon.asWellKnownText()).isEqualTo("POLYGON ((0 0, 5 0, 5 3, 0 3, 0 0))");
  }

  @Test
  public void should_build_complex_polygon_with_builder() {
    Polygon polygon =
        Polygon.builder()
            .addRing(
                Point.fromCoordinates(0, 0),
                Point.fromCoordinates(0, 3),
                Point.fromCoordinates(5, 3),
                Point.fromCoordinates(5, 0))
            .addRing(
                Point.fromCoordinates(1, 1),
                Point.fromCoordinates(1, 2),
                Point.fromCoordinates(2, 2),
                Point.fromCoordinates(2, 1))
            .addRing(
                Point.fromCoordinates(3, 1),
                Point.fromCoordinates(3, 2),
                Point.fromCoordinates(4, 2),
                Point.fromCoordinates(4, 1))
            .build();
    assertThat(polygon.asWellKnownText())
        .isEqualTo(
            "POLYGON ((0 0, 5 0, 5 3, 0 3, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (3 1, 3 2, 4 2, 4 1, 3 1))");
  }

  @Test
  public void should_expose_rings() {
    assertThat(polygon.getExteriorRing())
        .containsOnly(
            Point.fromCoordinates(30, 10),
            Point.fromCoordinates(10, 20),
            Point.fromCoordinates(20, 40),
            Point.fromCoordinates(40, 40));
    assertThat(polygon.getInteriorRings().isEmpty()).isTrue();

    Polygon fromWkt = Polygon.fromWellKnownText(wkt);
    assertThat(fromWkt.getExteriorRing())
        .containsOnly(
            Point.fromCoordinates(30, 10),
            Point.fromCoordinates(10, 20),
            Point.fromCoordinates(20, 40),
            Point.fromCoordinates(40, 40));
    assertThat(fromWkt.getInteriorRings().isEmpty()).isTrue();

    Polygon complex =
        Polygon.builder()
            .addRing(
                Point.fromCoordinates(0, 0),
                Point.fromCoordinates(0, 3),
                Point.fromCoordinates(5, 3),
                Point.fromCoordinates(5, 0))
            .addRing(
                Point.fromCoordinates(1, 1),
                Point.fromCoordinates(1, 2),
                Point.fromCoordinates(2, 2),
                Point.fromCoordinates(2, 1))
            .addRing(
                Point.fromCoordinates(3, 1),
                Point.fromCoordinates(3, 2),
                Point.fromCoordinates(4, 2),
                Point.fromCoordinates(4, 1))
            .build();
    assertThat(complex.getExteriorRing())
        .containsOnly(
            Point.fromCoordinates(0, 0),
            Point.fromCoordinates(0, 3),
            Point.fromCoordinates(5, 3),
            Point.fromCoordinates(5, 0));
    assertThat(complex.getInteriorRings()).hasSize(2);
    assertThat(complex.getInteriorRings().get(0))
        .containsOnly(
            Point.fromCoordinates(1, 1),
            Point.fromCoordinates(1, 2),
            Point.fromCoordinates(2, 2),
            Point.fromCoordinates(2, 1));
    assertThat(complex.getInteriorRings().get(1))
        .containsOnly(
            Point.fromCoordinates(3, 1),
            Point.fromCoordinates(3, 2),
            Point.fromCoordinates(4, 2),
            Point.fromCoordinates(4, 1));

    Polygon complexFromWkt =
        Polygon.fromWellKnownText(
            "POLYGON ((0 0, 5 0, 5 3, 0 3, 0 0), (1 1, 1 2, 2 2, 2 1, 1 1), (3 1, 3 2, 4 2, 4 1, 3 1))");
    assertThat(complexFromWkt.getExteriorRing())
        .containsOnly(
            Point.fromCoordinates(0, 0),
            Point.fromCoordinates(0, 3),
            Point.fromCoordinates(5, 3),
            Point.fromCoordinates(5, 0));
    assertThat(complexFromWkt.getInteriorRings()).hasSize(2);
    assertThat(complexFromWkt.getInteriorRings().get(0))
        .containsOnly(
            Point.fromCoordinates(1, 1),
            Point.fromCoordinates(1, 2),
            Point.fromCoordinates(2, 2),
            Point.fromCoordinates(2, 1));
    assertThat(complexFromWkt.getInteriorRings().get(1))
        .containsOnly(
            Point.fromCoordinates(3, 1),
            Point.fromCoordinates(3, 2),
            Point.fromCoordinates(4, 2),
            Point.fromCoordinates(4, 1));
  }

  @Test
  public void should_encode_and_decode() throws Exception {
    assertThat(SerializationUtils.serializeAndDeserialize(polygon)).isEqualTo(polygon);
  }

  @Test
  public void should_contain_self() {
    assertThat(polygon.contains(polygon)).isTrue();
  }

  @Test
  public void should_not_contain_point_or_linestring_on_exterior_ring() {
    assertThat(polygon.contains(Point.fromCoordinates(30, 10))).isFalse();
    assertThat(polygon.contains(Point.fromCoordinates(30, 40))).isFalse();
    assertThat(
            polygon.contains(
                LineString.fromPoints(
                    Point.fromCoordinates(35, 40), Point.fromCoordinates(25, 40))))
        .isFalse();
  }

  @Test
  public void should_contain_interior_shape() {
    assertThat(polygon.contains(Point.fromCoordinates(20, 20))).isTrue();
    assertThat(
            polygon.contains(
                LineString.fromPoints(
                    Point.fromCoordinates(20, 20), Point.fromCoordinates(30, 20))))
        .isTrue();
    assertThat(
            polygon.contains(
                Polygon.fromPoints(
                    Point.fromCoordinates(20, 20),
                    Point.fromCoordinates(30, 20),
                    Point.fromCoordinates(20, 30))))
        .isTrue();
  }

  @Test
  public void should_not_contain_exterior_shape() {
    assertThat(polygon.contains(Point.fromCoordinates(10, 10))).isFalse();
    assertThat(
            polygon.contains(
                LineString.fromPoints(
                    Point.fromCoordinates(10, 10), Point.fromCoordinates(20, 20))))
        .isFalse();
    assertThat(
            polygon.contains(
                Polygon.fromPoints(
                    Point.fromCoordinates(0, 0),
                    Point.fromCoordinates(0, 10),
                    Point.fromCoordinates(10, 10))))
        .isFalse();
  }

  @Test
  public void should_not_contain_shapes_in_interior_hole() {
    Polygon complex =
        Polygon.builder()
            .addRing(
                Point.fromCoordinates(0, 0),
                Point.fromCoordinates(30, 0),
                Point.fromCoordinates(30, 30),
                Point.fromCoordinates(0, 30))
            .addRing(
                Point.fromCoordinates(10, 10),
                Point.fromCoordinates(20, 10),
                Point.fromCoordinates(20, 20),
                Point.fromCoordinates(10, 20))
            .build();
    assertThat(complex.contains(Point.fromCoordinates(15, 15))).isFalse();
  }

  @Test
  public void should_accept_empty_shape() throws Exception {
    Polygon polygon = Polygon.fromWellKnownText("POLYGON EMPTY");
    assertThat(polygon.getExteriorRing()).isEmpty();
    assertThat(((DefaultPolygon) polygon).getOgcGeometry().isEmpty()).isTrue();
  }

  private void assertInvalidWkt(String s) {
    try {
      Polygon.fromWellKnownText(s);
      fail("Should have thrown InvalidTypeException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
