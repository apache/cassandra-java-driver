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
import com.esri.core.geometry.ogc.OGCLineString;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;

public class DefaultLineStringTest {
  private final LineString lineString =
      LineString.fromPoints(
          Point.fromCoordinates(30, 10),
          Point.fromCoordinates(10, 30),
          Point.fromCoordinates(40, 40));

  private final String wkt = "LINESTRING (30 10, 10 30, 40 40)";

  private final String json =
      "{\"type\":\"LineString\",\"coordinates\":[[30.0,10.0],[10.0,30.0],[40.0,40.0]]}";

  @Test
  public void should_parse_valid_well_known_text() {
    assertThat(LineString.fromWellKnownText(wkt)).isEqualTo(lineString);
  }

  @Test
  public void should_fail_to_parse_invalid_well_known_text() {
    assertInvalidWkt("linestring()");
    assertInvalidWkt("linestring(30 10 20, 10 30 20)"); // 3d
    assertInvalidWkt("linestring(0 0, 1 1, 0 1, 1 0)"); // crossing itself
    assertInvalidWkt("superlinestring(30 10, 10 30, 40 40)");
  }

  @Test
  public void should_convert_to_well_known_text() {
    assertThat(lineString.toString()).isEqualTo(wkt);
  }

  @Test
  public void should_convert_to_well_known_binary() {
    ByteBuffer actual = lineString.asWellKnownBinary();

    ByteBuffer expected = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
    expected.position(0);
    expected.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
    expected.putInt(2); // type
    expected.putInt(3); // num lineStrings
    expected.putDouble(30); // x1
    expected.putDouble(10); // y1
    expected.putDouble(10); // x2
    expected.putDouble(30); // y2
    expected.putDouble(40); // x3
    expected.putDouble(40); // y3
    expected.flip();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_load_from_well_known_binary() {
    ByteBuffer bb = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
    bb.position(0);
    bb.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
    bb.putInt(2); // type
    bb.putInt(3); // num lineStrings
    bb.putDouble(30); // x1
    bb.putDouble(10); // y1
    bb.putDouble(10); // x2
    bb.putDouble(30); // y2
    bb.putDouble(40); // x3
    bb.putDouble(40); // y3
    bb.flip();

    assertThat(LineString.fromWellKnownBinary(bb)).isEqualTo(lineString);
  }

  @Test
  public void should_parse_valid_geo_json() {
    assertThat(LineString.fromGeoJson(json)).isEqualTo(lineString);
  }

  @Test
  public void should_convert_to_geo_json() {
    assertThat(lineString.asGeoJson()).isEqualTo(json);
  }

  @Test
  public void should_convert_to_ogc_line_string() {
    assertThat(((DefaultLineString) lineString).getOgcGeometry()).isInstanceOf(OGCLineString.class);
  }

  @Test
  public void should_produce_same_hashCode_for_equal_objects() {
    LineString line1 =
        LineString.fromPoints(
            Point.fromCoordinates(30, 10),
            Point.fromCoordinates(10, 30),
            Point.fromCoordinates(40, 40));
    LineString line2 = LineString.fromWellKnownText(wkt);
    assertThat(line1).isEqualTo(line2);
    assertThat(line1.hashCode()).isEqualTo(line2.hashCode());
  }

  @Test
  public void should_expose_points() {
    assertThat(lineString.getPoints())
        .containsOnly(
            Point.fromCoordinates(30, 10),
            Point.fromCoordinates(10, 30),
            Point.fromCoordinates(40, 40));
    assertThat(LineString.fromWellKnownText(wkt).getPoints())
        .containsOnly(
            Point.fromCoordinates(30, 10),
            Point.fromCoordinates(10, 30),
            Point.fromCoordinates(40, 40));
  }

  @Test
  public void should_encode_and_decode() throws Exception {
    assertThat(SerializationUtils.serializeAndDeserialize(lineString)).isEqualTo(lineString);
  }

  @Test
  public void should_contain_self() {
    assertThat(lineString.contains(lineString)).isTrue();
  }

  @Test
  public void should_contain_all_intersected_points_except_start_and_end() {
    LineString s =
        LineString.fromPoints(
            Point.fromCoordinates(0, 0),
            Point.fromCoordinates(0, 30),
            Point.fromCoordinates(30, 30));
    assertThat(s.contains(Point.fromCoordinates(0, 0))).isFalse();
    assertThat(s.contains(Point.fromCoordinates(0, 15))).isTrue();
    assertThat(s.contains(Point.fromCoordinates(0, 30))).isTrue();
    assertThat(s.contains(Point.fromCoordinates(15, 30))).isTrue();
    assertThat(s.contains(Point.fromCoordinates(30, 30))).isFalse();
  }

  @Test
  public void should_contain_substring() {
    assertThat(
            lineString.contains(
                LineString.fromPoints(
                    Point.fromCoordinates(30, 10), Point.fromCoordinates(10, 30))))
        .isTrue();
  }

  @Test
  public void should_not_contain_unrelated_string() {
    assertThat(
            lineString.contains(
                LineString.fromPoints(
                    Point.fromCoordinates(10, 10), Point.fromCoordinates(30, 30))))
        .isFalse();
  }

  @Test
  public void should_not_contain_polygon() {
    LineString s =
        LineString.fromPoints(
            Point.fromCoordinates(0, 0),
            Point.fromCoordinates(0, 30),
            Point.fromCoordinates(30, 30),
            Point.fromCoordinates(30, 0));
    LineString p =
        LineString.fromPoints(
            Point.fromCoordinates(10, 10),
            Point.fromCoordinates(10, 20),
            Point.fromCoordinates(20, 20),
            Point.fromCoordinates(20, 10));
    assertThat(s.contains(p)).isFalse();
  }

  @Test
  public void should_accept_empty_shape() throws Exception {
    DefaultLineString s = ((DefaultLineString) LineString.fromWellKnownText("LINESTRING EMPTY"));
    assertThat(s.getOgcGeometry().isEmpty()).isTrue();
  }

  private void assertInvalidWkt(String s) {
    try {
      LineString.fromWellKnownText(s);
      fail("Should have thrown InvalidTypeException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
