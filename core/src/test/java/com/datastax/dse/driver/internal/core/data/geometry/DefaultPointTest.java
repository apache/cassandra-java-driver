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
package com.datastax.dse.driver.internal.core.data.geometry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.esri.core.geometry.ogc.OGCPoint;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.junit.Test;

public class DefaultPointTest {

  private DefaultPoint point = new DefaultPoint(1.1, 2.2);

  private final String wkt = "POINT (1.1 2.2)";

  private final String json = "{\"type\":\"Point\",\"coordinates\":[1.1,2.2]}";

  @Test
  public void should_parse_valid_well_known_text() {
    assertThat(Point.fromWellKnownText(wkt)).isEqualTo(point);
  }

  @Test
  public void should_fail_to_parse_invalid_well_known_text() {
    assertInvalidWkt("superpoint(1.1 2.2 3.3)");
  }

  @Test
  public void should_convert_to_well_known_text() {
    assertThat(point.toString()).isEqualTo(wkt);
  }

  @Test
  public void should_convert_to_well_knowm_binary() {
    ByteBuffer actual = point.asWellKnownBinary();

    ByteBuffer expected = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
    expected.position(0);
    expected.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
    expected.putInt(1); // type
    expected.putDouble(1.1); // x
    expected.putDouble(2.2); // y
    expected.flip();

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void should_load_from_well_known_binary() {
    ByteBuffer bb = ByteBuffer.allocate(1024).order(ByteOrder.nativeOrder());
    bb.position(0);
    bb.put((byte) (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? 1 : 0)); // endianness
    bb.putInt(1); // type
    bb.putDouble(1.1); // x
    bb.putDouble(2.2); // y
    bb.flip();

    assertThat(Point.fromWellKnownBinary(bb)).isEqualTo(point);
  }

  @Test
  public void should_parse_valid_geo_json() {
    assertThat(Point.fromGeoJson(json)).isEqualTo(point);
  }

  @Test
  public void should_convert_to_geo_json() {
    assertThat(point.asGeoJson()).isEqualTo(json);
  }

  @Test
  public void should_convert_to_ogc_point() {
    assertThat(point.getOgcGeometry()).isInstanceOf(OGCPoint.class);
  }

  @Test
  public void should_produce_same_hashCode_for_equal_objects() {
    Point point1 = new DefaultPoint(10, 20);
    Point point2 = Point.fromWellKnownText("POINT (10 20)");
    assertThat(point1).isEqualTo(point2);
    assertThat(point1.hashCode()).isEqualTo(point2.hashCode());
  }

  @Test
  public void should_encode_and_decode() throws Exception {
    assertThat(SerializationUtils.serializeAndDeserialize(point)).isEqualTo(point);
  }

  @Test
  public void should_contain_self() {
    assertThat(point.contains(point)).isTrue();
  }

  @Test
  public void should_not_contain_any_other_shape_than_self() {
    DefaultPoint point2 = new DefaultPoint(1, 2);
    DefaultPoint point3 = new DefaultPoint(1, 3);
    assertThat(point.contains(point2)).isFalse();
    assertThat(point.contains(new DefaultLineString(point, point2))).isFalse();
    assertThat(point.contains(new DefaultPolygon(point, point2, point3))).isFalse();
  }

  @Test
  public void should_accept_empty_shape() throws Exception {
    DefaultPoint point = ((DefaultPoint) Point.fromWellKnownText("POINT EMPTY"));
    assertThat(point.getOgcGeometry().isEmpty()).isTrue();
  }

  private void assertInvalidWkt(String s) {
    try {
      Point.fromWellKnownText(s);
      fail("Should have thrown InvalidTypeException");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
