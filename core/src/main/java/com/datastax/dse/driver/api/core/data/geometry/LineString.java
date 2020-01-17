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
package com.datastax.dse.driver.api.core.data.geometry;

import com.datastax.dse.driver.internal.core.data.geometry.DefaultGeometry;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultLineString;
import com.esri.core.geometry.ogc.OGCLineString;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The driver-side representation for DSE's {@code LineString}.
 *
 * <p>This is a curve in a two-dimensional XY-plane, represented by a set of points (with linear
 * interpolation between them).
 *
 * <p>The default implementation returned by the driver is immutable.
 */
public interface LineString extends Geometry {
  /**
   * Creates a line string from its <a
   * href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text</a> (WKT) representation.
   *
   * @param source the Well-known Text representation to parse.
   * @return the line string represented by the WKT.
   * @throws IllegalArgumentException if the string does not contain a valid Well-known Text
   *     representation.
   */
  @NonNull
  static LineString fromWellKnownText(@NonNull String source) {
    return new DefaultLineString(DefaultGeometry.fromOgcWellKnownText(source, OGCLineString.class));
  }

  /**
   * Creates a line string from its <a
   * href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a>
   * (WKB) representation.
   *
   * @param source the Well-known Binary representation to parse.
   * @return the line string represented by the WKB.
   * @throws IllegalArgumentException if the provided {@link ByteBuffer} does not contain a valid
   *     Well-known Binary representation.
   */
  @NonNull
  static LineString fromWellKnownBinary(@NonNull ByteBuffer source) {
    return new DefaultLineString(
        DefaultGeometry.fromOgcWellKnownBinary(source, OGCLineString.class));
  }

  /**
   * Creates a line string from a <a href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON
   * LineString</a> representation.
   *
   * @param source the <a href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON
   *     LineString</a> representation to parse.
   * @return the line string represented by the <a
   *     href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON LineString</a>.
   * @throws IllegalArgumentException if the string does not contain a valid <a
   *     href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON LineString</a>
   *     representation.
   */
  @NonNull
  static LineString fromGeoJson(@NonNull String source) {
    return new DefaultLineString(DefaultGeometry.fromOgcGeoJson(source, OGCLineString.class));
  }

  /** Creates a line string from two or more points. */
  @NonNull
  static LineString fromPoints(@NonNull Point p1, @NonNull Point p2, @NonNull Point... pn) {
    return new DefaultLineString(p1, p2, pn);
  }

  @NonNull
  List<Point> getPoints();
}
