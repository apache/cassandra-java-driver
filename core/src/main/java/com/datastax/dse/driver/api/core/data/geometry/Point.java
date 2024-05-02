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
package com.datastax.dse.driver.api.core.data.geometry;

import com.datastax.dse.driver.internal.core.data.geometry.DefaultGeometry;
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPoint;
import com.esri.core.geometry.ogc.OGCPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;

/**
 * The driver-side representation of DSE's {@code Point}.
 *
 * <p>This is a zero-dimensional object that represents a specific (X,Y) location in a
 * two-dimensional XY-plane. In case of Geographic Coordinate Systems, the X coordinate is the
 * longitude and the Y is the latitude.
 *
 * <p>The default implementation returned by the driver is immutable.
 */
public interface Point extends Geometry {

  /**
   * Creates a point from its <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known
   * Text</a> (WKT) representation.
   *
   * @param source the Well-known Text representation to parse.
   * @return the point represented by the WKT.
   * @throws IllegalArgumentException if the string does not contain a valid Well-known Text
   *     representation.
   */
  @NonNull
  static Point fromWellKnownText(@NonNull String source) {
    return new DefaultPoint(DefaultGeometry.fromOgcWellKnownText(source, OGCPoint.class));
  }

  /**
   * Creates a point from its <a
   * href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a>
   * (WKB) representation.
   *
   * @param source the Well-known Binary representation to parse.
   * @return the point represented by the WKB.
   * @throws IllegalArgumentException if the provided {@link ByteBuffer} does not contain a valid
   *     Well-known Binary representation.
   */
  @NonNull
  static Point fromWellKnownBinary(@NonNull ByteBuffer source) {
    return new DefaultPoint(DefaultGeometry.fromOgcWellKnownBinary(source, OGCPoint.class));
  }

  /**
   * Creates a point from a <a href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON
   * Point</a> representation.
   *
   * @param source the <a href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON Point</a>
   *     representation to parse.
   * @return the point represented by the <a
   *     href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON Point</a>.
   * @throws IllegalArgumentException if the string does not contain a valid <a
   *     href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON Point</a> representation.
   */
  @NonNull
  static Point fromGeoJson(@NonNull String source) {
    return new DefaultPoint(DefaultGeometry.fromOgcGeoJson(source, OGCPoint.class));
  }

  /**
   * Creates a new point.
   *
   * @param x The X coordinate of this point (or its longitude in Geographic Coordinate Systems).
   * @param y The Y coordinate of this point (or its latitude in Geographic Coordinate Systems).
   * @return the point represented by coordinates.
   */
  @NonNull
  static Point fromCoordinates(double x, double y) {
    return new DefaultPoint(x, y);
  }

  /**
   * Returns the X coordinate of this 2D point (or its longitude in Geographic Coordinate Systems).
   */
  double X();

  /**
   * Returns the Y coordinate of this 2D point (or its latitude in Geographic Coordinate Systems).
   */
  double Y();
}
