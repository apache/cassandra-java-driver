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
import com.datastax.dse.driver.internal.core.data.geometry.DefaultPolygon;
import com.esri.core.geometry.ogc.OGCPolygon;
import java.nio.ByteBuffer;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * The driver-side representation of DSE's {@code Polygon}.
 *
 * <p>This is a planar surface in a two-dimensional XY-plane, represented by one exterior boundary
 * and 0 or more interior boundaries.
 *
 * <p>The default implementation returned by the driver is immutable.
 */
public interface Polygon extends Geometry {
  /**
   * Creates a polygon from its <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known
   * Text</a> (WKT) representation.
   *
   * @param source the Well-known Text representation to parse.
   * @return the polygon represented by the WKT.
   * @throws IllegalArgumentException if the string does not contain a valid Well-known Text
   *     representation.
   */
  @Nonnull
  static Polygon fromWellKnownText(@Nonnull String source) {
    return new DefaultPolygon(DefaultGeometry.fromOgcWellKnownText(source, OGCPolygon.class));
  }

  /**
   * Creates a polygon from its <a
   * href="https://en.wikipedia.org/wiki/Well-known_text#Well-known_binary">Well-known Binary</a>
   * (WKB) representation.
   *
   * @param source the Well-known Binary representation to parse.
   * @return the polygon represented by the WKB.
   * @throws IllegalArgumentException if the provided {@link ByteBuffer} does not contain a valid
   *     Well-known Binary representation.
   */
  @Nonnull
  static Polygon fromWellKnownBinary(@Nonnull ByteBuffer source) {
    return new DefaultPolygon(DefaultGeometry.fromOgcWellKnownBinary(source, OGCPolygon.class));
  }

  /**
   * Creates a polygon from a <a href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON
   * Polygon</a> representation.
   *
   * @param source the <a href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON Polygon</a>
   *     representation to parse.
   * @return the polygon represented by the <a
   *     href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON Polygon</a>.
   * @throws IllegalArgumentException if the string does not contain a valid <a
   *     href="https://tools.ietf.org/html/rfc7946#appendix-A">GeoJSON Polygon</a> representation.
   */
  @Nonnull
  static Polygon fromGeoJson(@Nonnull String source) {
    return new DefaultPolygon(DefaultGeometry.fromOgcGeoJson(source, OGCPolygon.class));
  }

  /** Creates a polygon from a series of 3 or more points. */
  @Nonnull
  static Polygon fromPoints(
      @Nonnull Point p1, @Nonnull Point p2, @Nonnull Point p3, @Nonnull Point... pn) {
    return new DefaultPolygon(p1, p2, p3, pn);
  }

  /**
   * Returns a polygon builder.
   *
   * <p>This is intended for complex polygons with multiple rings (i.e. holes inside the polygon).
   * For simple cases, consider {@link #fromPoints(Point, Point, Point, Point...)} instead.
   */
  @Nonnull
  static Builder builder() {
    return new DefaultPolygon.Builder();
  }

  /** Returns the external ring of the polygon. */
  @Nonnull
  List<Point> getExteriorRing();

  /**
   * Returns the internal rings of the polygon, i.e. any holes inside of it (or islands inside of
   * the holes).
   */
  @Nonnull
  List<List<Point>> getInteriorRings();

  /** Provides a simple DSL to build a polygon. */
  interface Builder {
    /**
     * Adds a new ring for this polygon.
     *
     * <p>There can be one or more outer rings and zero or more inner rings. If a polygon has an
     * inner ring, the inner ring looks like a hole. If the hole contains another outer ring, that
     * outer ring looks like an island.
     *
     * <p>There must be one "main" outer ring that contains all the others.
     */
    @Nonnull
    Builder addRing(@Nonnull Point p1, @Nonnull Point p2, @Nonnull Point p3, @Nonnull Point... pn);

    @Nonnull
    Polygon build();
  }
}
