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

import static java.util.regex.Pattern.CASE_INSENSITIVE;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.esri.core.geometry.MultiPath;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import net.jcip.annotations.Immutable;

/**
 * The driver-side representation of DSE's {@code Geo.distance}.
 *
 * <p>This is a circle in a two-dimensional XY plane represented by its center point and radius. It
 * is used as a search criteria to determine whether or not another geospatial object lies within a
 * circular area.
 *
 * <p>Note that this shape has no equivalent in the OGC and GeoJSON standards: as a consequence,
 * {@link #asWellKnownText()} returns a custom format, and {@link #getOgcGeometry()}, {@link
 * #asWellKnownBinary()}, and {@link #asGeoJson()} throw {@link UnsupportedOperationException}.
 *
 * <p>Unlike other geo types, this class is never exposed directly to driver clients: it is used
 * internally by {@linkplain Geo#inside(Point, double) geo predicates}, but cannot be a column type,
 * nor appear in CQL or graph results. Therefore it doesn't have a public-facing interface, nor a
 * built-in codec.
 */
@Immutable
public class Distance extends DefaultGeometry {

  private static final Pattern WKT_PATTERN =
      Pattern.compile(
          "distance *\\( *\\( *([\\d\\.-]+) *([\\d+\\.-]+) *\\) *([\\d+\\.-]+) *\\)",
          CASE_INSENSITIVE);

  /**
   * Creates a distance from its <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known
   * Text</a> (WKT) representation.
   *
   * @param source the Well-known Text representation to parse.
   * @return the point represented by the WKT.
   * @throws IllegalArgumentException if the string does not contain a valid Well-known Text
   *     representation.
   * @see Distance#asWellKnownText()
   */
  @NonNull
  public static Distance fromWellKnownText(@NonNull String source) {
    Matcher matcher = WKT_PATTERN.matcher(source.trim());
    if (matcher.matches() && matcher.groupCount() == 3) {
      try {
        return new Distance(
            new DefaultPoint(
                Double.parseDouble(matcher.group(1)), Double.parseDouble(matcher.group(2))),
            Double.parseDouble(matcher.group(3)));
      } catch (NumberFormatException var3) {
        throw new IllegalArgumentException(String.format("Unable to parse %s", source));
      }
    } else {
      throw new IllegalArgumentException(String.format("Unable to parse %s", source));
    }
  }

  private final DefaultPoint center;

  private final double radius;

  /**
   * Creates a new distance with the given center and radius.
   *
   * @param center The center point.
   * @param radius The radius of the circle representing distance.
   */
  public Distance(@NonNull Point center, double radius) {
    super(((DefaultPoint) center).getOgcGeometry());
    Preconditions.checkNotNull(center);
    Preconditions.checkArgument(radius >= 0.0D, "Radius must be >= 0 (got %s)", radius);
    this.center = ((DefaultPoint) center);
    this.radius = radius;
  }

  /** @return The center point of the circle representing this distance. */
  @NonNull
  public Point getCenter() {
    return center;
  }

  /** @return The radius of the circle representing this distance. */
  public double getRadius() {
    return radius;
  }

  /**
   * Returns a <a href="https://en.wikipedia.org/wiki/Well-known_text">Well-known Text</a> (WKT)
   * representation of this geospatial type.
   *
   * <p>Since there is no Well-known Text specification for Distance, this returns a custom format
   * of: <code>DISTANCE((center.x center.y) radius)</code>
   *
   * @return a Well-known Text representation of this object.
   */
  @NonNull
  @Override
  public String asWellKnownText() {
    return String.format("DISTANCE((%s %s) %s)", this.center.X(), this.center.Y(), this.radius);
  }

  /**
   * The distance type has no equivalent in the OGC standard: this method throws an {@link
   * UnsupportedOperationException}.
   */
  @NonNull
  @Override
  public OGCGeometry getOgcGeometry() {
    throw new UnsupportedOperationException();
  }

  /**
   * The distance type has no equivalent in the OGC standard: this method throws an {@link
   * UnsupportedOperationException}.
   */
  @NonNull
  @Override
  public ByteBuffer asWellKnownBinary() {
    throw new UnsupportedOperationException();
  }

  /**
   * The distance type has no equivalent in the GeoJSON standard: this method throws an {@link
   * UnsupportedOperationException}.
   */
  @Override
  @NonNull
  public String asGeoJson() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof Distance) {
      Distance that = (Distance) other;
      return Objects.equals(this.center, that.center) && this.radius == that.radius;
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(center, radius);
  }

  @SuppressWarnings("SimplifiableConditionalExpression")
  @Override
  public boolean contains(@NonNull Geometry geometry) {
    return geometry instanceof Distance
        ? this.containsDistance((Distance) geometry)
        : geometry instanceof Point
            ? this.containsPoint((Point) geometry)
            : geometry instanceof LineString
                ? this.containsLineString((LineString) geometry)
                : geometry instanceof Polygon ? this.containsPolygon((Polygon) geometry) : false;
  }

  private boolean containsDistance(Distance distance) {
    return this.center.getOgcGeometry().distance(distance.center.getOgcGeometry()) + distance.radius
        <= this.radius;
  }

  private boolean containsPoint(Point point) {
    return this.containsOGCPoint(((DefaultPoint) point).getOgcGeometry());
  }

  private boolean containsLineString(LineString lineString) {
    MultiPath multiPath =
        (MultiPath) ((DefaultLineString) lineString).getOgcGeometry().getEsriGeometry();
    return containsMultiPath(multiPath);
  }

  private boolean containsPolygon(Polygon polygon) {
    MultiPath multiPath =
        (com.esri.core.geometry.Polygon)
            ((DefaultPolygon) polygon).getOgcGeometry().getEsriGeometry();
    return containsMultiPath(multiPath);
  }

  private boolean containsMultiPath(MultiPath multiPath) {
    int numPoints = multiPath.getPointCount();
    for (int i = 0; i < numPoints; ++i) {
      OGCPoint point = new OGCPoint(multiPath.getPoint(i), DefaultGeometry.SPATIAL_REFERENCE_4326);
      if (!this.containsOGCPoint(point)) {
        return false;
      }
    }
    return true;
  }

  private boolean containsOGCPoint(OGCPoint point) {
    return this.center.getOgcGeometry().distance(point) <= this.radius;
  }

  /**
   * This object gets replaced by an internal proxy for serialization.
   *
   * @serialData Point (wkb) for center followed by double for radius
   */
  private Object writeReplace() {
    return new DistanceSerializationProxy(this);
  }
}
