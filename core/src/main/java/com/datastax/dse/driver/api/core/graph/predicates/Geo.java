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
package com.datastax.dse.driver.api.core.graph.predicates;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import com.datastax.dse.driver.internal.core.graph.GeoPredicate;
import com.datastax.dse.driver.internal.core.graph.GeoUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

public interface Geo {

  enum Unit {
    MILES(GeoUtils.MILES_TO_KM * GeoUtils.KM_TO_DEG),
    KILOMETERS(GeoUtils.KM_TO_DEG),
    METERS(GeoUtils.KM_TO_DEG / 1000.0),
    DEGREES(1);

    private final double multiplier;

    Unit(double multiplier) {
      this.multiplier = multiplier;
    }

    /** Convert distance to degrees (used internally only). */
    public double toDegrees(double distance) {
      return distance * multiplier;
    }
  }

  /**
   * Finds whether an entity is inside the given circular area using a geo coordinate system.
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> inside(Point center, double radius, Unit units) {
    return new P<>(GeoPredicate.inside, new Distance(center, units.toDegrees(radius)));
  }

  /**
   * Finds whether an entity is inside the given circular area using a cartesian coordinate system.
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> inside(Point center, double radius) {
    return new P<>(GeoPredicate.insideCartesian, new Distance(center, radius));
  }

  /**
   * Finds whether an entity is inside the given polygon.
   *
   * @return a predicate to apply in a {@link GraphTraversal}.
   */
  static P<Object> inside(Polygon polygon) {
    return new P<>(GeoPredicate.insideCartesian, polygon);
  }

  /**
   * Creates a point from the given coordinates.
   *
   * <p>This is just a shortcut to {@link Point#fromCoordinates(double, double)}. It is duplicated
   * here so that {@code Geo} can be used as a single entry point in Gremlin-groovy scripts.
   */
  @NonNull
  static Point point(double x, double y) {
    return Point.fromCoordinates(x, y);
  }

  /**
   * Creates a line string from the given (at least 2) points.
   *
   * <p>This is just a shortcut to {@link LineString#fromPoints(Point, Point, Point...)}. It is
   * duplicated here so that {@code Geo} can be used as a single entry point in Gremlin-groovy
   * scripts.
   */
  @NonNull
  static LineString lineString(
      @NonNull Point point1, @NonNull Point point2, @NonNull Point... otherPoints) {
    return LineString.fromPoints(point1, point2, otherPoints);
  }

  /**
   * Creates a line string from the coordinates of its points.
   *
   * <p>This is provided for backward compatibility with previous DSE versions. We recommend {@link
   * #lineString(Point, Point, Point...)} instead.
   */
  @NonNull
  static LineString lineString(double... coordinates) {
    if (coordinates.length % 2 != 0) {
      throw new IllegalArgumentException("lineString() must be passed an even number of arguments");
    } else if (coordinates.length < 4) {
      throw new IllegalArgumentException(
          "lineString() must be passed at least 4 arguments (2 points)");
    }
    Point point1 = Point.fromCoordinates(coordinates[0], coordinates[1]);
    Point point2 = Point.fromCoordinates(coordinates[2], coordinates[3]);
    Point[] otherPoints = new Point[coordinates.length / 2 - 2];
    for (int i = 4; i < coordinates.length; i += 2) {
      otherPoints[i / 2 - 2] = Point.fromCoordinates(coordinates[i], coordinates[i + 1]);
    }
    return LineString.fromPoints(point1, point2, otherPoints);
  }

  /**
   * Creates a polygon from the given (at least 3) points.
   *
   * <p>This is just a shortcut to {@link Polygon#fromPoints(Point, Point, Point, Point...)}. It is
   * duplicated here so that {@code Geo} can be used as a single entry point in Gremlin-groovy
   * scripts.
   */
  @NonNull
  static Polygon polygon(
      @NonNull Point p1, @NonNull Point p2, @NonNull Point p3, @NonNull Point... otherPoints) {
    return Polygon.fromPoints(p1, p2, p3, otherPoints);
  }

  /**
   * Creates a polygon from the coordinates of its points.
   *
   * <p>This is provided for backward compatibility with previous DSE versions. We recommend {@link
   * #polygon(Point, Point, Point, Point...)} instead.
   */
  @NonNull
  static Polygon polygon(double... coordinates) {
    if (coordinates.length % 2 != 0) {
      throw new IllegalArgumentException("polygon() must be passed an even number of arguments");
    } else if (coordinates.length < 6) {
      throw new IllegalArgumentException(
          "polygon() must be passed at least 6 arguments (3 points)");
    }
    Point point1 = Point.fromCoordinates(coordinates[0], coordinates[1]);
    Point point2 = Point.fromCoordinates(coordinates[2], coordinates[3]);
    Point point3 = Point.fromCoordinates(coordinates[4], coordinates[5]);
    Point[] otherPoints = new Point[coordinates.length / 2 - 3];
    for (int i = 6; i < coordinates.length; i += 2) {
      otherPoints[i / 2 - 3] = Point.fromCoordinates(coordinates[i], coordinates[i + 1]);
    }
    return Polygon.fromPoints(point1, point2, point3, otherPoints);
  }
}
