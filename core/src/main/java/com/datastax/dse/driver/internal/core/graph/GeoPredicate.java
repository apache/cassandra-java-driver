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
package com.datastax.dse.driver.internal.core.graph;

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.dse.driver.api.core.graph.predicates.Geo;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;

/**
 * List of predicates for geolocation usage with DseGraph and Search indexes. Should not be accessed
 * directly but through the {@link Geo} static methods.
 */
public enum GeoPredicate implements DsePredicate {

  /** Matches values within the distance specified by the condition over a Haversine geometry. */
  inside {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      if (value == null) {
        return false;
      }
      Preconditions.checkArgument(value instanceof Geometry);
      Distance distance = (Distance) condition;
      if (value instanceof Point) {
        return haversineDistanceInDegrees(distance.getCenter(), (Point) value)
            <= distance.getRadius();
      } else if (value instanceof Polygon) {
        for (Point point : ((Polygon) value).getExteriorRing()) {
          if (haversineDistanceInDegrees(distance.getCenter(), point) > distance.getRadius()) {
            return false;
          }
        }
      } else if (value instanceof LineString) {
        for (Point point : ((LineString) value).getPoints()) {
          if (haversineDistanceInDegrees(distance.getCenter(), point) > distance.getRadius()) {
            return false;
          }
        }
      } else {
        throw new UnsupportedOperationException(
            String.format("Value type '%s' unsupported", value.getClass().getName()));
      }

      return true;
    }

    @Override
    public String toString() {
      return "inside";
    }
  },

  /**
   * Matches values contained in the geometric entity specified by the condition on a 2D Euclidean
   * plane.
   */
  insideCartesian {
    @Override
    public boolean test(Object value, Object condition) {
      preEvaluate(condition);
      if (value == null) {
        return false;
      }
      Preconditions.checkArgument(value instanceof Geometry);
      return ((Geometry) condition).contains((Geometry) value);
    }

    @Override
    public String toString() {
      return "insideCartesian";
    }
  };

  @Override
  public boolean isValidCondition(Object condition) {
    return condition != null;
  }

  static double haversineDistanceInDegrees(Point p1, Point p2) {
    double dLat = Math.toRadians(p2.Y() - p1.Y());
    double dLon = Math.toRadians(p2.X() - p1.X());
    double lat1 = Math.toRadians(p1.Y());
    double lat2 = Math.toRadians(p2.Y());

    double a =
        Math.pow(Math.sin(dLat / 2), 2)
            + Math.pow(Math.sin(dLon / 2), 2) * Math.cos(lat1) * Math.cos(lat2);
    double c = 2 * Math.asin(Math.sqrt(a));
    return Math.toDegrees(c);
  }
}
