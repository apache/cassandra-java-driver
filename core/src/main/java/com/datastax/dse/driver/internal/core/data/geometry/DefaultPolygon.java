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

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.esri.core.geometry.Operator;
import com.esri.core.geometry.OperatorFactoryLocal;
import com.esri.core.geometry.OperatorSimplifyOGC;
import com.esri.core.geometry.ogc.OGCPolygon;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.List;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultPolygon extends DefaultGeometry implements Polygon {

  private static final long serialVersionUID = 3694196802962890314L;

  private final List<Point> exteriorRing;
  private final List<List<Point>> interiorRings;

  public DefaultPolygon(
      @NonNull Point p1, @NonNull Point p2, @NonNull Point p3, @NonNull Point... pn) {
    super(fromPoints(p1, p2, p3, pn));
    this.exteriorRing = ImmutableList.<Point>builder().add(p1).add(p2).add(p3).add(pn).build();
    this.interiorRings = Collections.emptyList();
  }

  public DefaultPolygon(@NonNull OGCPolygon polygon) {
    super(polygon);
    if (polygon.isEmpty()) {
      this.exteriorRing = ImmutableList.of();
    } else {
      this.exteriorRing = getPoints(polygon.exteriorRing());
    }

    ImmutableList.Builder<List<Point>> builder = ImmutableList.builder();
    for (int i = 0; i < polygon.numInteriorRing(); i++) {
      builder.add(getPoints(polygon.interiorRingN(i)));
    }
    this.interiorRings = builder.build();
  }

  @NonNull
  @Override
  public List<Point> getExteriorRing() {
    return exteriorRing;
  }

  @NonNull
  @Override
  public List<List<Point>> getInteriorRings() {
    return interiorRings;
  }

  private static OGCPolygon fromPoints(Point p1, Point p2, Point p3, Point... pn) {
    com.esri.core.geometry.Polygon polygon = new com.esri.core.geometry.Polygon();
    addPath(polygon, p1, p2, p3, pn);
    return new OGCPolygon(simplify(polygon), DefaultGeometry.SPATIAL_REFERENCE_4326);
  }

  private static void addPath(
      com.esri.core.geometry.Polygon polygon, Point p1, Point p2, Point p3, Point[] pn) {

    polygon.startPath(toEsri(p1));
    polygon.lineTo(toEsri(p2));
    polygon.lineTo(toEsri(p3));
    for (Point p : pn) {
      polygon.lineTo(toEsri(p));
    }
  }

  private static com.esri.core.geometry.Polygon simplify(com.esri.core.geometry.Polygon polygon) {
    OperatorSimplifyOGC op =
        (OperatorSimplifyOGC)
            OperatorFactoryLocal.getInstance().getOperator(Operator.Type.SimplifyOGC);
    return (com.esri.core.geometry.Polygon)
        op.execute(polygon, DefaultGeometry.SPATIAL_REFERENCE_4326, true, null);
  }

  /**
   * This object gets replaced by an internal proxy for serialization.
   *
   * @serialData a single byte array containing the Well-Known Binary representation.
   */
  private Object writeReplace() {
    return new WkbSerializationProxy(this.asWellKnownBinary());
  }

  public static class Builder implements Polygon.Builder {
    private final com.esri.core.geometry.Polygon polygon = new com.esri.core.geometry.Polygon();

    @NonNull
    @Override
    public Builder addRing(
        @NonNull Point p1, @NonNull Point p2, @NonNull Point p3, @NonNull Point... pn) {
      addPath(polygon, p1, p2, p3, pn);
      return this;
    }

    /**
     * Builds the polygon.
     *
     * @return the polygon.
     */
    @NonNull
    @Override
    public Polygon build() {
      return new DefaultPolygon(
          new OGCPolygon(simplify(polygon), DefaultGeometry.SPATIAL_REFERENCE_4326));
    }
  }
}
