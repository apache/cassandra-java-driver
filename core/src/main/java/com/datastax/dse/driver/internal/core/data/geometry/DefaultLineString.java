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

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.esri.core.geometry.Polyline;
import com.esri.core.geometry.ogc.OGCLineString;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultLineString extends DefaultGeometry implements LineString {

  private static final long serialVersionUID = 1280189361978382248L;

  private static OGCLineString fromPoints(Point p1, Point p2, Point... pn) {
    Polyline polyline = new Polyline(toEsri(p1), toEsri(p2));
    for (Point p : pn) {
      polyline.lineTo(toEsri(p));
    }
    return new OGCLineString(polyline, 0, DefaultGeometry.SPATIAL_REFERENCE_4326);
  }

  private final List<Point> points;

  public DefaultLineString(@NonNull Point p1, @NonNull Point p2, @NonNull Point... pn) {
    super(fromPoints(p1, p2, pn));
    this.points = ImmutableList.<Point>builder().add(p1).add(p2).add(pn).build();
  }

  public DefaultLineString(@NonNull OGCLineString lineString) {
    super(lineString);
    this.points = getPoints(lineString);
  }

  @NonNull
  @Override
  public List<Point> getPoints() {
    return points;
  }

  /**
   * This object gets replaced by an internal proxy for serialization.
   *
   * @serialData a single byte array containing the Well-Known Binary representation.
   */
  private Object writeReplace() {
    return new WkbSerializationProxy(this.asWellKnownBinary());
  }
}
