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
import com.esri.core.geometry.ogc.OGCPoint;
import edu.umd.cs.findbugs.annotations.NonNull;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultPoint extends DefaultGeometry implements Point {

  private static final long serialVersionUID = -8337622213980781285L;

  public DefaultPoint(double x, double y) {
    this(
        new OGCPoint(
            new com.esri.core.geometry.Point(x, y), DefaultGeometry.SPATIAL_REFERENCE_4326));
  }

  public DefaultPoint(@NonNull OGCPoint point) {
    super(point);
  }

  @NonNull
  @Override
  public OGCPoint getOgcGeometry() {
    return (OGCPoint) super.getOgcGeometry();
  }

  @Override
  public double X() {
    return getOgcGeometry().X();
  }

  @Override
  public double Y() {
    return getOgcGeometry().Y();
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
