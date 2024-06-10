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

import com.datastax.dse.driver.api.core.data.geometry.Geometry;
import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.esri.core.geometry.GeometryException;
import com.esri.core.geometry.SpatialReference;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCLineString;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import net.jcip.annotations.Immutable;

@Immutable
public abstract class DefaultGeometry implements Geometry, Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Default spatial reference for Well Known Text / Well Known Binary.
   *
   * <p>4326 is the <a href="http://www.epsg.org/">EPSG</a> identifier of the <a
   * href="https://en.wikipedia.org/wiki/World_Geodetic_System">World Geodetic System (WGS)</a> in
   * its later revision, WGS 84.
   */
  public static final SpatialReference SPATIAL_REFERENCE_4326 = SpatialReference.create(4326);

  @NonNull
  public static <T extends OGCGeometry> T fromOgcWellKnownText(
      @NonNull String source, @NonNull Class<T> klass) {
    OGCGeometry geometry;
    try {
      geometry = OGCGeometry.fromText(source);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    validateType(geometry, klass);
    return klass.cast(geometry);
  }

  @NonNull
  public static <T extends OGCGeometry> T fromOgcWellKnownBinary(
      @NonNull ByteBuffer source, @NonNull Class<T> klass) {
    OGCGeometry geometry;
    try {
      geometry = OGCGeometry.fromBinary(source);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    validateType(geometry, klass);
    return klass.cast(geometry);
  }

  @NonNull
  public static <T extends OGCGeometry> T fromOgcGeoJson(
      @NonNull String source, @NonNull Class<T> klass) {
    OGCGeometry geometry;
    try {
      geometry = OGCGeometry.fromGeoJson(source);
    } catch (Exception e) {
      throw new IllegalArgumentException(e.getMessage());
    }
    validateType(geometry, klass);
    return klass.cast(geometry);
  }

  private static void validateType(OGCGeometry geometry, Class<? extends OGCGeometry> klass) {
    if (!geometry.getClass().equals(klass)) {
      throw new IllegalArgumentException(
          String.format(
              "%s is not of type %s", geometry.getClass().getSimpleName(), klass.getSimpleName()));
    }
  }

  private final OGCGeometry ogcGeometry;

  protected DefaultGeometry(@NonNull OGCGeometry ogcGeometry) {
    this.ogcGeometry = ogcGeometry;
    Preconditions.checkNotNull(ogcGeometry);
    validateOgcGeometry(ogcGeometry);
  }

  private static void validateOgcGeometry(OGCGeometry geometry) {
    try {
      if (geometry.is3D()) {
        throw new IllegalArgumentException(String.format("'%s' is not 2D", geometry.asText()));
      }
      if (!geometry.isSimple()) {
        throw new IllegalArgumentException(
            String.format(
                "'%s' is not simple. Points and edges cannot self-intersect.", geometry.asText()));
      }
    } catch (GeometryException e) {
      throw new IllegalArgumentException("Invalid geometry" + e.getMessage());
    }
  }

  @NonNull
  public static ImmutableList<Point> getPoints(@NonNull OGCLineString lineString) {
    ImmutableList.Builder<Point> builder = ImmutableList.builder();
    for (int i = 0; i < lineString.numPoints(); i++) {
      builder.add(new DefaultPoint(lineString.pointN(i)));
    }
    return builder.build();
  }

  protected static com.esri.core.geometry.Point toEsri(Point p) {
    return new com.esri.core.geometry.Point(p.X(), p.Y());
  }

  @NonNull
  public OGCGeometry getOgcGeometry() {
    return ogcGeometry;
  }

  @NonNull
  public com.esri.core.geometry.Geometry getEsriGeometry() {
    return ogcGeometry.getEsriGeometry();
  }

  @NonNull
  @Override
  public String asWellKnownText() {
    return ogcGeometry.asText();
  }

  @NonNull
  @Override
  public ByteBuffer asWellKnownBinary() {
    return WkbUtil.asLittleEndianBinary(ogcGeometry);
  }

  @NonNull
  @Override
  public String asGeoJson() {
    return ogcGeometry.asGeoJson();
  }

  @Override
  public boolean contains(@NonNull Geometry other) {
    Preconditions.checkNotNull(other);
    if (other instanceof DefaultGeometry) {
      DefaultGeometry defautlOther = (DefaultGeometry) other;
      return getOgcGeometry().contains(defautlOther.getOgcGeometry());
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultGeometry)) {
      return false;
    }
    DefaultGeometry that = (DefaultGeometry) o;
    return this.getOgcGeometry().equals(that.getOgcGeometry());
  }

  @Override
  public int hashCode() {
    // OGCGeometry subclasses do not overwrite Object.hashCode()
    // while com.esri.core.geometry.Geometry subclasses usually do,
    // so use these instead; this is consistent with equals
    // because OGCGeometry.equals() actually compare between
    // com.esri.core.geometry.Geometry objects
    return getEsriGeometry().hashCode();
  }

  // Should never be called since we serialize a proxy (see subclasses)
  @SuppressWarnings("UnusedVariable")
  private void readObject(ObjectInputStream stream) throws InvalidObjectException {
    throw new InvalidObjectException("Proxy required");
  }

  @Override
  public String toString() {
    return asWellKnownText();
  }
}
