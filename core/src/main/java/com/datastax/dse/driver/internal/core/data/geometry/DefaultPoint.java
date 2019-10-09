/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
