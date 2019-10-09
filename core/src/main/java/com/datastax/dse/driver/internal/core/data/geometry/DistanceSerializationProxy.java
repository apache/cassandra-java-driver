/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.data.geometry;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import java.io.Serializable;

/**
 * A thin wrapper around {@link Distance}, that gets substituted during the serialization /
 * deserialization process. This allows {@link Distance} to be immutable and reference centers' OGC
 * counterpart.
 */
public class DistanceSerializationProxy implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Point center;
  private final double radius;

  public DistanceSerializationProxy(Distance distance) {
    this.center = distance.getCenter();
    this.radius = distance.getRadius();
  }

  private Object readResolve() {
    return new Distance(center, radius);
  }
}
