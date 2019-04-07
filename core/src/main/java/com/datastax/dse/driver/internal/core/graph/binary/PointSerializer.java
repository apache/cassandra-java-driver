/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import java.nio.ByteBuffer;

public class PointSerializer extends GeometrySerializer<Point> {

  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_POINT_TYPE_NAME;
  }

  @Override
  public Point fromWellKnownBinary(ByteBuffer buffer) {
    return Point.fromWellKnownBinary(buffer);
  }
}
