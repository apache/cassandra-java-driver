/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.Polygon;
import java.nio.ByteBuffer;

public class PolygonSerializer extends GeometrySerializer<Polygon> {
  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_POLYGON_TYPE_NAME;
  }

  @Override
  public Polygon fromWellKnownBinary(ByteBuffer buffer) {
    return Polygon.fromWellKnownBinary(buffer);
  }
}
