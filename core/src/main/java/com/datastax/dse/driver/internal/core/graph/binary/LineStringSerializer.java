/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.LineString;
import java.nio.ByteBuffer;

public class LineStringSerializer extends GeometrySerializer<LineString> {
  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_LINESTRING_TYPE_NAME;
  }

  @Override
  public LineString fromWellKnownBinary(ByteBuffer buffer) {
    return LineString.fromWellKnownBinary(buffer);
  }
}
