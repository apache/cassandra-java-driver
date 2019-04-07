/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.api.core.data.geometry.Point;
import com.datastax.dse.driver.internal.core.data.geometry.Distance;
import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

public class DistanceSerializer extends AbstractSimpleGraphBinaryCustomSerializer<Distance> {
  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_DISTANCE_TYPE_NAME;
  }

  @Override
  protected Distance readCustomValue(int valueLength, ByteBuf buffer, GraphBinaryReader context)
      throws SerializationException {
    Point p = context.readValue(buffer, Point.class, false);
    checkValueSize(GraphBinaryUtils.sizeOfDistance(p), valueLength);
    return new Distance(p, context.readValue(buffer, Double.class, false));
  }

  @Override
  protected void writeCustomValue(Distance value, ByteBuf buffer, GraphBinaryWriter context)
      throws SerializationException {
    context.writeValue(GraphBinaryUtils.sizeOfDistance(value.getCenter()), buffer, false);
    context.writeValue(value.getCenter(), buffer, false);
    context.writeValue(value.getRadius(), buffer, false);
  }
}
