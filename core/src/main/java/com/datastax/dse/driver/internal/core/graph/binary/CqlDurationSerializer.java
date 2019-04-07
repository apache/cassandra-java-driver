/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;

public class CqlDurationSerializer extends AbstractSimpleGraphBinaryCustomSerializer<CqlDuration> {

  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_DURATION_TYPE_NAME;
  }

  @Override
  protected CqlDuration readCustomValue(
      final int valueLength, final ByteBuf buffer, final GraphBinaryReader context)
      throws SerializationException {
    checkValueSize(GraphBinaryUtils.sizeOfDuration(), valueLength);
    return CqlDuration.newInstance(
        context.readValue(buffer, Integer.class, false),
        context.readValue(buffer, Integer.class, false),
        context.readValue(buffer, Long.class, false));
  }

  @Override
  protected void writeCustomValue(CqlDuration value, ByteBuf buffer, GraphBinaryWriter context)
      throws SerializationException {
    context.writeValue(GraphBinaryUtils.sizeOfDuration(), buffer, false);
    context.writeValue(value.getMonths(), buffer, false);
    context.writeValue(value.getDays(), buffer, false);
    context.writeValue(value.getNanoseconds(), buffer, false);
  }
}
