/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.javatuples.Pair;

public class PairSerializer extends AbstractDynamicGraphBinaryCustomSerializer<Pair> {

  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_PAIR_TYPE_NAME;
  }

  @Override
  protected Pair readDynamicCustomValue(ByteBuf buffer, GraphBinaryReader context)
      throws SerializationException {
    return new Pair<>(context.read(buffer), context.read(buffer));
  }

  @Override
  protected void writeDynamicCustomValue(Pair value, ByteBuf buffer, GraphBinaryWriter context)
      throws SerializationException {
    context.write(value.getValue0(), buffer);
    context.write(value.getValue1(), buffer);
  }
}
