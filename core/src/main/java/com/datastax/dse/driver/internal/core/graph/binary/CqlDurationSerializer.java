/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public class CqlDurationSerializer extends AbstractSimpleGraphBinaryCustomSerializer<CqlDuration> {

  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_DURATION_TYPE_NAME;
  }

  @Override
  protected CqlDuration readCustomValue(
      final int valueLength, final Buffer buffer, final GraphBinaryReader context)
      throws IOException {
    checkValueSize(GraphBinaryUtils.sizeOfDuration(), valueLength);
    return CqlDuration.newInstance(
        context.readValue(buffer, Integer.class, false),
        context.readValue(buffer, Integer.class, false),
        context.readValue(buffer, Long.class, false));
  }

  @Override
  protected void writeCustomValue(CqlDuration value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
    context.writeValue(GraphBinaryUtils.sizeOfDuration(), buffer, false);
    context.writeValue(value.getMonths(), buffer, false);
    context.writeValue(value.getDays(), buffer, false);
    context.writeValue(value.getNanoseconds(), buffer, false);
  }
}
