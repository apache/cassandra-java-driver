/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.internal.core.graph.EditDistance;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public class EditDistanceSerializer
    extends AbstractSimpleGraphBinaryCustomSerializer<EditDistance> {
  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_EDIT_DISTANCE_TYPE_NAME;
  }

  @Override
  protected EditDistance readCustomValue(int valueLength, Buffer buffer, GraphBinaryReader context)
      throws IOException {
    int distance = context.readValue(buffer, Integer.class, false);
    String query = context.readValue(buffer, String.class, false);
    checkValueSize(GraphBinaryUtils.sizeOfEditDistance(query), valueLength);

    return new EditDistance(query, distance);
  }

  @Override
  protected void writeCustomValue(EditDistance value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
    context.writeValue(GraphBinaryUtils.sizeOfEditDistance(value.query), buffer, false);
    context.writeValue(value.distance, buffer, false);
    context.writeValue(value.query, buffer, false);
  }
}
