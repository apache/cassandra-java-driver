/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public class TupleValueSerializer extends AbstractDynamicGraphBinaryCustomSerializer<TupleValue> {

  private final DseDriverContext driverContext;

  public TupleValueSerializer(DseDriverContext driverContext) {
    this.driverContext = driverContext;
  }

  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_TUPLE_VALUE_TYPE_NAME;
  }

  @Override
  public TupleValue readDynamicCustomValue(Buffer buffer, GraphBinaryReader context)
      throws IOException {
    // read the type first
    DataType type = ComplexTypeSerializerUtil.decodeTypeDefinition(buffer, driverContext);

    assert type instanceof TupleType
        : "GraphBinary TupleValue deserializer was called on a value that is not encoded as a TupleValue.";

    TupleType tupleType = (TupleType) type;
    TupleValue value = tupleType.newValue();

    // then decode the values from the buffer
    return ComplexTypeSerializerUtil.decodeValue(
        buffer, value, tupleType.getComponentTypes().size());
  }

  @Override
  public void writeDynamicCustomValue(TupleValue value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
    // write type first in native protocol
    ComplexTypeSerializerUtil.encodeTypeDefinition(value.getType(), buffer, driverContext);

    // write value after
    ComplexTypeSerializerUtil.encodeValue(value, buffer);
  }
}
