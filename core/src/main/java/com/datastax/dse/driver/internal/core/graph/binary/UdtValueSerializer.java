/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public class UdtValueSerializer extends AbstractDynamicGraphBinaryCustomSerializer<UdtValue> {
  private final DseDriverContext driverContext;

  public UdtValueSerializer(DseDriverContext driverContext) {
    this.driverContext = driverContext;
  }

  @Override
  public String getTypeName() {
    return GraphBinaryModule.GRAPH_BINARY_UDT_VALUE_TYPE_NAME;
  }

  @Override
  public UdtValue readDynamicCustomValue(Buffer buffer, GraphBinaryReader context)
      throws IOException {
    // read type definition first
    DataType driverType = ComplexTypeSerializerUtil.decodeTypeDefinition(buffer, driverContext);

    assert driverType instanceof UserDefinedType
        : "GraphBinary UdtValue deserializer was called on a value that is not encoded as a UdtValue.";

    UserDefinedType userDefinedType = (UserDefinedType) driverType;
    UdtValue value = userDefinedType.newValue();

    // then read values
    return ComplexTypeSerializerUtil.decodeValue(
        buffer, value, userDefinedType.getFieldTypes().size());
  }

  @Override
  public void writeDynamicCustomValue(UdtValue value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
    // write type first in native protocol format
    ComplexTypeSerializerUtil.encodeTypeDefinition(value.getType(), buffer, driverContext);
    // write value after
    ComplexTypeSerializerUtil.encodeValue(value, buffer);
  }
}
