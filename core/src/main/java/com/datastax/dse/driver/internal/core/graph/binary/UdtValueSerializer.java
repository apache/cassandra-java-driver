/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public class UdtValueSerializer extends AbstractDynamicGraphBinaryCustomSerializer<UdtValue> {
  private final DefaultDriverContext driverContext;

  public UdtValueSerializer(DefaultDriverContext driverContext) {
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
