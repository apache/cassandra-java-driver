/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

public class TupleValueSerializer extends AbstractDynamicGraphBinaryCustomSerializer<TupleValue> {

  private final DefaultDriverContext driverContext;

  public TupleValueSerializer(DefaultDriverContext driverContext) {
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
