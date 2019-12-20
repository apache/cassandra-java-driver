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

import com.datastax.dse.driver.internal.core.graph.TinkerpopBufferUtil;
import com.datastax.dse.driver.internal.core.graph.binary.buffer.DseNettyBufferFactory;
import com.datastax.dse.driver.internal.core.protocol.TinkerpopBufferPrimitiveCodec;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.SettableByIndex;
import com.datastax.oss.driver.api.core.type.CustomType;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.type.DataTypeHelper;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.RawType;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

class ComplexTypeSerializerUtil {

  private static final PrimitiveCodec<Buffer> codec =
      new TinkerpopBufferPrimitiveCodec(new DseNettyBufferFactory());

  static void encodeTypeDefinition(
      DataType type, Buffer buffer, DefaultDriverContext driverContext) {
    RawType protocolType = toProtocolSpec(type);
    protocolType.encode(buffer, codec, driverContext.getProtocolVersion().getCode());
  }

  static DataType decodeTypeDefinition(Buffer buffer, DefaultDriverContext driverContext) {
    RawType type = RawType.decode(buffer, codec, driverContext.getProtocolVersion().getCode());
    return DataTypeHelper.fromProtocolSpec(type, driverContext);
  }

  /* Tinkerpop-based encoding of UDT values, based on the UdtCoded.encode() method, but using Tinkerpop buffers directly to avoid
  unnecessary NIO ByteBuffer copies. */
  static void encodeValue(@Nullable GettableByIndex value, Buffer tinkerBuff) {
    if (value == null) {
      return;
    }

    for (int i = 0; i < value.size(); i++) {
      ByteBuffer fieldBuffer = value.getBytesUnsafe(i);
      if (fieldBuffer == null) {
        tinkerBuff.writeInt(-1);
      } else {
        tinkerBuff.writeInt(fieldBuffer.remaining());
        tinkerBuff.writeBytes(fieldBuffer.duplicate());
      }
    }
  }

  /* This method will move forward the Tinkerpop buffer given in parameter based on the UDT value read.
  Content of the method is roughly equivalent to UdtCodec.decode(), but using Tinkerpop buffers directly to avoid
  unnecessary NIO ByteBuffer copies. */
  static <T extends SettableByIndex<T>> T decodeValue(Buffer tinkerBuff, T val, int size) {
    try {
      for (int i = 0; i < size; i++) {
        int fieldSize = tinkerBuff.readInt();
        if (fieldSize >= 0) {
          // the reassignment is to shut down the error-prone warning about ignoring return values.
          val = val.setBytesUnsafe(i, TinkerpopBufferUtil.readBytes(tinkerBuff, fieldSize));
        }
      }
      return val;
    } catch (BufferUnderflowException e) {
      throw new IllegalArgumentException("Not enough bytes to deserialize a UDT value", e);
    }
  }

  private static RawType toProtocolSpec(DataType dataType) {
    int id = dataType.getProtocolCode();
    RawType type = RawType.PRIMITIVES.get(id);
    if (type != null) {
      return type;
    }

    switch (id) {
      case ProtocolConstants.DataType.CUSTOM:
        CustomType customType = ((CustomType) dataType);
        type = new RawType.RawCustom(customType.getClassName());
        break;
      case ProtocolConstants.DataType.LIST:
        ListType listType = ((ListType) dataType);
        type = new RawType.RawList(toProtocolSpec(listType.getElementType()));
        break;
      case ProtocolConstants.DataType.SET:
        SetType setType = ((SetType) dataType);
        type = new RawType.RawSet(toProtocolSpec(setType.getElementType()));
        break;
      case ProtocolConstants.DataType.MAP:
        MapType mapType = ((MapType) dataType);
        type =
            new RawType.RawMap(
                toProtocolSpec(mapType.getKeyType()), toProtocolSpec(mapType.getValueType()));
        break;
      case ProtocolConstants.DataType.TUPLE:
        TupleType tupleType = ((TupleType) dataType);
        ImmutableList.Builder<RawType> subTypesList =
            ImmutableList.builderWithExpectedSize(tupleType.getComponentTypes().size());
        for (int i = 0; i < tupleType.getComponentTypes().size(); i++) {
          subTypesList.add(toProtocolSpec(tupleType.getComponentTypes().get(i)));
        }
        type = new RawType.RawTuple(subTypesList.build());
        break;
      case ProtocolConstants.DataType.UDT:
        UserDefinedType userDefinedType = ((UserDefinedType) dataType);
        ImmutableMap.Builder<String, RawType> subTypesMap =
            ImmutableMap.builderWithExpectedSize(userDefinedType.getFieldNames().size());
        for (int i = 0; i < userDefinedType.getFieldTypes().size(); i++) {
          subTypesMap.put(
              userDefinedType.getFieldNames().get(i).asInternal(),
              toProtocolSpec(userDefinedType.getFieldTypes().get(i)));
        }
        type =
            new RawType.RawUdt(
                Objects.requireNonNull(userDefinedType.getKeyspace()).asInternal(),
                userDefinedType.getName().asInternal(),
                subTypesMap.build());
        break;
      default:
        throw new IllegalArgumentException("Unsupported type: " + dataType.asCql(true, true));
    }
    return type;
  }
}
