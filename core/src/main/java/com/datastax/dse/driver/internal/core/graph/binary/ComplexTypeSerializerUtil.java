/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.dse.driver.internal.core.context.DseDriverContext;
import com.datastax.dse.driver.internal.core.graph.ByteBufUtil;
import com.datastax.oss.driver.api.core.data.GettableByIndex;
import com.datastax.oss.driver.api.core.data.SettableByIndex;
import com.datastax.oss.driver.api.core.type.*;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.type.DataTypeHelper;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.result.RawType;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.buffer.ByteBuf;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

class ComplexTypeSerializerUtil {

  private static final PrimitiveCodec<ByteBuf> protocolCodec =
      new ByteBufPrimitiveCodec(GraphBinaryModule.ALLOCATOR);

  static void encodeTypeDefinition(DataType type, ByteBuf buffer, DseDriverContext driverContext) {
    RawType protocolType = toProtocolSpec(type);
    protocolType.encode(buffer, protocolCodec, driverContext.getProtocolVersion().getCode());
  }

  static DataType decodeTypeDefinition(ByteBuf buffer, DseDriverContext driverContext) {
    RawType type =
        RawType.decode(buffer, protocolCodec, driverContext.getProtocolVersion().getCode());
    return DataTypeHelper.fromProtocolSpec(type, driverContext);
  }

  /* Netty-based encoding of UDT values, based on the UdtCoded.encode() method, but using Netty buffers directly to avoid
  unnecessary NIO ByteBuffer copies. */
  static void encodeValue(@Nullable GettableByIndex value, ByteBuf nettyBuf) {
    if (value == null) {
      return;
    }

    for (int i = 0; i < value.size(); i++) {
      ByteBuffer fieldBuffer = value.getBytesUnsafe(i);
      if (fieldBuffer == null) {
        nettyBuf.writeInt(-1);
      } else {
        nettyBuf.writeInt(fieldBuffer.remaining());
        nettyBuf.writeBytes(fieldBuffer.duplicate());
      }
    }
  }

  /* This method will move forward the netty buffer given in parameter based on the UDT value read.
  Content of the method is roughly equivalent to UdtCodec.decode(), but using Netty buffers directly to avoid
  unnecessary NIO ByteBuffer copies. */
  static <T extends SettableByIndex<T>> T decodeValue(ByteBuf nettyBuf, T val, int size) {
    try {
      for (int i = 0; i < size; i++) {
        int fieldSize = nettyBuf.readInt();
        if (fieldSize >= 0) {
          // the reassignment is to shut down the error-prone warning about ignoring return values.
          val = val.setBytesUnsafe(i, ByteBufUtil.readBytes(nettyBuf, fieldSize));
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
