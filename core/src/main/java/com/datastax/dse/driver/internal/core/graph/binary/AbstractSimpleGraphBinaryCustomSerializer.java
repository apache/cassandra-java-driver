/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dse.driver.internal.core.graph.binary;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import org.apache.tinkerpop.gremlin.driver.ser.SerializationException;
import org.apache.tinkerpop.gremlin.driver.ser.binary.DataType;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.driver.ser.binary.GraphBinaryWriter;
import org.apache.tinkerpop.gremlin.driver.ser.binary.types.CustomTypeSerializer;

/**
 * A base custom type serializer for DSE types that handles most of the boiler plate code associated
 * with GraphBinary's custom types.
 *
 * <p>The full format of a custom type in GraphBinary is the following:
 *
 * <p>{type_code}{custom_type_name}{custom_type_info_length}{custom_type_info_bytes}{value_flag}{value_length}{value_bytes}
 *
 * <p>This class is made to handle
 * {type_code}{custom_type_name}{custom_type_info_length}{custom_type_info_bytes}{value_flag} for
 * DSE types.
 *
 * <p>Implementing classes are still in charge of encoding {value_length}{value_bytes} in the {@link
 * #readCustomValue(int, ByteBuf, GraphBinaryReader)} implementations.
 *
 * <p>Implementing classes must override {@link CustomTypeSerializer#getTypeName()} with their own
 * type name.
 *
 * @param <T> the java type the implementing classes will encode and decode.
 */
abstract class AbstractSimpleGraphBinaryCustomSerializer<T> implements CustomTypeSerializer<T> {
  AbstractSimpleGraphBinaryCustomSerializer() {
    super();
  }

  protected static final String INCORRECT_VALUE_LENGTH_ERROR_MESSAGE =
      "{value_length} read for this value does not correspond to the size of a '%s' value. [%s] bytes required but got [%s]";

  protected abstract T readCustomValue(int valueLength, ByteBuf buffer, GraphBinaryReader context)
      throws SerializationException;

  protected abstract void writeCustomValue(T value, ByteBuf buffer, GraphBinaryWriter context)
      throws SerializationException;

  protected void checkValueSize(int lengthRequired, int lengthFound) {
    Preconditions.checkArgument(
        lengthFound == lengthRequired,
        INCORRECT_VALUE_LENGTH_ERROR_MESSAGE,
        getTypeName(),
        lengthRequired,
        lengthFound);
  }

  @Override
  public DataType getDataType() {
    return DataType.CUSTOM;
  }

  @Override
  public T read(ByteBuf buffer, GraphBinaryReader context) throws SerializationException {
    // the type serializer registry will take care of deserializing {custom_type_name}
    // read {custom_type_info_length} and verify it is 0.
    // See #write(T, ByteBuf, GraphBinaryWriter) for why it is set to 0
    if (context.readValue(buffer, Integer.class, false) != 0) {
      throw new SerializationException(
          "{custom_type_info} should not be provided for this custom type");
    }

    return readValue(buffer, context, true);
  }

  @Override
  public T readValue(ByteBuf buffer, GraphBinaryReader context, boolean nullable)
      throws SerializationException {
    if (nullable) {
      // read {value_flag}
      final byte valueFlag = buffer.readByte();

      // if value is null and the value is nullable
      if ((valueFlag & 1) == 1) {
        return null;
      }
      // Note: we don't error out if the valueFlag == "value is null" and nullable == false because
      // the serializer
      // should have errored out at write time if that was the case.
    }

    // Read the byte length of the value bytes
    final int valueLength = buffer.readInt();

    if (valueLength <= 0) {
      throw new SerializationException(String.format("Unexpected value length: %d", valueLength));
    }

    if (valueLength > buffer.readableBytes()) {
      throw new SerializationException(
          String.format(
              "Not enough readable bytes: %d bytes required for value (%d bytes available)",
              valueLength, buffer.readableBytes()));
    }

    // subclasses are responsible for reading {value}
    return readCustomValue(valueLength, buffer, context);
  }

  @Override
  public void write(final T value, final ByteBuf buffer, final GraphBinaryWriter context)
      throws SerializationException {
    // the type serializer registry will take care of serializing {custom_type_name}
    // write "{custom_type_info_length}" to 0 because we don't need it for the DSE types
    context.writeValue(0, buffer, false);
    writeValue(value, buffer, context, true);
  }

  @Override
  public void writeValue(
      final T value, final ByteBuf buffer, final GraphBinaryWriter context, final boolean nullable)
      throws SerializationException {
    if (value == null) {
      if (!nullable) {
        throw new SerializationException("Unexpected null value when nullable is false");
      }

      // writes {value_flag} to "1" which means "the value is null"
      context.writeValueFlagNull(buffer);
      return;
    }

    if (nullable) {
      // writes {value_flag} to "0" which means "value is not null"
      context.writeValueFlagNone(buffer);
    }

    // sub classes will be responsible for writing {value_length} and {value_bytes}
    writeCustomValue(value, buffer, context);
  }
}
