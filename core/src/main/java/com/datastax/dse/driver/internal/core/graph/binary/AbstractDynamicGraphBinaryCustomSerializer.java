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

/**
 * Convenience class for dynamic types implemented as Custom types in GraphBinary. This class will
 * take care of handling {value_length} automatically for implementing classes. {@link
 * #writeDynamicCustomValue(Object, ByteBuf, GraphBinaryWriter)} and {@link
 * #readDynamicCustomValue(ByteBuf, GraphBinaryReader)} only need to handle writing the internal
 * components of the custom type.
 *
 * @param <T> the java type the implementing classes will encode and decode.
 */
public abstract class AbstractDynamicGraphBinaryCustomSerializer<T>
    extends AbstractSimpleGraphBinaryCustomSerializer<T> {
  protected abstract void writeDynamicCustomValue(
      T value, ByteBuf buffer, GraphBinaryWriter context) throws SerializationException;

  protected abstract T readDynamicCustomValue(ByteBuf buffer, GraphBinaryReader context)
      throws SerializationException;

  @Override
  protected T readCustomValue(int valueLength, ByteBuf buffer, GraphBinaryReader context)
      throws SerializationException {
    int initialIndex = buffer.readerIndex();

    // read actual custom value
    T read = readDynamicCustomValue(buffer, context);

    // make sure we didn't read more than what was input as {value_length}
    checkValueSize((buffer.readerIndex() - initialIndex), valueLength);

    return read;
  }

  @Override
  protected void writeCustomValue(T value, ByteBuf buffer, GraphBinaryWriter context)
      throws SerializationException {
    // Store the current writer index
    final int valueLengthIndex = buffer.writerIndex();

    // Write a dummy length that will be overwritten at the end of this method
    buffer.writeInt(0);

    // Custom type's writer logic
    writeDynamicCustomValue(value, buffer, context);

    // value_length = diff written - 4 bytes for the dummy length
    final int valueLength = buffer.writerIndex() - valueLengthIndex - GraphBinaryUtils.sizeOfInt();

    // Go back, write the {value_length} and then reset back the writer index
    buffer.markWriterIndex().writerIndex(valueLengthIndex).writeInt(valueLength).resetWriterIndex();
  }
}
