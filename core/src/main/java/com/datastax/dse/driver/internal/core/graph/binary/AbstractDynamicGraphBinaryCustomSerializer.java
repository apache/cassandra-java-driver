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

import java.io.IOException;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;

/**
 * Convenience class for dynamic types implemented as Custom types in GraphBinary. This class will
 * take care of handling {value_length} automatically for implementing classes. {@link
 * #writeDynamicCustomValue(Object, Buffer, GraphBinaryWriter)} and {@link
 * #readDynamicCustomValue(Buffer, GraphBinaryReader)} only need to handle writing the internal
 * components of the custom type.
 *
 * @param <T> the java type the implementing classes will encode and decode.
 */
public abstract class AbstractDynamicGraphBinaryCustomSerializer<T>
    extends AbstractSimpleGraphBinaryCustomSerializer<T> {
  protected abstract void writeDynamicCustomValue(T value, Buffer buffer, GraphBinaryWriter context)
      throws IOException;

  protected abstract T readDynamicCustomValue(Buffer buffer, GraphBinaryReader context)
      throws IOException;

  @Override
  protected T readCustomValue(int valueLength, Buffer buffer, GraphBinaryReader context)
      throws IOException {
    int initialIndex = buffer.readerIndex();

    // read actual custom value
    T read = readDynamicCustomValue(buffer, context);

    // make sure we didn't read more than what was input as {value_length}
    checkValueSize(valueLength, (buffer.readerIndex() - initialIndex));

    return read;
  }

  @Override
  protected void writeCustomValue(T value, Buffer buffer, GraphBinaryWriter context)
      throws IOException {
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
