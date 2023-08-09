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
package com.datastax.oss.driver.internal.core.type.codec.extras.array;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Base class for all codecs dealing with Java primitive arrays. This class provides a more
 * efficient implementation of {@link #encode(Object, ProtocolVersion)} and {@link
 * #decode(ByteBuffer, ProtocolVersion)} for primitive arrays.
 *
 * @param <PrimitiveArrayT> The Java primitive array type this codec handles
 */
public abstract class AbstractPrimitiveListToArrayCodec<PrimitiveArrayT>
    extends AbstractListToArrayCodec<PrimitiveArrayT> {

  /**
   * @param cqlType The CQL type. Must be a list type.
   * @param javaClass The Java type. Must be an array class.
   */
  protected AbstractPrimitiveListToArrayCodec(
      @NonNull ListType cqlType, @NonNull GenericType<PrimitiveArrayT> javaClass) {
    super(cqlType, javaClass);
    GenericType<?> componentType = Objects.requireNonNull(javaClass.getComponentType());
    if (!componentType.isPrimitive()) {
      throw new IllegalArgumentException(
          "Expecting primitive array component type, got " + componentType);
    }
  }

  @Nullable
  @Override
  public ByteBuffer encode(
      @Nullable PrimitiveArrayT array, @NonNull ProtocolVersion protocolVersion) {
    if (array == null) {
      return null;
    }
    int length = Array.getLength(array);
    int sizeOfElement = 4 + sizeOfComponentType();
    int totalSize = 4 + length * sizeOfElement;
    ByteBuffer output = ByteBuffer.allocate(totalSize);
    output.putInt(length);
    for (int i = 0; i < length; i++) {
      output.putInt(sizeOfComponentType());
      serializeElement(output, array, i, protocolVersion);
    }
    output.flip();
    return output;
  }

  @Nullable
  @Override
  public PrimitiveArrayT decode(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return newInstance(0);
    }
    ByteBuffer input = bytes.duplicate();
    int length = input.getInt();
    PrimitiveArrayT array = newInstance(length);
    for (int i = 0; i < length; i++) {
      int elementSize = input.getInt();
      // Null elements can happen on the decode path, but we cannot tolerate them
      if (elementSize < 0) {
        throw new NullPointerException("Primitive arrays cannot store null elements");
      } else {
        deserializeElement(input, array, i, protocolVersion);
      }
    }
    return array;
  }

  /**
   * Return the size in bytes of the array component type.
   *
   * @return the size in bytes of the array component type.
   */
  protected abstract int sizeOfComponentType();

  /**
   * Write the {@code index}th element of {@code array} to {@code output}.
   *
   * @param output The ByteBuffer to write to.
   * @param array The array to read from.
   * @param index The element index.
   * @param protocolVersion The protocol version to use.
   */
  protected abstract void serializeElement(
      @NonNull ByteBuffer output,
      @NonNull PrimitiveArrayT array,
      int index,
      @NonNull ProtocolVersion protocolVersion);

  /**
   * Read the {@code index}th element of {@code array} from {@code input}.
   *
   * @param input The ByteBuffer to read from.
   * @param array The array to write to.
   * @param index The element index.
   * @param protocolVersion The protocol version to use.
   */
  protected abstract void deserializeElement(
      @NonNull ByteBuffer input,
      @NonNull PrimitiveArrayT array,
      int index,
      @NonNull ProtocolVersion protocolVersion);
}
