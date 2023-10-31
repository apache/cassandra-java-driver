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
package com.datastax.oss.driver.internal.core.type.codec.extras.vector;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Common super-class for all codecs which map a CQL vector type onto a primitive array */
public abstract class AbstractVectorToArrayCodec<ArrayT> implements TypeCodec<ArrayT> {

  @Nonnull protected final VectorType cqlType;
  @Nonnull protected final GenericType<ArrayT> javaType;

  /**
   * @param cqlType The CQL type. Must be a list type.
   * @param arrayType The Java type. Must be an array class.
   */
  protected AbstractVectorToArrayCodec(
      @Nonnull VectorType cqlType, @Nonnull GenericType<ArrayT> arrayType) {
    this.cqlType = Objects.requireNonNull(cqlType, "cqlType cannot be null");
    this.javaType = Objects.requireNonNull(arrayType, "arrayType cannot be null");
    if (!arrayType.isArray()) {
      throw new IllegalArgumentException("Expecting Java array class, got " + arrayType);
    }
  }

  @Nonnull
  @Override
  public GenericType<ArrayT> getJavaType() {
    return this.javaType;
  }

  @Nonnull
  @Override
  public DataType getCqlType() {
    return this.cqlType;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable ArrayT array, @Nonnull ProtocolVersion protocolVersion) {
    if (array == null) {
      return null;
    }
    int length = Array.getLength(array);
    int totalSize = length * sizeOfComponentType();
    ByteBuffer output = ByteBuffer.allocate(totalSize);
    for (int i = 0; i < length; i++) {
      serializeElement(output, array, i, protocolVersion);
    }
    output.flip();
    return output;
  }

  @Nullable
  @Override
  public ArrayT decode(@Nullable ByteBuffer bytes, @Nonnull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      throw new IllegalArgumentException(
          "Input ByteBuffer must not be null and must have non-zero remaining bytes");
    }
    ByteBuffer input = bytes.duplicate();
    int length = this.cqlType.getDimensions();
    int elementSize = sizeOfComponentType();
    ArrayT array = newInstance();
    for (int i = 0; i < length; i++) {
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
   * Creates a new array instance with a size matching the specified vector.
   *
   * @return a new array instance with a size matching the specified vector.
   */
  @Nonnull
  protected abstract ArrayT newInstance();

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
      @Nonnull ByteBuffer output,
      @Nonnull ArrayT array,
      int index,
      @Nonnull ProtocolVersion protocolVersion);

  /**
   * Read the {@code index}th element of {@code array} from {@code input}.
   *
   * @param input The ByteBuffer to read from.
   * @param array The array to write to.
   * @param index The element index.
   * @param protocolVersion The protocol version to use.
   */
  protected abstract void deserializeElement(
      @Nonnull ByteBuffer input,
      @Nonnull ArrayT array,
      int index,
      @Nonnull ProtocolVersion protocolVersion);
}
