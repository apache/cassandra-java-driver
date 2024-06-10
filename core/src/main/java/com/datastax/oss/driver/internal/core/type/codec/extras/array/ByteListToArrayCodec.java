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
package com.datastax.oss.driver.internal.core.type.codec.extras.array;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.SimpleBlobCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * A codec that maps the CQL type {@code list<byte>} to the Java type {@code byte[]}.
 *
 * <p>Note that this codec is not suitable for reading CQL blobs as byte arrays; you should use
 * {@link SimpleBlobCodec} for that.
 *
 * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
 * {@code byte[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code byte}
 * values; it also instantiates arrays without the need for an intermediary Java {@code List}
 * object.
 */
@Immutable
public class ByteListToArrayCodec extends AbstractPrimitiveListToArrayCodec<byte[]> {

  public ByteListToArrayCodec() {
    super(DataTypes.listOf(DataTypes.SMALLINT), GenericType.of(byte[].class));
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    Objects.requireNonNull(javaClass);
    return byte[].class.equals(javaClass);
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    return value instanceof byte[];
  }

  @Override
  protected int sizeOfComponentType() {
    return 1;
  }

  @Override
  protected void serializeElement(
      @NonNull ByteBuffer output,
      @NonNull byte[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    output.put(array[index]);
  }

  @Override
  protected void deserializeElement(
      @NonNull ByteBuffer input,
      @NonNull byte[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    array[index] = input.get();
  }

  @Override
  protected void formatElement(@NonNull StringBuilder output, @NonNull byte[] array, int index) {
    output.append(array[index]);
  }

  @Override
  protected void parseElement(@NonNull String input, @NonNull byte[] array, int index) {
    array[index] = Byte.parseByte(input);
  }

  @NonNull
  @Override
  protected byte[] newInstance(int size) {
    return new byte[size];
  }
}
