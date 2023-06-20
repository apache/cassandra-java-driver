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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * A codec that maps the CQL type {@code list<int>} to the Java type {@code int[]}.
 *
 * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
 * {@code int[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code int}
 * values; it also instantiates arrays without the need for an intermediary Java {@code List}
 * object.
 */
@Immutable
public class IntListToArrayCodec extends AbstractPrimitiveListToArrayCodec<int[]> {

  public IntListToArrayCodec() {
    super(DataTypes.listOf(DataTypes.INT), GenericType.of(int[].class));
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    Objects.requireNonNull(javaClass);
    return int[].class.equals(javaClass);
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    return value instanceof int[];
  }

  @Override
  protected int sizeOfComponentType() {
    return 4;
  }

  @Override
  protected void serializeElement(
      @NonNull ByteBuffer output,
      @NonNull int[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    output.putInt(array[index]);
  }

  @Override
  protected void deserializeElement(
      @NonNull ByteBuffer input,
      @NonNull int[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    array[index] = input.getInt();
  }

  @Override
  protected void formatElement(@NonNull StringBuilder output, @NonNull int[] array, int index) {
    output.append(array[index]);
  }

  @Override
  protected void parseElement(@NonNull String input, @NonNull int[] array, int index) {
    array[index] = Integer.parseInt(input);
  }

  @NonNull
  @Override
  protected int[] newInstance(int size) {
    return new int[size];
  }
}
