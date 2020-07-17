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
 * A codec that maps the CQL type {@code list<float>} to the Java type {@code float[]}.
 *
 * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
 * {@code float[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
 * float} values; it also instantiates arrays without the need for an intermediary Java {@code List}
 * object.
 */
@Immutable
public class FloatListToArrayCodec extends AbstractPrimitiveListToArrayCodec<float[]> {

  public FloatListToArrayCodec() {
    super(DataTypes.listOf(DataTypes.FLOAT), GenericType.of(float[].class));
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    Objects.requireNonNull(javaClass);
    return float[].class.equals(javaClass);
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    return value instanceof float[];
  }

  @Override
  protected int sizeOfComponentType() {
    return 4;
  }

  @Override
  protected void serializeElement(
      @NonNull ByteBuffer output,
      @NonNull float[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    output.putFloat(array[index]);
  }

  @Override
  protected void deserializeElement(
      @NonNull ByteBuffer input,
      @NonNull float[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    array[index] = input.getFloat();
  }

  @Override
  protected void formatElement(@NonNull StringBuilder output, @NonNull float[] array, int index) {
    output.append(array[index]);
  }

  @Override
  protected void parseElement(@NonNull String input, @NonNull float[] array, int index) {
    array[index] = Float.parseFloat(input);
  }

  @NonNull
  @Override
  protected float[] newInstance(int size) {
    return new float[size];
  }
}
