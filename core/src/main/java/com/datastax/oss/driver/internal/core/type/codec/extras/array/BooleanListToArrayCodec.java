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
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Objects;
import net.jcip.annotations.Immutable;

/**
 * A codec that maps the CQL type {@code list<boolean>} to the Java type {@code boolean[]}.
 *
 * <p>Note that this codec is designed for performance and converts CQL lists <em>directly</em> to
 * {@code boolean[]}, thus avoiding any unnecessary boxing and unboxing of Java primitive {@code
 * boolean} values; it also instantiates arrays without the need for an intermediary Java {@code
 * List} object.
 */
@Immutable
public class BooleanListToArrayCodec extends AbstractPrimitiveListToArrayCodec<boolean[]> {

  private static final byte TRUE = (byte) 1;
  private static final byte FALSE = (byte) 0;

  public BooleanListToArrayCodec() {
    super(DataTypes.listOf(DataTypes.BOOLEAN), GenericType.of(boolean[].class));
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    Objects.requireNonNull(javaClass);
    return boolean[].class.equals(javaClass);
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    Objects.requireNonNull(value);
    return value instanceof boolean[];
  }

  @Override
  protected int sizeOfComponentType() {
    return 1;
  }

  @Override
  protected void serializeElement(
      @NonNull ByteBuffer output,
      @NonNull boolean[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    byte element = array[index] ? TRUE : FALSE;
    output.put(element);
  }

  @Override
  protected void deserializeElement(
      @NonNull ByteBuffer input,
      @NonNull boolean[] array,
      int index,
      @NonNull ProtocolVersion protocolVersion) {
    array[index] = input.get() == TRUE;
  }

  @Override
  protected void formatElement(@NonNull StringBuilder output, @NonNull boolean[] array, int index) {
    output.append(array[index]);
  }

  @Override
  protected void parseElement(@NonNull String input, @NonNull boolean[] array, int index) {
    array[index] = Boolean.parseBoolean(input);
  }

  @NonNull
  @Override
  protected boolean[] newInstance(int size) {
    return new boolean[size];
  }
}
