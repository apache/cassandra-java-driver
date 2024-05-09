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
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.FloatCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

/** A codec that maps CQL vectors to the Java type {@code float[]}. */
public class FloatVectorToArrayCodec extends AbstractVectorToArrayCodec<float[]> {

  public FloatVectorToArrayCodec(VectorType type) {
    super(type, GenericType.of(float[].class));
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

  @NonNull
  @Override
  protected float[] newInstance() {
    return new float[cqlType.getDimensions()];
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

  @NonNull
  @Override
  public String format(@Nullable float[] value) {
    return value == null ? "NULL" : Arrays.toString(value);
  }

  @Nullable
  @Override
  public float[] parse(@Nullable String str) {
    Preconditions.checkArgument(str != null, "Cannot create float array from null string");
    Preconditions.checkArgument(!str.isEmpty(), "Cannot create float array from empty string");

    FloatCodec codec = new FloatCodec();
    float[] rv = this.newInstance();
    Iterator<String> strIter =
        Splitter.on(", ").trimResults().split(str.substring(1, str.length() - 1)).iterator();
    for (int i = 0; i < rv.length; ++i) {
      String strVal = strIter.next();
      if (strVal == null) {
        throw new IllegalArgumentException("Null element observed in float array string");
      }
      Float f = codec.parse(strVal);
      rv[i] = f.floatValue();
    }
    return rv;
  }
}
