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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.data.CqlVector;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.DefaultVectorType;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class VectorCodec<SubtypeT extends DataType> implements TypeCodec<CqlVector<SubtypeT>> {

  private final VectorType cqlType;
  private final GenericType<CqlVector<SubtypeT>> javaType;
  private final TypeCodec<SubtypeT> subtypeCodec;

  public VectorCodec(@NonNull VectorType cqlType, @NonNull TypeCodec<SubtypeT> subtypeCodec) {
    this.cqlType = cqlType;
    this.subtypeCodec = subtypeCodec;
    this.javaType = GenericType.vectorOf(subtypeCodec.getJavaType());
  }

  public VectorCodec(int dimensions, @NonNull TypeCodec<SubtypeT> subtypeCodec) {
    this(new DefaultVectorType(subtypeCodec.getCqlType(), dimensions), subtypeCodec);
  }

  @NonNull
  @Override
  public GenericType<CqlVector<SubtypeT>> getJavaType() {
    return this.javaType;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return this.cqlType;
  }

  @Nullable
  @Override
  public ByteBuffer encode(
      @Nullable CqlVector<SubtypeT> value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null || cqlType.getDimensions() <= 0) {
      return null;
    }
    ByteBuffer[] valueBuffs = new ByteBuffer[cqlType.getDimensions()];
    Iterator<SubtypeT> values = value.iterator();
    int allValueBuffsSize = 0;
    for (int i = 0; i < cqlType.getDimensions(); ++i) {
      ByteBuffer valueBuff;
      SubtypeT valueObj;

      try {
        valueObj = values.next();
      } catch (NoSuchElementException nsee) {
        throw new IllegalArgumentException(
            String.format(
                "Not enough elements; must provide elements for %d dimensions",
                cqlType.getDimensions()));
      }

      try {
        valueBuff = this.subtypeCodec.encode(valueObj, protocolVersion);
      } catch (ClassCastException e) {
        throw new IllegalArgumentException("Invalid type for element: " + valueObj.getClass());
      }
      if (valueBuff == null) {
        throw new NullPointerException("Vector elements cannot encode to CQL NULL");
      }
      allValueBuffsSize += valueBuff.limit();
      valueBuff.rewind();
      valueBuffs[i] = valueBuff;
    }
    /* Since we already did an early return for <= 0 dimensions above */
    assert valueBuffs.length > 0;
    ByteBuffer rv = ByteBuffer.allocate(allValueBuffsSize);
    for (int i = 0; i < cqlType.getDimensions(); ++i) {
      rv.put(valueBuffs[i]);
    }
    rv.flip();
    return rv;
  }

  @Nullable
  @Override
  public CqlVector<SubtypeT> decode(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    }

    /* Determine element size by dividing count of remaining bytes by number of elements.  This should have a remainder
    of zero if we assume all elements are of uniform size (which is really a terrible assumption).

    TODO: We should probably tweak serialization format for vectors if we're going to allow them for arbitrary subtypes.
     Elements should at least precede themselves with their size (along the lines of what lists do). */
    int elementSize = Math.floorDiv(bytes.remaining(), cqlType.getDimensions());
    if (!(bytes.remaining() % cqlType.getDimensions() == 0)) {
      throw new IllegalArgumentException(
          String.format(
              "Expected elements of uniform size, observed %d elements with total bytes %d",
              cqlType.getDimensions(), bytes.remaining()));
    }

    ByteBuffer slice = bytes.slice();
    List<SubtypeT> rv = new ArrayList<SubtypeT>(cqlType.getDimensions());
    for (int i = 0; i < cqlType.getDimensions(); ++i) {
      // Set the limit for the current element
      int originalPosition = slice.position();
      slice.limit(originalPosition + elementSize);
      rv.add(this.subtypeCodec.decode(slice, protocolVersion));
      // Move to the start of the next element
      slice.position(originalPosition + elementSize);
      // Reset the limit to the end of the buffer
      slice.limit(slice.capacity());
    }

    return CqlVector.newInstance(rv);
  }

  @NonNull
  @Override
  public String format(@Nullable CqlVector<SubtypeT> value) {
    return value == null ? "NULL" : Iterables.toString(value);
  }

  @Nullable
  @Override
  public CqlVector<SubtypeT> parse(@Nullable String value) {
    return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
        ? null
        : CqlVector.from(value, this.subtypeCodec);
  }
}
