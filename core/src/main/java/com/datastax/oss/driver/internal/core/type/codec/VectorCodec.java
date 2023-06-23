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
package com.datastax.oss.driver.internal.core.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.VectorType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.driver.shaded.guava.common.collect.Streams;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public class VectorCodec<SubtypeT> implements TypeCodec<List<SubtypeT>> {

  private final VectorType cqlType;
  private final GenericType<List<SubtypeT>> javaType;
  private final TypeCodec<SubtypeT> subtypeCodec;

  public VectorCodec(VectorType cqlType, TypeCodec<SubtypeT> subtypeCodec) {
    this.cqlType = cqlType;
    this.subtypeCodec = subtypeCodec;
    this.javaType = GenericType.listOf(subtypeCodec.getJavaType());
  }

  @NonNull
  @Override
  public GenericType<List<SubtypeT>> getJavaType() {
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
      @Nullable List<SubtypeT> value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null || cqlType.getDimensions() <= 0) {
      return null;
    }
    ByteBuffer[] valueBuffs = new ByteBuffer[cqlType.getDimensions()];
    Iterator<SubtypeT> values = value.iterator();
    int allValueBuffsSize = 0;
    for (int i = 0; i < cqlType.getDimensions(); ++i) {
      ByteBuffer valueBuff = this.subtypeCodec.encode(values.next(), protocolVersion);
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
  public List<SubtypeT> decode(
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

    ImmutableList.Builder<SubtypeT> builder = ImmutableList.builder();
    for (int i = 0; i < cqlType.getDimensions(); ++i) {
      ByteBuffer slice = bytes.slice();
      slice.limit(elementSize);
      builder.add(this.subtypeCodec.decode(slice, protocolVersion));
      bytes.position(bytes.position() + elementSize);
    }

    /* Restore the input ByteBuffer to its original state */
    bytes.rewind();

    return builder.build();
  }

  @NonNull
  @Override
  public String format(@Nullable List<SubtypeT> value) {
    return value == null ? "NULL" : Iterables.toString(value);
  }

  @Nullable
  @Override
  public List<SubtypeT> parse(@Nullable String value) {
    return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
        ? null
        : this.from(value);
  }

  private List<SubtypeT> from(@Nullable String value) {

    return Streams.stream(Splitter.on(", ").split(value.substring(1, value.length() - 1)))
        .map(subtypeCodec::parse)
        .collect(ImmutableList.toImmutableList());
  }
}
