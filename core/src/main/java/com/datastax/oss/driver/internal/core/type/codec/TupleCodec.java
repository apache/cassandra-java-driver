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
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class TupleCodec implements TypeCodec<TupleValue> {

  private final TupleType cqlType;

  public TupleCodec(@NonNull TupleType cqlType) {
    this.cqlType = cqlType;
  }

  @NonNull
  @Override
  public GenericType<TupleValue> getJavaType() {
    return GenericType.TUPLE_VALUE;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return cqlType;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return (value instanceof TupleValue) && ((TupleValue) value).getType().equals(cqlType);
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return TupleValue.class.equals(javaClass);
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable TupleValue value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    if (!value.getType().equals(cqlType)) {
      throw new IllegalArgumentException(
          String.format("Invalid tuple type, expected %s but got %s", cqlType, value.getType()));
    }
    // Encoding: each field as a [bytes] value ([bytes] = int length + contents, null is
    // represented by -1)
    int toAllocate = 0;
    for (int i = 0; i < value.size(); i++) {
      ByteBuffer field = value.getBytesUnsafe(i);
      toAllocate += 4 + (field == null ? 0 : field.remaining());
    }
    ByteBuffer result = ByteBuffer.allocate(toAllocate);
    for (int i = 0; i < value.size(); i++) {
      ByteBuffer field = value.getBytesUnsafe(i);
      if (field == null) {
        result.putInt(-1);
      } else {
        result.putInt(field.remaining());
        result.put(field.duplicate());
      }
    }
    return (ByteBuffer) result.flip();
  }

  @Nullable
  @Override
  public TupleValue decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null) {
      return null;
    }
    // empty byte buffers will result in empty values
    try {
      ByteBuffer input = bytes.duplicate();
      TupleValue value = cqlType.newValue();
      int i = 0;
      while (input.hasRemaining()) {
        if (i > cqlType.getComponentTypes().size()) {
          throw new IllegalArgumentException(
              String.format(
                  "Too many fields in encoded tuple, expected %d",
                  cqlType.getComponentTypes().size()));
        }
        int elementSize = input.getInt();
        ByteBuffer element;
        if (elementSize < 0) {
          element = null;
        } else {
          element = input.slice();
          element.limit(elementSize);
          input.position(input.position() + elementSize);
        }
        value = value.setBytesUnsafe(i, element);
        i += 1;
      }
      return value;
    } catch (BufferUnderflowException e) {
      throw new IllegalArgumentException("Not enough bytes to deserialize a tuple", e);
    }
  }

  @NonNull
  @Override
  public String format(@Nullable TupleValue value) {
    if (value == null) {
      return "NULL";
    }
    if (!value.getType().equals(cqlType)) {
      throw new IllegalArgumentException(
          String.format("Invalid tuple type, expected %s but got %s", cqlType, value.getType()));
    }
    CodecRegistry registry = cqlType.getAttachmentPoint().getCodecRegistry();

    StringBuilder sb = new StringBuilder("(");
    boolean first = true;
    for (int i = 0; i < value.size(); i++) {
      if (first) {
        first = false;
      } else {
        sb.append(",");
      }
      DataType elementType = cqlType.getComponentTypes().get(i);
      TypeCodec<Object> codec = registry.codecFor(elementType);
      sb.append(codec.format(value.get(i, codec)));
    }
    sb.append(")");
    return sb.toString();
  }

  @Nullable
  @Override
  public TupleValue parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    }

    TupleValue tuple = cqlType.newValue();
    int length = value.length();

    int position = ParseUtils.skipSpaces(value, 0);
    if (value.charAt(position) != '(') {
      throw new IllegalArgumentException(
          String.format(
              "Cannot parse tuple value from \"%s\", at character %d expecting '(' but got '%c'",
              value, position, value.charAt(position)));
    }

    position++;
    position = ParseUtils.skipSpaces(value, position);

    CodecRegistry registry = cqlType.getAttachmentPoint().getCodecRegistry();

    int field = 0;
    while (position < length) {
      if (value.charAt(position) == ')') {
        position = ParseUtils.skipSpaces(value, position + 1);
        if (position == length) {
          return tuple;
        }
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse tuple value from \"%s\", at character %d expecting EOF or blank, but got \"%s\"",
                value, position, value.substring(position)));
      }
      int n;
      try {
        n = ParseUtils.skipCQLValue(value, position);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse tuple value from \"%s\", invalid CQL value at field %d (character %d)",
                value, field, position),
            e);
      }

      String fieldValue = value.substring(position, n);
      DataType elementType = cqlType.getComponentTypes().get(field);
      TypeCodec<Object> codec = registry.codecFor(elementType);
      Object parsed;
      try {
        parsed = codec.parse(fieldValue);
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse tuple value from \"%s\", invalid CQL value at field %d (character %d): %s",
                value, field, position, e.getMessage()),
            e);
      }
      tuple = tuple.set(field, parsed, codec);

      position = n;

      position = ParseUtils.skipSpaces(value, position);
      if (position == length) {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse tuple value from \"%s\", at field %d (character %d) expecting ',' or ')', but got EOF",
                value, field, position));
      }
      if (value.charAt(position) == ')') {
        continue;
      }
      if (value.charAt(position) != ',') {
        throw new IllegalArgumentException(
            String.format(
                "Cannot parse tuple value from \"%s\", at field %d (character %d) expecting ',' but got '%c'",
                value, field, position, value.charAt(position)));
      }
      ++position; // skip ','

      position = ParseUtils.skipSpaces(value, position);
      field += 1;
    }
    throw new IllegalArgumentException(
        String.format(
            "Cannot parse tuple value from \"%s\", at field %d (character %d) expecting CQL value or ')', got EOF",
            value, field, position));
  }
}
