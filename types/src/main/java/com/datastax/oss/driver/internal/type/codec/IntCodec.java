/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.type.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.type.DataType;
import com.datastax.oss.driver.api.type.DataTypes;
import com.datastax.oss.driver.api.type.codec.PrimitiveIntCodec;
import com.datastax.oss.driver.api.type.reflect.GenericType;
import java.nio.ByteBuffer;

public class IntCodec implements PrimitiveIntCodec {

  @Override
  public GenericType<Integer> getJavaType() {
    return GenericType.INTEGER;
  }

  @Override
  public DataType getCqlType() {
    return DataTypes.INT;
  }

  @Override
  public boolean canEncode(Object value) {
    return value instanceof Integer;
  }

  @Override
  public boolean canEncode(Class<?> javaClass) {
    return javaClass == Integer.class;
  }

  @Override
  public ByteBuffer encodePrimitive(int value, ProtocolVersion protocolVersion) {
    ByteBuffer bytes = ByteBuffer.allocate(4);
    bytes.putInt(0, value);
    return bytes;
  }

  @Override
  public int decodePrimitive(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return 0;
    } else if (bytes.remaining() != 4) {
      throw new IllegalArgumentException(
          "Invalid 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());
    } else {
      return bytes.getInt(bytes.position());
    }
  }

  @Override
  public String format(Integer value) {
    return (value == null) ? "NULL" : Integer.toString(value);
  }

  @Override
  public Integer parse(String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse 32-bits int value from \"%s\"", value));
    }
  }
}
