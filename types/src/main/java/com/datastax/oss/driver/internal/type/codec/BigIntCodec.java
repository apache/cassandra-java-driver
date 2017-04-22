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
import com.datastax.oss.driver.api.type.codec.PrimitiveLongCodec;
import com.datastax.oss.driver.api.type.reflect.GenericType;
import java.nio.ByteBuffer;

public class BigIntCodec implements PrimitiveLongCodec {
  @Override
  public GenericType<Long> getJavaType() {
    return GenericType.LONG;
  }

  @Override
  public DataType getCqlType() {
    return DataTypes.BIGINT;
  }

  @Override
  public ByteBuffer encodePrimitive(long value, ProtocolVersion protocolVersion) {
    ByteBuffer bytes = ByteBuffer.allocate(8);
    bytes.putLong(0, value);
    return bytes;
  }

  @Override
  public long decodePrimitive(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return 0;
    } else if (bytes.remaining() != 8) {
      throw new IllegalArgumentException(
          "Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());
    } else {
      return bytes.getLong(bytes.position());
    }
  }

  @Override
  public String format(Long value) {
    return (value == null) ? "NULL" : Long.toString(value);
  }

  @Override
  public Long parse(String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse 64-bits long value from \"%s\"", value));
    }
  }
}
