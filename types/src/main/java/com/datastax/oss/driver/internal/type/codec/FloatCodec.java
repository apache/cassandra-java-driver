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
import com.datastax.oss.driver.api.type.codec.PrimitiveFloatCodec;
import com.datastax.oss.driver.api.type.reflect.GenericType;
import java.nio.ByteBuffer;

public class FloatCodec implements PrimitiveFloatCodec {
  @Override
  public GenericType<Float> getJavaType() {
    return GenericType.FLOAT;
  }

  @Override
  public DataType getCqlType() {
    return DataTypes.FLOAT;
  }

  @Override
  public ByteBuffer encodePrimitive(float value, ProtocolVersion protocolVersion) {
    ByteBuffer bytes = ByteBuffer.allocate(4);
    bytes.putFloat(0, value);
    return bytes;
  }

  @Override
  public float decodePrimitive(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return 0;
    } else if (bytes.remaining() != 4) {
      throw new IllegalArgumentException(
          "Invalid 32-bits float value, expecting 4 bytes but got " + bytes.remaining());
    } else {
      return bytes.getFloat(bytes.position());
    }
  }

  @Override
  public String format(Float value) {
    return (value == null) ? "NULL" : Float.toString(value);
  }

  @Override
  public Float parse(String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : Float.parseFloat(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse 32-bits float value from \"%s\"", value));
    }
  }
}
