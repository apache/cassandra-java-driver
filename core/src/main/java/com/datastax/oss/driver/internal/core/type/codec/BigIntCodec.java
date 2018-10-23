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
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.PrimitiveLongCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class BigIntCodec implements PrimitiveLongCodec {
  @NonNull
  @Override
  public GenericType<Long> getJavaType() {
    return GenericType.LONG;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.BIGINT;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof Long;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == Long.class || javaClass == long.class;
  }

  @Nullable
  @Override
  public ByteBuffer encodePrimitive(long value, @NonNull ProtocolVersion protocolVersion) {
    ByteBuffer bytes = ByteBuffer.allocate(8);
    bytes.putLong(0, value);
    return bytes;
  }

  @Override
  public long decodePrimitive(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return 0;
    } else if (bytes.remaining() != 8) {
      throw new IllegalArgumentException(
          "Invalid 64-bits long value, expecting 8 bytes but got " + bytes.remaining());
    } else {
      return bytes.getLong(bytes.position());
    }
  }

  @NonNull
  @Override
  public String format(@Nullable Long value) {
    return (value == null) ? "NULL" : Long.toString(value);
  }

  @Nullable
  @Override
  public Long parse(@Nullable String value) {
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
