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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class UuidCodec implements TypeCodec<UUID> {
  @NonNull
  @Override
  public GenericType<UUID> getJavaType() {
    return GenericType.UUID;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.UUID;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof UUID;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == UUID.class;
  }

  @Nullable
  @Override
  public ByteBuffer encode(@Nullable UUID value, @NonNull ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    }
    ByteBuffer bytes = ByteBuffer.allocate(16);
    bytes.putLong(0, value.getMostSignificantBits());
    bytes.putLong(8, value.getLeastSignificantBits());
    return bytes;
  }

  @Nullable
  @Override
  public UUID decode(@Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    } else if (bytes.remaining() != 16) {
      throw new IllegalArgumentException(
          "Unexpected number of bytes for a UUID, expected 16, got " + bytes.remaining());
    } else {
      return new UUID(bytes.getLong(bytes.position()), bytes.getLong(bytes.position() + 8));
    }
  }

  @NonNull
  @Override
  public String format(@Nullable UUID value) {
    return (value == null) ? "NULL" : value.toString();
  }

  @Nullable
  @Override
  public UUID parse(@Nullable String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : UUID.fromString(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse UUID value from \"%s\"", value), e);
    }
  }
}
