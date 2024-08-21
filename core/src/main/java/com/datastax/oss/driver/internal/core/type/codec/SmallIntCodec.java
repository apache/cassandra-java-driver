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
import com.datastax.oss.driver.api.core.type.codec.PrimitiveShortCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Optional;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class SmallIntCodec implements PrimitiveShortCodec {
  @NonNull
  @Override
  public GenericType<Short> getJavaType() {
    return GenericType.SHORT;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.SMALLINT;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof Short;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == Short.class || javaClass == short.class;
  }

  @Nullable
  @Override
  public ByteBuffer encodePrimitive(short value, @NonNull ProtocolVersion protocolVersion) {
    ByteBuffer bytes = ByteBuffer.allocate(2);
    bytes.putShort(0, value);
    return bytes;
  }

  @Override
  public short decodePrimitive(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return 0;
    } else if (bytes.remaining() != 2) {
      throw new IllegalArgumentException(
          "Invalid 16-bits integer value, expecting 2 bytes but got " + bytes.remaining());
    } else {
      return bytes.getShort(bytes.position());
    }
  }

  @NonNull
  @Override
  public String format(@Nullable Short value) {
    return (value == null) ? "NULL" : Short.toString(value);
  }

  @Nullable
  @Override
  public Short parse(@Nullable String value) {
    try {
      return (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL"))
          ? null
          : Short.parseShort(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Cannot parse 16-bits int value from \"%s\"", value));
    }
  }

  @NonNull
  @Override
  public Optional<Integer> serializedSize() {
    return Optional.absent();
  }
}
