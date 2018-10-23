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
import com.datastax.oss.driver.api.core.type.codec.PrimitiveBooleanCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class BooleanCodec implements PrimitiveBooleanCodec {

  private static final ByteBuffer TRUE = ByteBuffer.wrap(new byte[] {1});
  private static final ByteBuffer FALSE = ByteBuffer.wrap(new byte[] {0});

  @NonNull
  @Override
  public GenericType<Boolean> getJavaType() {
    return GenericType.BOOLEAN;
  }

  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.BOOLEAN;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof Boolean;
  }

  @Override
  public boolean accepts(@NonNull Class<?> javaClass) {
    return javaClass == Boolean.class || javaClass == boolean.class;
  }

  @Nullable
  @Override
  public ByteBuffer encodePrimitive(boolean value, @NonNull ProtocolVersion protocolVersion) {
    return value ? TRUE.duplicate() : FALSE.duplicate();
  }

  @Override
  public boolean decodePrimitive(
      @Nullable ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return false;
    } else if (bytes.remaining() != 1) {
      throw new IllegalArgumentException(
          "Invalid boolean value, expecting 1 byte but got " + bytes.remaining());
    } else {
      return bytes.get(bytes.position()) != 0;
    }
  }

  @NonNull
  @Override
  public String format(@Nullable Boolean value) {
    if (value == null) {
      return "NULL";
    } else {
      return value ? "true" : "false";
    }
  }

  @Nullable
  @Override
  public Boolean parse(@Nullable String value) {
    if (value == null || value.isEmpty() || value.equalsIgnoreCase("NULL")) {
      return null;
    } else if (value.equalsIgnoreCase(Boolean.FALSE.toString())) {
      return false;
    } else if (value.equalsIgnoreCase(Boolean.TRUE.toString())) {
      return true;
    } else {
      throw new IllegalArgumentException(
          String.format("Cannot parse boolean value from \"%s\"", value));
    }
  }
}
