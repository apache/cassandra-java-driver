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
import java.nio.ByteBuffer;
import java.util.UUID;

public class TimeUuidCodec extends UuidCodec {
  @Override
  public DataType getCqlType() {
    return DataTypes.TIMEUUID;
  }

  @Override
  public boolean canEncode(Object value) {
    return value instanceof UUID && ((UUID) value).version() == 1;
  }

  @Override
  public boolean canEncode(Class<?> javaClass) {
    return javaClass == UUID.class;
  }

  @Override
  public ByteBuffer encode(UUID value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    } else if (value.version() != 1) {
      throw new IllegalArgumentException(
          String.format("%s is not a Type 1 (time-based) UUID", value));
    } else {
      return super.encode(value, protocolVersion);
    }
  }

  @Override
  public String format(UUID value) {
    if (value == null) {
      return "NULL";
    } else if (value.version() != 1) {
      throw new IllegalArgumentException(
          String.format("%s is not a Type 1 (time-based) UUID", value));
    } else {
      return super.format(value);
    }
  }
}
