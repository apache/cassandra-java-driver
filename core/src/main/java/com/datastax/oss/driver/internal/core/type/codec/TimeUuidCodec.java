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
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class TimeUuidCodec extends UuidCodec {
  @NonNull
  @Override
  public DataType getCqlType() {
    return DataTypes.TIMEUUID;
  }

  @Override
  public boolean accepts(@NonNull Object value) {
    return value instanceof UUID && ((UUID) value).version() == 1;
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
    } else if (value.version() != 1) {
      throw new IllegalArgumentException(
          String.format("%s is not a Type 1 (time-based) UUID", value));
    } else {
      return super.encode(value, protocolVersion);
    }
  }

  @NonNull
  @Override
  public String format(@Nullable UUID value) {
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
