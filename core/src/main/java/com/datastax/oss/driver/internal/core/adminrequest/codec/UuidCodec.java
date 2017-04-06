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
package com.datastax.oss.driver.internal.core.adminrequest.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidCodec implements TypeCodec<UUID> {

  @Override
  public ByteBuffer encode(UUID value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    } else {
      ByteBuffer bytes = ByteBuffer.allocate(16);
      bytes.putLong(0, value.getMostSignificantBits());
      bytes.putLong(8, value.getLeastSignificantBits());
      return bytes;
    }
  }

  @Override
  public UUID decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    return (bytes == null || bytes.remaining() == 0)
        ? null
        : new UUID(bytes.getLong(bytes.position()), bytes.getLong(bytes.position() + 8));
  }
}
