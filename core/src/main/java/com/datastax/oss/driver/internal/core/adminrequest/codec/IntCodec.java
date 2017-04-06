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

public class IntCodec implements TypeCodec<Integer> {

  @Override
  public ByteBuffer encode(Integer value, ProtocolVersion protocolVersion) {
    if (value == null) {
      return null;
    } else {
      ByteBuffer bytes = ByteBuffer.allocate(4);
      bytes.putInt(0, value);
      return bytes;
    }
  }

  @Override
  public Integer decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null || bytes.remaining() == 0) {
      return null;
    } else if (bytes.remaining() != 4) {
      throw new IllegalArgumentException(
          "Invalid 32-bits integer value, expecting 4 bytes but got " + bytes.remaining());
    } else {
      return bytes.getInt(bytes.position());
    }
  }
}
