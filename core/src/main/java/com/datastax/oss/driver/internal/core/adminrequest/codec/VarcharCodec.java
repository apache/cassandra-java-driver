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
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.base.Charsets;
import java.nio.ByteBuffer;

public class VarcharCodec implements TypeCodec<String> {

  @Override
  public ByteBuffer encode(String value, ProtocolVersion protocolVersion) {
    return (value == null) ? null : ByteBuffer.wrap(value.getBytes(Charsets.UTF_8));
  }

  @Override
  public String decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
    if (bytes == null) {
      return null;
    } else if (bytes.remaining() == 0) {
      return "";
    } else {
      return new String(Bytes.getArray(bytes), Charsets.UTF_8);
    }
  }
}
