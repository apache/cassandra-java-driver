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
package com.datastax.oss.driver.internal.core.util;

import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.nio.ByteBuffer;

/** Helper class to create {@link io.netty.buffer.ByteBuf} instances in tests. */
public class ByteBufs {
  public static ByteBuf wrap(int... bytes) {
    ByteBuf bb = ByteBufAllocator.DEFAULT.buffer(bytes.length);
    for (int b : bytes) {
      bb.writeByte(b);
    }
    return bb;
  }

  public static ByteBuf fromHexString(String hexString) {
    ByteBuffer tmp = Bytes.fromHexString(hexString);
    ByteBuf target = ByteBufAllocator.DEFAULT.buffer(tmp.remaining());
    target.writeBytes(tmp);
    return target;
  }
}
