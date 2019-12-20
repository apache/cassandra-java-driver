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
package com.datastax.dse.driver.internal.core.graph;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.nio.ByteBuffer;

public class ByteBufUtil {

  // Does not move the reader index of the ByteBuf parameter
  public static ByteBuffer toByteBuffer(ByteBuf buffer) {
    if (buffer.isDirect()) {
      return buffer.nioBuffer();
    }
    final byte[] bytes = new byte[buffer.readableBytes()];
    buffer.getBytes(buffer.readerIndex(), bytes);
    return ByteBuffer.wrap(bytes);
  }

  static ByteBuf toByteBuf(ByteBuffer buffer) {
    return Unpooled.wrappedBuffer(buffer);
  }

  // read a predefined amount of bytes from the netty buffer and move its readerIndex
  public static ByteBuffer readBytes(ByteBuf nettyBuf, int size) {
    ByteBuffer res = ByteBuffer.allocate(size);
    nettyBuf.readBytes(res);
    res.flip();
    return res;
  }
}
