/*
 * Copyright DataStax, Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
