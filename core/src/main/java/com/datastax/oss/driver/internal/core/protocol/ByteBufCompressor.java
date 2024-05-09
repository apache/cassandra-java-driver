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
package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.protocol.internal.Compressor;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public abstract class ByteBufCompressor implements Compressor<ByteBuf> {

  @Override
  public ByteBuf compress(ByteBuf uncompressed) {
    return uncompressed.isDirect()
        ? compressDirect(uncompressed, true)
        : compressHeap(uncompressed, true);
  }

  @Override
  public ByteBuf compressWithoutLength(ByteBuf uncompressed) {
    return uncompressed.isDirect()
        ? compressDirect(uncompressed, false)
        : compressHeap(uncompressed, false);
  }

  protected abstract ByteBuf compressDirect(ByteBuf input, boolean prependWithUncompressedLength);

  protected abstract ByteBuf compressHeap(ByteBuf input, boolean prependWithUncompressedLength);

  @Override
  public ByteBuf decompress(ByteBuf compressed) {
    return decompressWithoutLength(compressed, readUncompressedLength(compressed));
  }

  protected abstract int readUncompressedLength(ByteBuf compressed);

  @Override
  public ByteBuf decompressWithoutLength(ByteBuf compressed, int uncompressedLength) {
    return compressed.isDirect()
        ? decompressDirect(compressed, uncompressedLength)
        : decompressHeap(compressed, uncompressedLength);
  }

  protected abstract ByteBuf decompressDirect(ByteBuf input, int uncompressedLength);

  protected abstract ByteBuf decompressHeap(ByteBuf input, int uncompressedLength);

  protected static ByteBuffer inputNioBuffer(ByteBuf buf) {
    // Using internalNioBuffer(...) as we only hold the reference in this method and so can
    // reduce Object allocations.
    int index = buf.readerIndex();
    int len = buf.readableBytes();
    return buf.nioBufferCount() == 1
        ? buf.internalNioBuffer(index, len)
        : buf.nioBuffer(index, len);
  }

  protected static ByteBuffer outputNioBuffer(ByteBuf buf) {
    int index = buf.writerIndex();
    int len = buf.writableBytes();
    return buf.nioBufferCount() == 1
        ? buf.internalNioBuffer(index, len)
        : buf.nioBuffer(index, len);
  }
}
