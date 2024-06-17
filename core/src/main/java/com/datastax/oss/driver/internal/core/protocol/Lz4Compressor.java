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

import static com.datastax.oss.driver.internal.core.util.Dependency.LZ4;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.internal.core.util.DefaultDependencyChecker;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import net.jcip.annotations.ThreadSafe;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class Lz4Compressor extends ByteBufCompressor {

  private static final Logger LOG = LoggerFactory.getLogger(Lz4Compressor.class);

  private final LZ4Compressor compressor;
  private final LZ4FastDecompressor decompressor;

  public Lz4Compressor(DriverContext context) {
    this(context.getSessionName());
  }

  @VisibleForTesting
  Lz4Compressor(String sessionName) {
    if (DefaultDependencyChecker.isPresent(LZ4)) {
      LZ4Factory lz4Factory = LZ4Factory.fastestInstance();
      LOG.info("[{}] Using {}", sessionName, lz4Factory.toString());
      this.compressor = lz4Factory.fastCompressor();
      this.decompressor = lz4Factory.fastDecompressor();
    } else {
      throw new IllegalStateException(
          "Could not find the LZ4 library on the classpath "
              + "(the driver declares it as an optional dependency, "
              + "so you need to declare it explicitly)");
    }
  }

  @Override
  public String algorithm() {
    return "lz4";
  }

  @Override
  protected ByteBuf compressDirect(ByteBuf input, boolean prependWithUncompressedLength) {
    int maxCompressedLength = compressor.maxCompressedLength(input.readableBytes());
    // If the input is direct we will allocate a direct output buffer as well as this will allow us
    // to use LZ4Compressor.compress and so eliminate memory copies.
    ByteBuf output =
        input.alloc().directBuffer((prependWithUncompressedLength ? 4 : 0) + maxCompressedLength);
    try {
      ByteBuffer in = inputNioBuffer(input);
      // Increase reader index.
      input.readerIndex(input.writerIndex());

      if (prependWithUncompressedLength) {
        output.writeInt(in.remaining());
      }

      ByteBuffer out = outputNioBuffer(output);
      int written =
          compressor.compress(
              in, in.position(), in.remaining(), out, out.position(), out.remaining());
      // Set the writer index so the amount of written bytes is reflected
      output.writerIndex(output.writerIndex() + written);
    } catch (Exception e) {
      // release output buffer so we not leak and rethrow exception.
      output.release();
      throw e;
    }
    return output;
  }

  @Override
  protected ByteBuf compressHeap(ByteBuf input, boolean prependWithUncompressedLength) {
    int maxCompressedLength = compressor.maxCompressedLength(input.readableBytes());

    // Not a direct buffer so use byte arrays...
    int inOffset = input.arrayOffset() + input.readerIndex();
    byte[] in = input.array();
    int len = input.readableBytes();
    // Increase reader index.
    input.readerIndex(input.writerIndex());

    // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and
    // so can eliminate the overhead of allocate a new byte[].
    ByteBuf output =
        input.alloc().heapBuffer((prependWithUncompressedLength ? 4 : 0) + maxCompressedLength);
    try {
      if (prependWithUncompressedLength) {
        output.writeInt(len);
      }
      // calculate the correct offset.
      int offset = output.arrayOffset() + output.writerIndex();
      byte[] out = output.array();
      int written = compressor.compress(in, inOffset, len, out, offset);

      // Set the writer index so the amount of written bytes is reflected
      output.writerIndex(output.writerIndex() + written);
    } catch (Exception e) {
      // release output buffer so we not leak and rethrow exception.
      output.release();
      throw e;
    }
    return output;
  }

  @Override
  protected int readUncompressedLength(ByteBuf compressed) {
    return compressed.readInt();
  }

  @Override
  protected ByteBuf decompressDirect(ByteBuf input, int uncompressedLength) {
    // If the input is direct we will allocate a direct output buffer as well as this will allow us
    // to use LZ4Compressor.decompress and so eliminate memory copies.
    int readable = input.readableBytes();
    ByteBuffer in = inputNioBuffer(input);
    // Increase reader index.
    input.readerIndex(input.writerIndex());
    ByteBuf output = input.alloc().directBuffer(uncompressedLength);
    try {
      ByteBuffer out = outputNioBuffer(output);
      int read = decompressor.decompress(in, in.position(), out, out.position(), out.remaining());
      if (read != readable) {
        throw new IllegalArgumentException("Compressed lengths mismatch");
      }

      // Set the writer index so the amount of written bytes is reflected
      output.writerIndex(output.writerIndex() + uncompressedLength);
    } catch (Exception e) {
      // release output buffer so we not leak and rethrow exception.
      output.release();
      throw e;
    }
    return output;
  }

  @Override
  protected ByteBuf decompressHeap(ByteBuf input, int uncompressedLength) {
    // Not a direct buffer so use byte arrays...
    byte[] in = input.array();
    int len = input.readableBytes();
    int inOffset = input.arrayOffset() + input.readerIndex();
    // Increase reader index.
    input.readerIndex(input.writerIndex());

    // Allocate a heap buffer from the ByteBufAllocator as we may use a PooledByteBufAllocator and
    // so can eliminate the overhead of allocate a new byte[].
    ByteBuf output = input.alloc().heapBuffer(uncompressedLength);
    try {
      int offset = output.arrayOffset() + output.writerIndex();
      byte[] out = output.array();
      int read = decompressor.decompress(in, inOffset, out, offset, uncompressedLength);
      if (read != len) {
        throw new IllegalArgumentException("Compressed lengths mismatch");
      }

      // Set the writer index so the amount of written bytes is reflected
      output.writerIndex(output.writerIndex() + uncompressedLength);
    } catch (Exception e) {
      // release output buffer so we not leak and rethrow exception.
      output.release();
      throw e;
    }
    return output;
  }
}
