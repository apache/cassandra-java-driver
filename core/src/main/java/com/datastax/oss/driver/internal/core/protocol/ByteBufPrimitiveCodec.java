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
package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class ByteBufPrimitiveCodec implements PrimitiveCodec<ByteBuf> {

  private final ByteBufAllocator allocator;

  public ByteBufPrimitiveCodec(ByteBufAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public ByteBuf allocate(int size) {
    return allocator.ioBuffer(size, size);
  }

  @Override
  public void release(ByteBuf toRelease) {
    toRelease.release();
  }

  @Override
  public int sizeOf(ByteBuf toMeasure) {
    return toMeasure.readableBytes();
  }

  @Override
  public ByteBuf concat(ByteBuf left, ByteBuf right) {
    if (!left.isReadable()) {
      return right.duplicate();
    } else if (!right.isReadable()) {
      return left.duplicate();
    } else {
      CompositeByteBuf c = allocator.compositeBuffer(2);
      c.addComponents(left, right);
      // c.readerIndex() is 0, which is the first readable byte in left
      c.writerIndex(
          left.writerIndex() - left.readerIndex() + right.writerIndex() - right.readerIndex());
      return c;
    }
  }

  @Override
  public void markReaderIndex(ByteBuf source) {
    source.markReaderIndex();
  }

  @Override
  public void resetReaderIndex(ByteBuf source) {
    source.resetReaderIndex();
  }

  @Override
  public byte readByte(ByteBuf source) {
    return source.readByte();
  }

  @Override
  public int readInt(ByteBuf source) {
    return source.readInt();
  }

  @Override
  public int readInt(ByteBuf source, int offset) {
    return source.getInt(source.readerIndex() + offset);
  }

  @Override
  public InetAddress readInetAddr(ByteBuf source) {
    int length = readByte(source) & 0xFF;
    byte[] bytes = new byte[length];
    source.readBytes(bytes);
    return newInetAddress(bytes);
  }

  @Override
  public long readLong(ByteBuf source) {
    return source.readLong();
  }

  @Override
  public int readUnsignedShort(ByteBuf source) {
    return source.readUnsignedShort();
  }

  @Override
  public ByteBuffer readBytes(ByteBuf source) {
    int length = readInt(source);
    if (length < 0) return null;
    ByteBuf slice = source.readSlice(length);
    return ByteBuffer.wrap(readRawBytes(slice));
  }

  @Override
  public byte[] readShortBytes(ByteBuf source) {
    try {
      int length = readUnsignedShort(source);
      byte[] bytes = new byte[length];
      source.readBytes(bytes);
      return bytes;
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(
          "Not enough bytes to read a byte array preceded by its 2 bytes length");
    }
  }

  @Override
  public String readString(ByteBuf source) {
    int length = readUnsignedShort(source);
    return readString(source, length);
  }

  @Override
  public String readLongString(ByteBuf source) {
    int length = readInt(source);
    return readString(source, length);
  }

  @Override
  public ByteBuf readRetainedSlice(ByteBuf source, int sliceLength) {
    return source.readRetainedSlice(sliceLength);
  }

  @Override
  public void updateCrc(ByteBuf source, CRC32 crc) {
    crc.update(source.internalNioBuffer(source.readerIndex(), source.readableBytes()));
  }

  @Override
  public void writeByte(byte b, ByteBuf dest) {
    dest.writeByte(b);
  }

  @Override
  public void writeInt(int i, ByteBuf dest) {
    dest.writeInt(i);
  }

  @Override
  public void writeInetAddr(InetAddress inetAddr, ByteBuf dest) {
    byte[] bytes = inetAddr.getAddress();
    writeByte((byte) bytes.length, dest);
    dest.writeBytes(bytes);
  }

  @Override
  public void writeLong(long l, ByteBuf dest) {
    dest.writeLong(l);
  }

  @Override
  public void writeUnsignedShort(int i, ByteBuf dest) {
    dest.writeShort(i);
  }

  @Override
  public void writeString(String s, ByteBuf dest) {
    byte[] bytes = s.getBytes(CharsetUtil.UTF_8);
    writeUnsignedShort(bytes.length, dest);
    dest.writeBytes(bytes);
  }

  @Override
  public void writeLongString(String s, ByteBuf dest) {
    byte[] bytes = s.getBytes(CharsetUtil.UTF_8);
    writeInt(bytes.length, dest);
    dest.writeBytes(bytes);
  }

  @Override
  public void writeBytes(ByteBuffer bytes, ByteBuf dest) {
    if (bytes == null) {
      writeInt(-1, dest);
    } else {
      writeInt(bytes.remaining(), dest);
      dest.writeBytes(bytes.duplicate());
    }
  }

  @Override
  public void writeBytes(byte[] bytes, ByteBuf dest) {
    if (bytes == null) {
      writeInt(-1, dest);
    } else {
      writeInt(bytes.length, dest);
      dest.writeBytes(bytes);
    }
  }

  @Override
  public void writeShortBytes(byte[] bytes, ByteBuf dest) {
    writeUnsignedShort(bytes.length, dest);
    dest.writeBytes(bytes);
  }

  // Reads *all* readable bytes from a buffer and return them.
  // If the buffer is backed by an array, this will return the underlying array directly, without
  // copy.
  private static byte[] readRawBytes(ByteBuf buffer) {
    if (buffer.hasArray() && buffer.readableBytes() == buffer.array().length) {
      // Move the readerIndex just so we consistently consume the input
      buffer.readerIndex(buffer.writerIndex());
      return buffer.array();
    }

    // Otherwise, just read the bytes in a new array
    byte[] bytes = new byte[buffer.readableBytes()];
    buffer.readBytes(bytes);
    return bytes;
  }

  private static String readString(ByteBuf source, int length) {
    try {
      String str = source.toString(source.readerIndex(), length, CharsetUtil.UTF_8);
      source.readerIndex(source.readerIndex() + length);
      return str;
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(
          "Not enough bytes to read an UTF-8 serialized string of size " + length, e);
    }
  }

  private InetAddress newInetAddress(byte[] bytes) {
    try {
      return InetAddress.getByAddress(bytes);
    } catch (UnknownHostException e) {
      // Per the Javadoc, the only way this can happen is if the length is illegal
      throw new IllegalArgumentException(
          String.format("Invalid address length: %d (%s)", bytes.length, Arrays.toString(bytes)));
    }
  }
}
