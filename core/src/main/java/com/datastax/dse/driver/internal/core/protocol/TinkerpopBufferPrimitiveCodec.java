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
package com.datastax.dse.driver.internal.core.protocol;

import com.datastax.dse.driver.internal.core.graph.binary.buffer.DseNettyBufferFactory;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.shaded.guava.common.base.Charsets;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.CRC32;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;

/**
 * Minimal implementation of {@link PrimitiveCodec} for Tinkerpop {@link Buffer} instances.
 *
 * <p>This approach represents a temporary design compromise. PrimitiveCodec is primarily used for
 * handling data directly from Netty, a task satisfied by {@link ByteBufPrimitiveCodec}. But
 * PrimitiveCodec is also used to implement graph serialization for some of the "dynamic" types
 * (notably UDTs and tuples). Since we're converting graph serialization to use the new Tinkerpop
 * Buffer API we need just enough of a PrimitiveCodec impl to satisfy the needs of graph
 * serialization... and nothing more.
 *
 * <p>A more explicit approach would be to change graph serialization to use a different interface,
 * some kind of subset of PrimitiveCodec.... and then make PrimitiveCodec extend this interface.
 * This is left as future work for now since it involves changes to the native-protocol lib(s).
 */
public class TinkerpopBufferPrimitiveCodec implements PrimitiveCodec<Buffer> {

  private final DseNettyBufferFactory factory;

  public TinkerpopBufferPrimitiveCodec(DseNettyBufferFactory factory) {
    this.factory = factory;
  }

  @Override
  public Buffer allocate(int size) {
    // Note: we use io() here to match up to what ByteBufPrimitiveCodec does, but be warned that
    // ByteBufs created in this way don't support the array() method used elsewhere in this codec
    // (readString() specifically).  As such usage of this method to create Buffer instances is
    // discouraged; we have a factory for that.
    return this.factory.io(size, size);
  }

  @Override
  public void release(Buffer toRelease) {
    toRelease.release();
  }

  @Override
  public int sizeOf(Buffer toMeasure) {
    return toMeasure.readableBytes();
  }

  // TODO
  @Override
  public Buffer concat(Buffer left, Buffer right) {
    boolean leftReadable = left.readableBytes() > 0;
    boolean rightReadable = right.readableBytes() > 0;
    if (!(leftReadable || rightReadable)) {
      return factory.heap();
    }
    if (!leftReadable) {
      return right;
    }
    if (!rightReadable) {
      return left;
    }
    Buffer rv = factory.composite(left, right);
    // c.readerIndex() is 0, which is the first readable byte in left
    rv.writerIndex(
        left.writerIndex() - left.readerIndex() + right.writerIndex() - right.readerIndex());
    return rv;
  }

  @Override
  public void markReaderIndex(Buffer source) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetReaderIndex(Buffer source) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte readByte(Buffer source) {
    return source.readByte();
  }

  @Override
  public int readInt(Buffer source) {
    return source.readInt();
  }

  @Override
  public int readInt(Buffer source, int offset) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InetAddress readInetAddr(Buffer source) {
    int length = readByte(source) & 0xFF;
    byte[] bytes = new byte[length];
    source.readBytes(bytes);
    return newInetAddress(bytes);
  }

  @Override
  public long readLong(Buffer source) {
    return source.readLong();
  }

  @Override
  public int readUnsignedShort(Buffer source) {
    return source.readShort() & 0xFFFF;
  }

  @Override
  public ByteBuffer readBytes(Buffer source) {
    int length = readInt(source);
    if (length < 0) return null;
    return source.nioBuffer(source.readerIndex(), length);
  }

  @Override
  public byte[] readShortBytes(Buffer source) {
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

  // Copy of PrimitiveCodec<ByteBuf> impl
  @Override
  public String readString(Buffer source) {
    int length = readUnsignedShort(source);
    return readString(source, length);
  }

  @Override
  public String readLongString(Buffer source) {
    int length = readInt(source);
    return readString(source, length);
  }

  @Override
  public Buffer readRetainedSlice(Buffer source, int sliceLength) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateCrc(Buffer source, CRC32 crc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeByte(byte b, Buffer dest) {
    dest.writeByte(b);
  }

  @Override
  public void writeInt(int i, Buffer dest) {
    dest.writeInt(i);
  }

  @Override
  public void writeInetAddr(InetAddress address, Buffer dest) {
    byte[] bytes = address.getAddress();
    writeByte((byte) bytes.length, dest);
    dest.writeBytes(bytes);
  }

  @Override
  public void writeLong(long l, Buffer dest) {
    dest.writeLong(l);
  }

  @Override
  public void writeUnsignedShort(int i, Buffer dest) {
    dest.writeShort(i);
  }

  // Copy of PrimitiveCodec<ByteBuf> impl
  @Override
  public void writeString(String s, Buffer dest) {

    byte[] bytes = s.getBytes(Charsets.UTF_8);
    writeUnsignedShort(bytes.length, dest);
    dest.writeBytes(bytes);
  }

  @Override
  public void writeLongString(String s, Buffer dest) {
    byte[] bytes = s.getBytes(Charsets.UTF_8);
    writeInt(bytes.length, dest);
    dest.writeBytes(bytes);
  }

  @Override
  public void writeBytes(ByteBuffer bytes, Buffer dest) {
    if (bytes == null) {
      writeInt(-1, dest);
    } else {
      writeInt(bytes.remaining(), dest);
      dest.writeBytes(bytes.duplicate());
    }
  }

  @Override
  public void writeBytes(byte[] bytes, Buffer dest) {
    if (bytes == null) {
      writeInt(-1, dest);
    } else {
      writeInt(bytes.length, dest);
      dest.writeBytes(bytes);
    }
  }

  @Override
  public void writeShortBytes(byte[] bytes, Buffer dest) {
    writeUnsignedShort(bytes.length, dest);
    dest.writeBytes(bytes);
  }

  // Based on PrimitiveCodec<ByteBuf> impl, although that method leverages some
  // Netty built-ins which we have to do manually here
  private static String readString(Buffer buff, int length) {
    try {

      // Basically what io.netty.buffer.ByteBufUtil.decodeString() does minus some extra
      // ByteBuf-specific ops
      int offset;
      byte[] bytes;
      ByteBuffer byteBuff = buff.nioBuffer();
      if (byteBuff.hasArray()) {

        bytes = byteBuff.array();
        offset = byteBuff.arrayOffset();
      } else {

        bytes = new byte[length];
        byteBuff.get(bytes, 0, length);
        offset = 0;
      }

      String str = new String(bytes, offset, length, Charsets.UTF_8);

      // Ops against the NIO buffers don't impact the read/write indexes for he Buffer
      // itself so we have to do that manually
      buff.readerIndex(buff.readerIndex() + length);
      return str;
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException(
          "Not enough bytes to read an UTF-8 serialized string of size " + length, e);
    }
  }

  // TODO: Code below copied directly from ByteBufPrimitiveCodec, probably want to consolidate this
  // somewhere
  private static InetAddress newInetAddress(byte[] bytes) {
    try {
      return InetAddress.getByAddress(bytes);
    } catch (UnknownHostException e) {
      // Per the Javadoc, the only way this can happen is if the length is illegal
      throw new IllegalArgumentException(
          String.format("Invalid address length: %d (%s)", bytes.length, Arrays.toString(bytes)));
    }
  }
}
