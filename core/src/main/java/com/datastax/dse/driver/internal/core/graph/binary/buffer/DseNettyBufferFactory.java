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
package com.datastax.dse.driver.internal.core.graph.binary.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.ByteBuffer;
import java.util.function.Supplier;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.BufferFactory;

/**
 * Internal BufferFactory impl for creation of Tinkerpop buffers. We implement an internal type here
 * to allow for this class to use shaded Netty types (without bringing all of Tinkerpop into the
 * shaded JAR). The impl is based on the initial impl of {@code
 * org.apache.tinkerpop.gremlin.driver.ser.NettyBufferFactory} but we don't guarantee that this
 * class will mirror changes to that class over time.
 */
public class DseNettyBufferFactory implements BufferFactory<ByteBuf> {

  private static final ByteBufAllocator DEFAULT_ALLOCATOR = new UnpooledByteBufAllocator(false);

  private final ByteBufAllocator allocator;

  public DseNettyBufferFactory() {
    this.allocator = DEFAULT_ALLOCATOR;
  }

  public DseNettyBufferFactory(ByteBufAllocator allocator) {
    this.allocator = allocator;
  }

  @Override
  public Buffer create(final ByteBuf value) {
    return new DseNettyBuffer(value);
  }

  @Override
  public Buffer wrap(final ByteBuffer value) {
    return create(Unpooled.wrappedBuffer(value));
  }

  public Buffer heap() {
    return create(allocator.heapBuffer());
  }

  public Buffer heap(int initialSize) {
    return create(allocator.heapBuffer(initialSize));
  }

  public Buffer heap(int initialSize, int maxSize) {
    return create(allocator.heapBuffer(initialSize, maxSize));
  }

  public Buffer io() {
    return create(allocator.ioBuffer());
  }

  public Buffer io(int initialSize) {
    return create(allocator.ioBuffer(initialSize));
  }

  public Buffer io(int initialSize, int maxSize) {
    return create(allocator.ioBuffer(initialSize, maxSize));
  }

  public Buffer direct() {
    return create(allocator.directBuffer());
  }

  public Buffer direct(int initialSize) {
    return create(allocator.directBuffer(initialSize));
  }

  public Buffer direct(int initialSize, int maxSize) {
    return create(allocator.directBuffer(initialSize, maxSize));
  }

  public Buffer composite(ByteBuf... components) {

    CompositeByteBuf buff = allocator.compositeBuffer(components.length);
    buff.addComponents(components);
    return create(buff);
  }

  public Buffer composite(Buffer... components) {
    ByteBuf[] nettyBufs = new ByteBuf[components.length];
    for (int i = 0; i < components.length; ++i) {
      if (!(components[i] instanceof DseNettyBuffer)) {
        throw new IllegalArgumentException("Can only concatenate DseNettyBuffer instances");
      }
      nettyBufs[i] = ((DseNettyBuffer) components[i]).getUnderlyingBuffer();
    }
    return composite(nettyBufs);
  }

  public Buffer withBytes(int... bytes) {
    return withBytes(this::heap, bytes);
  }

  public Buffer withBytes(Supplier<Buffer> supplier, int... bytes) {
    Buffer buff = supplier.get();
    for (int val : bytes) {
      buff.writeByte(val);
    }
    return buff;
  }
}
