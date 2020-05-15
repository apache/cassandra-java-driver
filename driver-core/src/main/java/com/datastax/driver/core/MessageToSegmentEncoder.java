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
package com.datastax.driver.core;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

class MessageToSegmentEncoder extends ChannelOutboundHandlerAdapter {

  private final ByteBufAllocator allocator;
  private final Message.ProtocolEncoder requestEncoder;

  private SegmentBuilder segmentBuilder;

  MessageToSegmentEncoder(ByteBufAllocator allocator, Message.ProtocolEncoder requestEncoder) {
    this.allocator = allocator;
    this.requestEncoder = requestEncoder;
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    super.handlerAdded(ctx);
    this.segmentBuilder = new SegmentBuilder(ctx, allocator, requestEncoder);
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof Message.Request) {
      segmentBuilder.addRequest(((Message.Request) msg), promise);
    } else {
      super.write(ctx, msg, promise);
    }
  }

  @Override
  public void flush(ChannelHandlerContext ctx) throws Exception {
    segmentBuilder.flush();
    super.flush(ctx);
  }
}
