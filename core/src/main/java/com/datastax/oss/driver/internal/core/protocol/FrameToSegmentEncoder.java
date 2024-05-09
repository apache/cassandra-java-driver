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

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class FrameToSegmentEncoder extends ChannelOutboundHandlerAdapter {

  private final PrimitiveCodec<ByteBuf> primitiveCodec;
  private final FrameCodec<ByteBuf> frameCodec;
  private final String logPrefix;

  private ByteBufSegmentBuilder segmentBuilder;

  public FrameToSegmentEncoder(
      @NonNull PrimitiveCodec<ByteBuf> primitiveCodec,
      @NonNull FrameCodec<ByteBuf> frameCodec,
      @NonNull String logPrefix) {
    this.primitiveCodec = primitiveCodec;
    this.frameCodec = frameCodec;
    this.logPrefix = logPrefix;
  }

  @Override
  public void handlerAdded(@NonNull ChannelHandlerContext ctx) {
    segmentBuilder = new ByteBufSegmentBuilder(ctx, primitiveCodec, frameCodec, logPrefix);
  }

  @Override
  public void write(
      @NonNull ChannelHandlerContext ctx, @NonNull Object msg, @NonNull ChannelPromise promise)
      throws Exception {
    if (msg instanceof Frame) {
      segmentBuilder.addFrame(((Frame) msg), promise);
    } else {
      super.write(ctx, msg, promise);
    }
  }

  @Override
  public void flush(@NonNull ChannelHandlerContext ctx) throws Exception {
    segmentBuilder.flush();
    super.flush(ctx);
  }
}
