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

import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import net.jcip.annotations.ThreadSafe;

@ChannelHandler.Sharable
@ThreadSafe
public class FrameEncoder extends MessageToMessageEncoder<Frame> {

  private final FrameCodec<ByteBuf> frameCodec;
  private final int maxFrameLength;

  public FrameEncoder(FrameCodec<ByteBuf> frameCodec, int maxFrameLength) {
    super(Frame.class);
    this.frameCodec = frameCodec;
    this.maxFrameLength = maxFrameLength;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
    ByteBuf buffer = frameCodec.encode(frame);
    int actualLength = buffer.readableBytes();
    if (actualLength > maxFrameLength) {
      throw new FrameTooLongException(
          ctx.channel().remoteAddress(),
          String.format("Outgoing frame length exceeds %d: %d", maxFrameLength, actualLength));
    }
    out.add(buffer);
  }
}
