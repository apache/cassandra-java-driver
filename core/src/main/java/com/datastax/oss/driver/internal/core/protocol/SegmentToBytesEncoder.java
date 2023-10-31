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

import com.datastax.oss.protocol.internal.Segment;
import com.datastax.oss.protocol.internal.SegmentCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;
import javax.annotation.Nonnull;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
@ChannelHandler.Sharable
public class SegmentToBytesEncoder extends MessageToMessageEncoder<Segment<ByteBuf>> {

  private final SegmentCodec<ByteBuf> segmentCodec;

  public SegmentToBytesEncoder(@Nonnull SegmentCodec<ByteBuf> segmentCodec) {
    this.segmentCodec = segmentCodec;
  }

  @Override
  protected void encode(
      @Nonnull ChannelHandlerContext ctx,
      @Nonnull Segment<ByteBuf> segment,
      @Nonnull List<Object> out) {
    segmentCodec.encode(segment, out);
  }
}
