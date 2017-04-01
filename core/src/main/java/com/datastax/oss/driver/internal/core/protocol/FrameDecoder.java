/*
 * Copyright (C) 2017-2017 DataStax Inc.
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

import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class FrameDecoder extends LengthFieldBasedFrameDecoder {

  // Where the length of the frame is located in the payload
  private static final int LENGTH_FIELD_OFFSET = 5;
  private static final int LENGTH_FIELD_LENGTH = 4;

  private final FrameCodec<ByteBuf> frameCodec;

  public FrameDecoder(FrameCodec<ByteBuf> frameCodec, int maxFrameLengthInBytes) {
    super(maxFrameLengthInBytes, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, 0, 0, true);
    this.frameCodec = frameCodec;
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
    int startIndex = in.readerIndex();
    try {
      ByteBuf buffer = (ByteBuf) super.decode(ctx, in);
      return (buffer == null)
          ? null // did not receive whole frame yet, keep reading
          : frameCodec.decode(buffer);
    } catch (Exception e) {
      // If decoding failed, try to read at least the stream id, so that the error can be
      // propagated to the client request matching that id (otherwise we have to fail all
      // pending requests on this channel)
      int streamId;
      try {
        streamId = in.getShort(startIndex + 2);
      } catch (Exception e1) {
        // Should never happen, super.decode does not return a non-null buffer until the length
        // field has been read, and the stream id comes before
        streamId = -1;
        // TODO log e1?
      }
      throw new FrameDecodingException(streamId, e);
    }
  }
}
