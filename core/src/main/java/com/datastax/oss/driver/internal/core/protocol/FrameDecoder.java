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
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.TooLongFrameException;
import java.util.Collections;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class FrameDecoder extends LengthFieldBasedFrameDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(FrameDecoder.class);

  // Where the length of the frame is located in the payload
  private static final int LENGTH_FIELD_OFFSET = 5;
  private static final int LENGTH_FIELD_LENGTH = 4;

  private final FrameCodec<ByteBuf> frameCodec;
  private boolean isFirstResponse;

  public FrameDecoder(FrameCodec<ByteBuf> frameCodec, int maxFrameLengthInBytes) {
    super(maxFrameLengthInBytes, LENGTH_FIELD_OFFSET, LENGTH_FIELD_LENGTH, 0, 0, true);
    this.frameCodec = frameCodec;
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
    int startIndex = in.readerIndex();
    if (isFirstResponse) {
      isFirstResponse = false;

      // Must read at least the protocol v1/v2 header (see below)
      if (in.readableBytes() < 8) {
        return null;
      }
      // Special case for obsolete protocol versions (< v3): the length field is at a different
      // position, so we can't delegate to super.decode() which would read the wrong length.
      int protocolVersion = (int) in.getByte(startIndex) & 0b0111_1111;
      if (protocolVersion < 3) {
        int streamId = in.getByte(startIndex + 2);
        int length = in.getInt(startIndex + 4);
        // We don't need a full-blown decoder, just to signal the protocol error. So discard the
        // incoming data and spoof a server-side protocol error.
        if (in.readableBytes() < 8 + length) {
          return null; // keep reading until we can discard the whole message at once
        } else {
          in.readerIndex(startIndex + 8 + length);
        }
        return Frame.forResponse(
            protocolVersion,
            streamId,
            null,
            Frame.NO_PAYLOAD,
            Collections.emptyList(),
            new Error(
                ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
                "Invalid or unsupported protocol version"));
      }
    }

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
        Loggers.warnWithException(LOG, "Unexpected error while reading stream id", e1);
        streamId = -1;
      }
      if (e instanceof TooLongFrameException) {
        // Translate the Netty error to our own type
        e = new FrameTooLongException(ctx.channel().remoteAddress(), e.getMessage());
      }
      throw new FrameDecodingException(streamId, e);
    }
  }

  @Override
  protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
    return buffer.slice(index, length);
  }
}
