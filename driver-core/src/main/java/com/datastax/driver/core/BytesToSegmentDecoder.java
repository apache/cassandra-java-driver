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

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.nio.ByteOrder;

/**
 * Decodes {@link Segment}s from a stream of bytes.
 *
 * <p>This works like a regular length-field-based decoder, but we override {@link
 * #getUnadjustedFrameLength} to handle two peculiarities: the length is encoded on 17 bits, and we
 * also want to check the header CRC before we use it. So we parse the whole segment header ahead of
 * time, and store it until we're ready to build the segment.
 */
class BytesToSegmentDecoder extends LengthFieldBasedFrameDecoder {

  private final SegmentCodec segmentCodec;
  private SegmentCodec.Header header;

  BytesToSegmentDecoder(SegmentCodec segmentCodec) {
    super(
        // max length (Netty wants this to be the overall length including everything):
        segmentCodec.headerLength()
            + SegmentCodec.CRC24_LENGTH
            + Segment.MAX_PAYLOAD_LENGTH
            + SegmentCodec.CRC32_LENGTH,
        // offset and size of the "length" field: that's the whole header
        0,
        segmentCodec.headerLength() + SegmentCodec.CRC24_LENGTH,
        // length adjustment: add the trailing CRC to the declared length
        SegmentCodec.CRC32_LENGTH,
        // bytes to skip: the header (we've already parsed it while reading the length)
        segmentCodec.headerLength() + SegmentCodec.CRC24_LENGTH);
    this.segmentCodec = segmentCodec;
  }

  @Override
  protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
    try {
      ByteBuf payloadAndCrc = (ByteBuf) super.decode(ctx, in);
      if (payloadAndCrc == null) {
        return null;
      } else {
        assert header != null;
        Segment segment = segmentCodec.decode(header, payloadAndCrc);
        header = null;
        return segment;
      }
    } catch (Exception e) {
      // Don't hold on to a stale header if we failed to decode the rest of the segment
      header = null;
      throw e;
    }
  }

  @Override
  protected long getUnadjustedFrameLength(ByteBuf buffer, int offset, int length, ByteOrder order) {
    // The parent class calls this repeatedly for the same "frame" if there weren't enough
    // accumulated bytes the first time. Only decode the header the first time:
    if (header == null) {
      header = segmentCodec.decodeHeader(buffer.slice(offset, length));
    }
    return header.payloadLength;
  }
}
