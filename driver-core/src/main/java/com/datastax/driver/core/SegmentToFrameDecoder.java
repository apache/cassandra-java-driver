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

import com.datastax.driver.core.Frame.Header;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts the segments decoded by {@link BytesToSegmentDecoder} into legacy frames understood by
 * the rest of the driver.
 */
class SegmentToFrameDecoder extends MessageToMessageDecoder<Segment> {

  private static final Logger logger = LoggerFactory.getLogger(SegmentToFrameDecoder.class);

  // Accumulated state when we are reading a sequence of slices
  private Header pendingHeader;
  private final List<ByteBuf> accumulatedSlices = new ArrayList<ByteBuf>();
  private int accumulatedLength;

  SegmentToFrameDecoder() {
    super(Segment.class);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, Segment segment, List<Object> out) {
    if (segment.isSelfContained()) {
      decodeSelfContained(segment, out);
    } else {
      decodeSlice(segment, ctx.alloc(), out);
    }
  }

  private void decodeSelfContained(Segment segment, List<Object> out) {
    ByteBuf payload = segment.getPayload();
    int frameCount = 0;
    do {
      Header header = Header.decode(payload);
      ByteBuf body = payload.readSlice(header.bodyLength);
      body.retain();
      out.add(new Frame(header, body));
      frameCount += 1;
    } while (payload.isReadable());
    payload.release();
    logger.trace("Decoded self-contained segment into {} frame(s)", frameCount);
  }

  private void decodeSlice(Segment segment, ByteBufAllocator allocator, List<Object> out) {
    assert pendingHeader != null ^ (accumulatedSlices.isEmpty() && accumulatedLength == 0);
    ByteBuf payload = segment.getPayload();
    if (pendingHeader == null) { // first slice
      pendingHeader = Header.decode(payload); // note: this consumes the header data
    }
    accumulatedSlices.add(payload);
    accumulatedLength += payload.readableBytes();
    logger.trace(
        "StreamId {}: decoded slice {}, {}/{} bytes",
        pendingHeader.streamId,
        accumulatedSlices.size(),
        accumulatedLength,
        pendingHeader.bodyLength);
    assert accumulatedLength <= pendingHeader.bodyLength;
    if (accumulatedLength == pendingHeader.bodyLength) {
      // We've received enough data to reassemble the whole message
      CompositeByteBuf body = allocator.compositeBuffer(accumulatedSlices.size());
      body.addComponents(true, accumulatedSlices);
      out.add(new Frame(pendingHeader, body));
      // Reset our state
      pendingHeader = null;
      accumulatedSlices.clear();
      accumulatedLength = 0;
    }
  }
}
