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
package com.datastax.oss.driver.internal.core.protocol;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Segment;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.ArrayList;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts the segments decoded by {@link BytesToSegmentDecoder} into legacy frames understood by
 * the rest of the driver.
 */
@NotThreadSafe
public class SegmentToFrameDecoder extends MessageToMessageDecoder<Segment<ByteBuf>> {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentToFrameDecoder.class);

  private static final int UNKNOWN_LENGTH = Integer.MIN_VALUE;

  private final FrameCodec<ByteBuf> frameCodec;
  private final String logPrefix;

  // Accumulated state when we are reading a sequence of slices
  private int targetLength = UNKNOWN_LENGTH;
  private final List<ByteBuf> accumulatedSlices = new ArrayList<>();
  private int accumulatedLength;

  public SegmentToFrameDecoder(@NonNull FrameCodec<ByteBuf> frameCodec, @NonNull String logPrefix) {
    this.logPrefix = logPrefix;
    this.frameCodec = frameCodec;
  }

  @Override
  protected void decode(
      @NonNull ChannelHandlerContext ctx,
      @NonNull Segment<ByteBuf> segment,
      @NonNull List<Object> out) {
    if (segment.isSelfContained) {
      decodeSelfContained(segment, out);
    } else {
      decodeSlice(segment, ctx.alloc(), out);
    }
  }

  private void decodeSelfContained(Segment<ByteBuf> segment, List<Object> out) {
    ByteBuf payload = segment.payload;
    int frameCount = 0;
    try {
      do {
        Frame frame = frameCodec.decode(payload);
        LOG.trace(
            "[{}] Decoded response frame {} from self-contained segment",
            logPrefix,
            frame.streamId);
        out.add(frame);
        frameCount += 1;
      } while (payload.isReadable());
    } finally {
      payload.release();
    }
    LOG.trace("[{}] Done processing self-contained segment ({} frames)", logPrefix, frameCount);
  }

  private void decodeSlice(Segment<ByteBuf> segment, ByteBufAllocator allocator, List<Object> out) {
    assert targetLength != UNKNOWN_LENGTH ^ (accumulatedSlices.isEmpty() && accumulatedLength == 0);
    ByteBuf slice = segment.payload;
    if (targetLength == UNKNOWN_LENGTH) {
      // First slice, read ahead to find the target length
      targetLength = FrameCodec.V3_ENCODED_HEADER_SIZE + frameCodec.decodeBodySize(slice);
    }
    accumulatedSlices.add(slice);
    accumulatedLength += slice.readableBytes();
    int accumulatedSlicesSize = accumulatedSlices.size();
    LOG.trace(
        "[{}] Decoded slice {}, {}/{} bytes",
        logPrefix,
        accumulatedSlicesSize,
        accumulatedLength,
        targetLength);
    assert accumulatedLength <= targetLength;
    if (accumulatedLength == targetLength) {
      // We've received enough data to reassemble the whole message
      CompositeByteBuf encodedFrame = allocator.compositeBuffer(accumulatedSlicesSize);
      encodedFrame.addComponents(true, accumulatedSlices);
      Frame frame;
      try {
        frame = frameCodec.decode(encodedFrame);
      } finally {
        encodedFrame.release();
        // Reset our state
        targetLength = UNKNOWN_LENGTH;
        accumulatedSlices.clear();
        accumulatedLength = 0;
      }
      LOG.trace(
          "[{}] Decoded response frame {} from {} slices",
          logPrefix,
          frame.streamId,
          accumulatedSlicesSize);
      out.add(frame);
    }
  }
}
