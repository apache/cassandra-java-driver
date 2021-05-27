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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.Segment;
import com.datastax.oss.protocol.internal.SegmentBuilder;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.ArrayList;
import java.util.List;
import net.jcip.annotations.NotThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class ByteBufSegmentBuilder extends SegmentBuilder<ByteBuf, ChannelPromise> {

  private static final Logger LOG = LoggerFactory.getLogger(ByteBufSegmentBuilder.class);

  private final ChannelHandlerContext context;
  private final String logPrefix;

  public ByteBufSegmentBuilder(
      @NonNull ChannelHandlerContext context,
      @NonNull PrimitiveCodec<ByteBuf> primitiveCodec,
      @NonNull FrameCodec<ByteBuf> frameCodec,
      @NonNull String logPrefix) {
    super(primitiveCodec, frameCodec);
    this.context = context;
    this.logPrefix = logPrefix;
  }

  @Override
  @NonNull
  protected ChannelPromise mergeStates(@NonNull List<ChannelPromise> framePromises) {
    if (framePromises.size() == 1) {
      return framePromises.get(0);
    }
    // We concatenate multiple frames into one segment. When the segment is written, all the frames
    // are written.
    ChannelPromise segmentPromise = context.newPromise();
    ImmutableList<ChannelPromise> dependents = ImmutableList.copyOf(framePromises);
    segmentPromise.addListener(
        future -> {
          if (future.isSuccess()) {
            for (ChannelPromise framePromise : dependents) {
              framePromise.setSuccess();
            }
          } else {
            Throwable cause = future.cause();
            for (ChannelPromise framePromise : dependents) {
              framePromise.setFailure(cause);
            }
          }
        });
    return segmentPromise;
  }

  @Override
  @NonNull
  protected List<ChannelPromise> splitState(@NonNull ChannelPromise framePromise, int sliceCount) {
    // We split one frame into multiple slices. When all slices are written, the frame is written.
    List<ChannelPromise> slicePromises = new ArrayList<>(sliceCount);
    for (int i = 0; i < sliceCount; i++) {
      slicePromises.add(context.newPromise());
    }
    GenericFutureListener<Future<Void>> sliceListener =
        new SliceWriteListener(framePromise, slicePromises);
    for (int i = 0; i < sliceCount; i++) {
      slicePromises.get(i).addListener(sliceListener);
    }
    return slicePromises;
  }

  @Override
  protected void processSegment(
      @NonNull Segment<ByteBuf> segment, @NonNull ChannelPromise segmentPromise) {
    context.write(segment, segmentPromise);
  }

  @Override
  protected void onLargeFrameSplit(@NonNull Frame frame, int frameLength, int sliceCount) {
    LOG.trace(
        "[{}] Frame {} is too large ({} > {}), splitting into {} segments",
        logPrefix,
        frame.streamId,
        frameLength,
        Segment.MAX_PAYLOAD_LENGTH,
        sliceCount);
  }

  @Override
  protected void onSegmentFull(
      @NonNull Frame frame, int frameLength, int currentPayloadLength, int currentFrameCount) {
    LOG.trace(
        "[{}] Current self-contained segment is full ({}/{} bytes, {} frames), processing now",
        logPrefix,
        currentPayloadLength,
        Segment.MAX_PAYLOAD_LENGTH,
        currentFrameCount);
  }

  @Override
  protected void onSmallFrameAdded(
      @NonNull Frame frame, int frameLength, int currentPayloadLength, int currentFrameCount) {
    LOG.trace(
        "[{}] Added frame {} to current self-contained segment "
            + "(bringing it to {}/{} bytes, {} frames)",
        logPrefix,
        frame.streamId,
        currentPayloadLength,
        Segment.MAX_PAYLOAD_LENGTH,
        currentFrameCount);
  }

  @Override
  protected void onLastSegmentFlushed(int currentPayloadLength, int currentFrameCount) {
    LOG.trace(
        "[{}] Flushing last self-contained segment ({}/{} bytes, {} frames)",
        logPrefix,
        currentPayloadLength,
        Segment.MAX_PAYLOAD_LENGTH,
        currentFrameCount);
  }

  @NotThreadSafe
  static class SliceWriteListener implements GenericFutureListener<Future<Void>> {

    private final ChannelPromise parentPromise;
    private final List<ChannelPromise> slicePromises;

    // All slices are written to the same channel, and the segment is built from the Flusher which
    // also runs on the same event loop, so we don't need synchronization.
    private int remainingSlices;

    SliceWriteListener(@NonNull ChannelPromise parentPromise, List<ChannelPromise> slicePromises) {
      this.parentPromise = parentPromise;
      this.slicePromises = slicePromises;
      this.remainingSlices = slicePromises.size();
    }

    @Override
    public void operationComplete(@NonNull Future<Void> future) {
      if (!parentPromise.isDone()) {
        if (future.isSuccess()) {
          remainingSlices -= 1;
          if (remainingSlices == 0) {
            parentPromise.setSuccess();
          }
        } else {
          // If any slice fails, we can immediately mark the whole frame as failed:
          parentPromise.setFailure(future.cause());
          // Cancel any remaining slice, Netty will not send the bytes.
          for (ChannelPromise slicePromise : slicePromises) {
            slicePromise.cancel(/*Netty ignores this*/ false);
          }
        }
      }
    }
  }
}
