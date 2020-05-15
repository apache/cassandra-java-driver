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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstracts the details of batching a sequence of {@link Message.Request}s into one or more {@link
 * Segment}s before sending them out on the network.
 *
 * <p>This class is not thread-safe.
 */
class SegmentBuilder {

  private static final Logger logger = LoggerFactory.getLogger(SegmentBuilder.class);

  private final ChannelHandlerContext context;
  private final ByteBufAllocator allocator;
  private final int maxPayloadLength;
  private final Message.ProtocolEncoder requestEncoder;

  private final List<Frame.Header> currentPayloadHeaders = new ArrayList<Frame.Header>();
  private final List<Message.Request> currentPayloadBodies = new ArrayList<Message.Request>();
  private final List<ChannelPromise> currentPayloadPromises = new ArrayList<ChannelPromise>();
  private int currentPayloadLength;

  SegmentBuilder(
      ChannelHandlerContext context,
      ByteBufAllocator allocator,
      Message.ProtocolEncoder requestEncoder) {
    this(context, allocator, requestEncoder, Segment.MAX_PAYLOAD_LENGTH);
  }

  /** Exposes the max length for unit tests; in production, this is hard-coded. */
  @VisibleForTesting
  SegmentBuilder(
      ChannelHandlerContext context,
      ByteBufAllocator allocator,
      Message.ProtocolEncoder requestEncoder,
      int maxPayloadLength) {
    this.context = context;
    this.allocator = allocator;
    this.requestEncoder = requestEncoder;
    this.maxPayloadLength = maxPayloadLength;
  }

  /**
   * Adds a new request. It will be encoded into one or more segments, that will be passed to {@link
   * #processSegment(Segment, ChannelPromise)} at some point in the future.
   *
   * <p>The caller <b>must</b> invoke {@link #flush()} after the last request.
   */
  public void addRequest(Message.Request request, ChannelPromise promise) {

    // Wrap the request into a legacy frame, append that frame to the payload.
    int frameHeaderLength = Frame.Header.lengthFor(requestEncoder.protocolVersion);
    int frameBodyLength = requestEncoder.encodedSize(request);
    int frameLength = frameHeaderLength + frameBodyLength;

    Frame.Header header =
        new Frame.Header(
            requestEncoder.protocolVersion,
            requestEncoder.computeFlags(request),
            request.getStreamId(),
            request.type.opcode,
            frameBodyLength);

    if (frameLength > maxPayloadLength) {
      // Large request: split into multiple dedicated segments and process them immediately:
      ByteBuf frame = allocator.ioBuffer(frameLength);
      header.encodeInto(frame);
      requestEncoder.encode(request, frame);

      int sliceCount =
          (frameLength / maxPayloadLength) + (frameLength % maxPayloadLength == 0 ? 0 : 1);

      logger.trace(
          "Splitting large request ({} bytes) into {} segments: {}",
          frameLength,
          sliceCount,
          request);

      List<ChannelPromise> segmentPromises = split(promise, sliceCount);
      int i = 0;
      do {
        ByteBuf part = frame.readSlice(Math.min(maxPayloadLength, frame.readableBytes()));
        part.retain();
        process(part, false, segmentPromises.get(i++));
      } while (frame.isReadable());
      // We've retained each slice, and won't reference this buffer anymore
      frame.release();
    } else {
      // Small request: append to an existing segment, together with other messages.
      if (currentPayloadLength + frameLength > maxPayloadLength) {
        // Current segment is full, process and start a new one:
        processCurrentPayload();
        resetCurrentPayload();
      }
      // Append frame to current segment
      logger.trace(
          "Adding {}th request to self-contained segment: {}",
          currentPayloadHeaders.size() + 1,
          request);
      currentPayloadHeaders.add(header);
      currentPayloadBodies.add(request);
      currentPayloadPromises.add(promise);
      currentPayloadLength += frameLength;
    }
  }

  /**
   * Signals that we're done adding requests.
   *
   * <p>This must be called after adding the last request, it will possibly trigger the generation
   * of one last segment.
   */
  public void flush() {
    if (currentPayloadLength > 0) {
      processCurrentPayload();
      resetCurrentPayload();
    }
  }

  /** What to do whenever a full segment is ready. */
  protected void processSegment(Segment segment, ChannelPromise segmentPromise) {
    context.write(segment, segmentPromise);
  }

  private void process(ByteBuf payload, boolean isSelfContained, ChannelPromise segmentPromise) {
    processSegment(new Segment(payload, isSelfContained), segmentPromise);
  }

  private void processCurrentPayload() {
    int requestCount = currentPayloadHeaders.size();
    assert currentPayloadBodies.size() == requestCount
        && currentPayloadPromises.size() == requestCount;
    logger.trace("Emitting new self-contained segment with {} frame(s)", requestCount);
    ByteBuf payload = this.allocator.ioBuffer(currentPayloadLength);
    for (int i = 0; i < requestCount; i++) {
      Frame.Header header = currentPayloadHeaders.get(i);
      Message.Request request = currentPayloadBodies.get(i);
      header.encodeInto(payload);
      requestEncoder.encode(request, payload);
    }
    process(payload, true, merge(currentPayloadPromises));
  }

  private void resetCurrentPayload() {
    currentPayloadHeaders.clear();
    currentPayloadBodies.clear();
    currentPayloadPromises.clear();
    currentPayloadLength = 0;
  }

  // Merges multiple promises into a single one, that will notify all of them when done.
  // This is used when multiple requests are sent as a single segment.
  private ChannelPromise merge(List<ChannelPromise> framePromises) {
    if (framePromises.size() == 1) {
      return framePromises.get(0);
    }
    ChannelPromise segmentPromise = context.newPromise();
    final ImmutableList<ChannelPromise> dependents = ImmutableList.copyOf(framePromises);
    segmentPromise.addListener(
        new GenericFutureListener<Future<? super Void>>() {
          @Override
          public void operationComplete(Future<? super Void> future) throws Exception {
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
          }
        });
    return segmentPromise;
  }

  // Splits a single promise into multiple ones. The original promise will complete when all the
  // splits have.
  // This is used when a single request is sliced into multiple segment.
  private List<ChannelPromise> split(ChannelPromise framePromise, int sliceCount) {
    // We split one frame into multiple slices. When all slices are written, the frame is written.
    List<ChannelPromise> slicePromises = new ArrayList<ChannelPromise>(sliceCount);
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

  static class SliceWriteListener implements GenericFutureListener<Future<Void>> {

    private final ChannelPromise parentPromise;
    private final List<ChannelPromise> slicePromises;

    // All slices are written to the same channel, and the segment is built from the Flusher which
    // also runs on the same event loop, so we don't need synchronization.
    private int remainingSlices;

    SliceWriteListener(ChannelPromise parentPromise, List<ChannelPromise> slicePromises) {
      this.parentPromise = parentPromise;
      this.slicePromises = slicePromises;
      this.remainingSlices = slicePromises.size();
    }

    @Override
    public void operationComplete(Future<Void> future) {
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
