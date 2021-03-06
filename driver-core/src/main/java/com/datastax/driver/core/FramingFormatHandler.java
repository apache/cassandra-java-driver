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

import com.datastax.driver.core.Message.Response.Type;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.MessageToMessageDecoder;
import java.util.List;

/**
 * A handler to deal with different protocol framing formats.
 *
 * <p>This handler detects when a handshake is successful; then, if necessary, adapts the pipeline
 * to the modern framing format introduced in protocol v5.
 */
public class FramingFormatHandler extends MessageToMessageDecoder<Frame> {

  private final Connection.Factory factory;

  FramingFormatHandler(Connection.Factory factory) {
    this.factory = factory;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, Frame frame, List<Object> out) throws Exception {
    boolean handshakeSuccessful =
        frame.header.opcode == Type.READY.opcode || frame.header.opcode == Type.AUTHENTICATE.opcode;
    if (handshakeSuccessful) {
      // By default, the pipeline is configured for legacy framing since this is the format used
      // by all protocol versions until handshake; after handshake however, we need to switch to
      // modern framing for protocol v5 and higher.
      if (frame.header.version.compareTo(ProtocolVersion.V5) >= 0) {
        switchToModernFraming(ctx);
      }
      // once the handshake is successful, the framing format cannot change anymore;
      // we can safely remove ourselves from the pipeline.
      ctx.pipeline().remove("framingFormatHandler");
    }
    out.add(frame);
  }

  private void switchToModernFraming(ChannelHandlerContext ctx) {
    ChannelPipeline pipeline = ctx.pipeline();
    SegmentCodec segmentCodec =
        new SegmentCodec(
            ctx.channel().alloc(), factory.configuration.getProtocolOptions().getCompression());

    // Outbound: "message -> segment -> bytes" instead of "message -> frame -> bytes"
    Message.ProtocolEncoder requestEncoder =
        (Message.ProtocolEncoder) pipeline.get("messageEncoder");
    pipeline.replace(
        "messageEncoder",
        "messageToSegmentEncoder",
        new MessageToSegmentEncoder(ctx.channel().alloc(), requestEncoder));
    pipeline.replace(
        "frameEncoder", "segmentToBytesEncoder", new SegmentToBytesEncoder(segmentCodec));

    // Inbound: "frame <- segment <- bytes" instead of "frame <- bytes"
    pipeline.replace(
        "frameDecoder", "bytesToSegmentDecoder", new BytesToSegmentDecoder(segmentCodec));
    pipeline.addAfter(
        "bytesToSegmentDecoder", "segmentToFrameDecoder", new SegmentToFrameDecoder());
  }
}
