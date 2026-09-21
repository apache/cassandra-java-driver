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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.base.Strings;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolV5ClientCodecs;
import com.datastax.oss.protocol.internal.ProtocolV5ServerCodecs;
import com.datastax.oss.protocol.internal.Segment;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class SegmentToFrameDecoderTest {

  private static final FrameCodec<ByteBuf> FRAME_CODEC =
      new FrameCodec<>(
          new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT),
          Compressor.none(),
          new ProtocolV5ClientCodecs(),
          new ProtocolV5ServerCodecs());

  private EmbeddedChannel channel;
  private SegmentToFrameDecoder segmentToFrameDecoder =
      new SegmentToFrameDecoder(FRAME_CODEC, "test");

  @Before
  public void setup() {
    channel = new EmbeddedChannel();
    channel.pipeline().addLast(segmentToFrameDecoder);
  }

  @Test
  public void should_decode_self_contained() {
    ByteBuf payload = UnpooledByteBufAllocator.DEFAULT.buffer();
    payload.writeBytes(encodeFrame(Void.INSTANCE));
    payload.writeBytes(encodeFrame(new AuthResponse(Bytes.fromHexString("0xabcdef"))));

    channel.writeInbound(new Segment<>(payload, true));

    Frame frame1 = channel.readInbound();
    assertThat(frame1.message).isInstanceOf(Void.class);
    Frame frame2 = channel.readInbound();
    assertThat(frame2.message).isInstanceOf(AuthResponse.class);

    // check decoder is not holding onto any segments
    assertThat(segmentToFrameDecoder.getAccumulatedSlices())
        .withFailMessage("Expected decoded to have no more partial frame segments")
        .isEmpty();
  }

  @Test
  public void should_decode_sequence_of_slices() {
    ByteBuf encodedFrame =
        encodeFrame(new AuthResponse(Bytes.fromHexString("0x" + Strings.repeat("aa", 1011))));
    int sliceLength = 100;
    do {
      ByteBuf payload =
          encodedFrame.readRetainedSlice(Math.min(sliceLength, encodedFrame.readableBytes()));
      channel.writeInbound(new Segment<>(payload, false));
    } while (encodedFrame.isReadable());

    Frame frame = channel.readInbound();
    assertThat(frame.message).isInstanceOf(AuthResponse.class);

    // check decoder is not holding onto any segments
    assertThat(segmentToFrameDecoder.getAccumulatedSlices())
        .withFailMessage("Expected decoded to have no more partial frame segments")
        .isEmpty();
  }

  @Test
  public void should_release_partial_frame_segments_on_channel_close() {
    ByteBuf encodedFrame =
        encodeFrame(new AuthResponse(Bytes.fromHexString("0x" + Strings.repeat("aa", 1011))));
    int sliceLength = 100;
    for (int i = 0; i < 5; i++) {
      // intentionally make copies of the original frame so we can check reference counts later
      ByteBuf payload = encodedFrame.readBytes(Math.min(sliceLength, encodedFrame.readableBytes()));
      channel.writeInbound(new Segment<>(payload, false));
    }

    assertThat(encodedFrame.isReadable())
        .withFailMessage("Expected remaining content in encoded frame")
        .isTrue();

    // read the segments that have been written so far (will give null response)
    assertThat(channel.<Frame>readInbound())
        .withFailMessage("Expected no frame to be decoded")
        .isNull();

    // make a copy of the segments we accumulated so we can check they are released later
    // check correct number of partial frame segments have been accumulated
    List<ByteBuf> segments = new ArrayList<>(segmentToFrameDecoder.getAccumulatedSlices());
    assertThat(segments)
        .withFailMessage("Expected correct number partial frame segments accumulated")
        .hasSize(5);

    // check segments have been released
    for (ByteBuf segment : segments) {
      assertThat(segment.refCnt())
          .withFailMessage("Expected all partial frame segments to be live (not released)")
          .isEqualTo(1);
    }

    // call channel close which will destroy the pipeline and call handlerRemoved on the decoder
    channel.close();

    // check decoder is not holding onto any segments
    assertThat(segmentToFrameDecoder.getAccumulatedSlices())
        .withFailMessage("Expected decoded to have no more partial frame segments")
        .isEmpty();

    // check segments we got from the decoder earlier have been released
    for (ByteBuf segment : segments) {
      assertThat(segment.refCnt())
          .withFailMessage("Expected all partial frame segments to be released")
          .isZero();
    }
  }

  private static ByteBuf encodeFrame(Message message) {
    Frame frame =
        Frame.forResponse(
            ProtocolConstants.Version.V5,
            1,
            null,
            Collections.emptyMap(),
            Collections.emptyList(),
            message);
    return FRAME_CODEC.encode(frame);
  }
}
