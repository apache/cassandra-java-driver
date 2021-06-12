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
import java.util.Collections;
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

  @Before
  public void setup() {
    channel = new EmbeddedChannel();
    channel.pipeline().addLast(new SegmentToFrameDecoder(FRAME_CODEC, "test"));
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
