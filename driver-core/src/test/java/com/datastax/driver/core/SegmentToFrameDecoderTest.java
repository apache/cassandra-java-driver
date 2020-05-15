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

import static com.datastax.driver.core.Message.Response.Type.READY;
import static com.datastax.driver.core.Message.Response.Type.RESULT;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Frame.Header;
import com.datastax.driver.core.Frame.Header.Flag;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.EnumSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SegmentToFrameDecoderTest {

  private static final ByteBuf SMALL_BODY_1 = buffer(128);
  private static final Header SMALL_HEADER_1 =
      new Header(
          ProtocolVersion.V5,
          EnumSet.noneOf(Flag.class),
          2,
          READY.opcode,
          SMALL_BODY_1.readableBytes());

  private static final ByteBuf SMALL_BODY_2 = buffer(1024);
  private static final Header SMALL_HEADER_2 =
      new Header(
          ProtocolVersion.V5,
          EnumSet.noneOf(Flag.class),
          7,
          RESULT.opcode,
          SMALL_BODY_2.readableBytes());

  private static final ByteBuf LARGE_BODY = buffer(256 * 1024);
  private static final Header LARGE_HEADER =
      new Header(
          ProtocolVersion.V5,
          EnumSet.noneOf(Flag.class),
          12,
          RESULT.opcode,
          LARGE_BODY.readableBytes());

  private EmbeddedChannel channel;

  @BeforeMethod(groups = "unit")
  public void setup() {
    channel = new EmbeddedChannel();
    channel.pipeline().addLast(new SegmentToFrameDecoder());
  }

  @Test(groups = "unit")
  public void should_decode_self_contained() {
    ByteBuf payload = UnpooledByteBufAllocator.DEFAULT.buffer();
    appendFrame(SMALL_HEADER_1, SMALL_BODY_1, payload);
    appendFrame(SMALL_HEADER_2, SMALL_BODY_2, payload);

    channel.writeInbound(new Segment(payload, true));

    Frame frame1 = (Frame) channel.readInbound();
    Header header1 = frame1.header;
    assertThat(header1.streamId).isEqualTo(SMALL_HEADER_1.streamId);
    assertThat(header1.opcode).isEqualTo(SMALL_HEADER_1.opcode);
    assertThat(frame1.body).isEqualTo(SMALL_BODY_1);

    Frame frame2 = (Frame) channel.readInbound();
    Header header2 = frame2.header;
    assertThat(header2.streamId).isEqualTo(SMALL_HEADER_2.streamId);
    assertThat(header2.opcode).isEqualTo(SMALL_HEADER_2.opcode);
    assertThat(frame2.body).isEqualTo(SMALL_BODY_2);
  }

  @Test(groups = "unit")
  public void should_decode_sequence_of_slices() {
    ByteBuf encodedFrame = UnpooledByteBufAllocator.DEFAULT.buffer();
    appendFrame(LARGE_HEADER, LARGE_BODY, encodedFrame);

    do {
      ByteBuf payload =
          encodedFrame.readSlice(
              Math.min(Segment.MAX_PAYLOAD_LENGTH, encodedFrame.readableBytes()));
      channel.writeInbound(new Segment(payload, false));
    } while (encodedFrame.isReadable());

    Frame frame = (Frame) channel.readInbound();
    Header header = frame.header;
    assertThat(header.streamId).isEqualTo(LARGE_HEADER.streamId);
    assertThat(header.opcode).isEqualTo(LARGE_HEADER.opcode);
    assertThat(frame.body).isEqualTo(LARGE_BODY);
  }

  private static final ByteBuf buffer(int length) {
    ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer(length);
    // Contents don't really matter, keep all zeroes
    buffer.writerIndex(buffer.readerIndex() + length);
    return buffer;
  }

  private static void appendFrame(Header frameHeader, ByteBuf frameBody, ByteBuf payload) {
    frameHeader.encodeInto(payload);
    // this method doesn't affect the body's indices:
    payload.writeBytes(frameBody, frameBody.readerIndex(), frameBody.readableBytes());
  }
}
