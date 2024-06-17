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
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.connection.FrameTooLongException;
import com.datastax.oss.driver.internal.core.channel.ChannelHandlerTestBase;
import com.datastax.oss.driver.internal.core.util.ByteBufs;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.Before;
import org.junit.Test;

public class FrameDecoderTest extends ChannelHandlerTestBase {
  // A valid binary payload for a response frame.
  private static final ByteBuf VALID_PAYLOAD =
      ByteBufs.fromHexString(
          "0x84" // response frame, protocol version 4
              + "00" // flags (none)
              + "002a" // stream id (42)
              + "10" // opcode for AUTH_SUCCESS message
              + "00000008" // body length
              + "00000004cafebabe" // body
          );

  // A binary payload that is invalid because the protocol version is not supported by the codec
  private static final ByteBuf INVALID_PAYLOAD =
      ByteBufs.fromHexString(
          "0xFF" // response frame, protocol version 127
              + "00002a100000000800000004cafebabe");

  private FrameCodec<ByteBuf> frameCodec;

  @Before
  @Override
  public void setup() {
    super.setup();
    frameCodec =
        FrameCodec.defaultClient(new ByteBufPrimitiveCodec(channel.alloc()), Compressor.none());
  }

  @Test
  public void should_decode_valid_payload() {
    // Given
    FrameDecoder decoder = new FrameDecoder(frameCodec, 1024);
    channel.pipeline().addLast(decoder);

    // When
    // The decoder releases the buffer, so make sure we retain it for the other tests
    VALID_PAYLOAD.retain();
    channel.writeInbound(VALID_PAYLOAD.duplicate());
    Frame frame = readInboundFrame();

    // Then
    assertThat(frame.message).isInstanceOf(AuthSuccess.class);
  }

  /**
   * Checks that an exception carrying the stream id is thrown when decoding fails in the {@link
   * LengthFieldBasedFrameDecoder} code.
   */
  @Test
  public void should_fail_to_decode_if_payload_is_valid_but_too_long() {
    // Given
    FrameDecoder decoder = new FrameDecoder(frameCodec, VALID_PAYLOAD.readableBytes() - 1);
    channel.pipeline().addLast(decoder);

    // When
    VALID_PAYLOAD.retain();
    try {
      channel.writeInbound(VALID_PAYLOAD.duplicate());
      fail("expected an exception");
    } catch (FrameDecodingException e) {
      // Then
      assertThat(e.streamId).isEqualTo(42);
      assertThat(e.getCause()).isInstanceOf(FrameTooLongException.class);
    }
  }

  /** Checks that an exception carrying the stream id is thrown when decoding fails in our code. */
  @Test
  public void should_fail_to_decode_if_payload_cannot_be_decoded() {
    // Given
    FrameDecoder decoder = new FrameDecoder(frameCodec, 1024);
    channel.pipeline().addLast(decoder);

    // When
    INVALID_PAYLOAD.retain();
    try {
      channel.writeInbound(INVALID_PAYLOAD.duplicate());
      fail("expected an exception");
    } catch (FrameDecodingException e) {
      // Then
      assertThat(e.streamId).isEqualTo(42);
      assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }
}
