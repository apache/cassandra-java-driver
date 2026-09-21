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
import static org.assertj.core.api.Assertions.catchThrowable;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.event.GracefulDisconnectEvent;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.junit.Test;

/**
 * Decodes the CEP-59 {@code GRACEFUL_DISCONNECT} event (CASSANDRA-21191) from raw bytes, as the
 * server would send them.
 */
public class GracefulDisconnectWireCompatTest {

  private final FrameCodec<ByteBuf> frameCodec =
      FrameCodec.defaultClient(
          new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), Compressor.none());

  private ByteBuf rawEventFrame(int protocolVersion, byte[] body) {
    ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer();
    buffer.writeByte(protocolVersion | 0x80); // response direction bit
    buffer.writeByte(0); // flags
    buffer.writeShort(-1); // stream id: events always use -1
    buffer.writeByte(ProtocolConstants.Opcode.EVENT);
    buffer.writeInt(body.length);
    buffer.writeBytes(body);
    return buffer;
  }

  private static byte[] eventBody(String eventType) {
    byte[] typeBytes = eventType.getBytes(StandardCharsets.UTF_8);
    byte[] body = new byte[2 + typeBytes.length];
    body[0] = (byte) (typeBytes.length >> 8);
    body[1] = (byte) typeBytes.length;
    System.arraycopy(typeBytes, 0, body, 2, typeBytes.length);
    return body;
  }

  @Test
  public void should_decode_v4_envelope() {
    ByteBuf raw = rawEventFrame(ProtocolConstants.Version.V4, eventBody("GRACEFUL_DISCONNECT"));

    Frame frame = frameCodec.decode(raw);

    assertThat(frame.streamId).isEqualTo(-1);
    assertThat(frame.message).isInstanceOf(GracefulDisconnectEvent.class);
  }

  @Test
  public void should_decode_v5_envelope() {
    ByteBuf raw = rawEventFrame(ProtocolConstants.Version.V5, eventBody("GRACEFUL_DISCONNECT"));

    Frame frame = frameCodec.decode(raw);

    assertThat(frame.message).isInstanceOf(GracefulDisconnectEvent.class);
  }

  @Test
  public void should_round_trip_encoded_event() {
    Frame outgoing =
        Frame.forResponse(
            ProtocolConstants.Version.V4,
            -1,
            null,
            Collections.emptyMap(),
            Collections.emptyList(),
            new GracefulDisconnectEvent());
    FrameCodec<ByteBuf> serverCodec =
        FrameCodec.defaultServer(
            new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), Compressor.none());

    Frame decoded = frameCodec.decode(serverCodec.encode(outgoing));

    assertThat(decoded.message).isInstanceOf(GracefulDisconnectEvent.class);
  }

  @Test
  public void should_reject_unknown_event_type() {
    ByteBuf raw = rawEventFrame(ProtocolConstants.Version.V4, eventBody("GRACEFUL_DISCONNECT_V2"));

    Throwable t = catchThrowable(() -> frameCodec.decode(raw));

    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported event type");
  }
}
