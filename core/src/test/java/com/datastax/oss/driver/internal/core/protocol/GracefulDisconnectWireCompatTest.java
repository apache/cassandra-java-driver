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
 * Wire-level compatibility tests for the CEP-59 {@code GRACEFUL_DISCONNECT} event
 * (CASSANDRA-21191).
 *
 * <p>The server-side implementation is still in flux, so these tests exercise the full decode path
 * on raw bytes, as the server would send them, rather than on pre-built message objects. They pin
 * down:
 *
 * <ul>
 *   <li>the current contract: an EVENT envelope whose body is just the type string;
 *   <li>forward compatibility: a future server that appends a payload (e.g. the grace period) to
 *       the event body must not break decoding;
 *   <li>the failure mode for an unknown event type, which is why the driver must never REGISTER for
 *       types the server did not advertise.
 * </ul>
 */
public class GracefulDisconnectWireCompatTest {

  private final FrameCodec<ByteBuf> frameCodec =
      FrameCodec.defaultClient(
          new ByteBufPrimitiveCodec(UnpooledByteBufAllocator.DEFAULT), Compressor.none());

  /** Builds a raw response envelope: version | flags | streamId | opcode | length | body. */
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

  private static byte[] eventBody(String eventType, byte[] extra) {
    byte[] typeBytes = eventType.getBytes(StandardCharsets.UTF_8);
    byte[] body = new byte[2 + typeBytes.length + extra.length];
    body[0] = (byte) (typeBytes.length >> 8);
    body[1] = (byte) typeBytes.length;
    System.arraycopy(typeBytes, 0, body, 2, typeBytes.length);
    System.arraycopy(extra, 0, body, 2 + typeBytes.length, extra.length);
    return body;
  }

  @Test
  public void should_decode_current_server_format() {
    // Body is exactly the type string, as sent by the CASSANDRA-21191 baseline:
    ByteBuf raw =
        rawEventFrame(ProtocolConstants.Version.V4, eventBody("GRACEFUL_DISCONNECT", new byte[0]));

    Frame frame = frameCodec.decode(raw);

    assertThat(frame.streamId).isEqualTo(-1);
    assertThat(frame.message).isInstanceOf(GracefulDisconnectEvent.class);
  }

  @Test
  public void should_decode_v5_envelope() {
    ByteBuf raw =
        rawEventFrame(ProtocolConstants.Version.V5, eventBody("GRACEFUL_DISCONNECT", new byte[0]));

    Frame frame = frameCodec.decode(raw);

    assertThat(frame.message).isInstanceOf(GracefulDisconnectEvent.class);
  }

  @Test
  public void should_tolerate_extra_body_bytes_from_future_server() {
    // A plausible CEP-59 evolution: the server appends the grace period (an [int], here 5000ms)
    // to the event body. An old driver must keep working, ignoring the extra payload.
    byte[] extra = {0x00, 0x00, 0x13, (byte) 0x88};
    ByteBuf raw =
        rawEventFrame(ProtocolConstants.Version.V4, eventBody("GRACEFUL_DISCONNECT", extra));

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
    // Documents the current failure mode if the server ever renames the event or pushes a type
    // the driver does not know: decoding fails. This is why ProtocolInitHandler must only
    // REGISTER for event types the server advertised (see
    // ProtocolInitHandlerGracefulDisconnectTest) — a server never pushes events that were not
    // registered.
    ByteBuf raw =
        rawEventFrame(
            ProtocolConstants.Version.V4, eventBody("GRACEFUL_DISCONNECT_V2", new byte[0]));

    Throwable t = catchThrowable(() -> frameCodec.decode(raw));

    assertThat(t)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported event type");
  }
}
