/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.channel;

import com.datastax.oss.driver.api.core.CoreProtocolVersion;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.connection.ConnectionException;
import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;

public class InFlightHandlerTest extends ChannelHandlerTestBase {
  private static final Query QUERY = new Query("select * from foo");
  public static final int SET_KEYSPACE_TIMEOUT_MILLIS = 100;

  @Mock private StreamIdGenerator streamIds;

  @BeforeMethod
  @Override
  public void setup() {
    super.setup();
    MockitoAnnotations.initMocks(this);
    channel
        .pipeline()
        .addLast(
            new InFlightHandler(CoreProtocolVersion.V3, streamIds, SET_KEYSPACE_TIMEOUT_MILLIS));
  }

  @Test
  public void should_fail_if_connection_busy() throws Throwable {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(-1);

    // When
    ChannelFuture writeFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(
                QUERY, false, Frame.NO_PAYLOAD, new MockResponseCallback()));

    // Then
    assertThat(writeFuture)
        .isFailed(e -> assertThat(e).isInstanceOf(BusyConnectionException.class));
  }

  @Test
  public void should_assign_streamid_and_send_frame() {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();

    // When
    ChannelFuture writeFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));

    // Then
    assertThat(writeFuture).isSuccess();
    Mockito.verify(streamIds).acquire();

    Frame frame = readOutboundFrame();
    assertThat(frame.streamId).isEqualTo(42);
    assertThat(frame.message).isEqualTo(QUERY);
  }

  @Test
  public void should_notify_callback_of_response() {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    Frame requestFrame = readOutboundFrame();

    // When
    Frame responseFrame = buildInboundFrame(requestFrame, Void.INSTANCE);
    writeInboundFrame(responseFrame);

    // Then
    assertThat(responseCallback.getLastResponse()).isSameAs(responseFrame);
    Mockito.verify(streamIds).release(42);
  }

  @Test
  public void should_notify_response_promise_when_decoding_fails() throws Throwable {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));

    // When
    RuntimeException mockCause = new RuntimeException("test");
    channel.pipeline().fireExceptionCaught(new FrameDecodingException(42, mockCause));

    // Then
    assertThat(responseCallback.getFailure()).isSameAs(mockCause);
    Mockito.verify(streamIds).release(42);
  }

  @Test
  public void should_delay_close_until_all_pending_complete() {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));

    // When
    ChannelFuture closeFuture = channel.close();

    // Then
    // not closed yet because there is one pending request
    assertThat(closeFuture).isNotDone();

    // When
    // completing pending request
    Frame requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, Void.INSTANCE);

    // Then
    assertThat(closeFuture).isSuccess();
  }

  @Test
  public void should_fail_all_pending_when_force_closed() throws Throwable {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1));
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2));

    // When
    ChannelFuture closeFuture = channel.close();
    assertThat(closeFuture).isNotDone();
    channel.pipeline().fireUserEventTriggered(DriverChannel.FORCE_CLOSE_EVENT);

    // Then
    assertThat(closeFuture).isSuccess();
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      assertThat(callback.getFailure())
          .isInstanceOf(ConnectionException.class)
          .hasMessageContaining("Channel was force-closed");
    }
  }

  @Test
  public void should_fail_all_pending_and_close_on_unexpected_inbound_exception() throws Throwable {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1));
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2));

    // When
    RuntimeException mockException = new RuntimeException("test");
    channel.pipeline().fireExceptionCaught(mockException);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      Throwable failure = callback.getFailure();
      assertThat(failure).isInstanceOf(ConnectionException.class);
      assertThat(failure.getCause()).isSameAs(mockException);
    }
  }

  @Test
  public void should_hold_stream_id_if_required() {
    // Given
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback(true);

    // When
    channel.writeAndFlush(
        new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    channel.runPendingTasks();

    // Then
    // notify callback of stream id
    assertThat(responseCallback.streamId).isEqualTo(42);

    Frame requestFrame = readOutboundFrame();
    for (int i = 0; i < 5; i++) {
      // When
      // completing pending request
      Frame responseFrame = buildInboundFrame(requestFrame, Void.INSTANCE);
      writeInboundFrame(responseFrame);

      // Then
      assertThat(responseCallback.getLastResponse()).isSameAs(responseFrame);
      // Stream id not released, callback can receive more responses
      Mockito.verify(streamIds, never()).release(42);
    }

    // When
    // the client releases the stream id
    channel.pipeline().fireUserEventTriggered(new DriverChannel.ReleaseEvent(42));

    // Then
    Mockito.verify(streamIds).release(42);
    writeInboundFrame(requestFrame, Void.INSTANCE);
    // if more responses use this stream id, the handler does not get them anymore
    assertThat(responseCallback.getLastResponse()).isNull();
  }

  @Test
  public void should_set_keyspace() {
    // Given
    ChannelPromise setKeyspacePromise = channel.newPromise();
    DriverChannel.SetKeyspaceEvent setKeyspaceEvent =
        new DriverChannel.SetKeyspaceEvent(CqlIdentifier.fromCql("ks"), setKeyspacePromise);

    // When
    channel.pipeline().fireUserEventTriggered(setKeyspaceEvent);
    Frame requestFrame = readOutboundFrame();

    // Then
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    writeInboundFrame(requestFrame, new SetKeyspace("ks"));
    assertThat(setKeyspacePromise).isSuccess();
  }

  @Test
  public void should_fail_to_set_keyspace_if_query_times_out() throws InterruptedException {
    // Given
    ChannelPromise setKeyspacePromise = channel.newPromise();
    DriverChannel.SetKeyspaceEvent setKeyspaceEvent =
        new DriverChannel.SetKeyspaceEvent(CqlIdentifier.fromCql("ks"), setKeyspacePromise);

    // When
    channel.pipeline().fireUserEventTriggered(setKeyspaceEvent);
    TimeUnit.MILLISECONDS.sleep(SET_KEYSPACE_TIMEOUT_MILLIS * 2);
    channel.runPendingTasks();

    // Then
    assertThat(setKeyspacePromise).isFailed();
  }

  static class MockResponseCallback implements ResponseCallback {
    private final boolean holdStreamId;
    private final Queue<Object> responses = new LinkedList<>();

    private volatile int streamId = -1;

    MockResponseCallback() {
      this(false);
    }

    MockResponseCallback(boolean holdStreamId) {
      this.holdStreamId = holdStreamId;
    }

    @Override
    public void onResponse(Frame responseFrame) {
      responses.offer(responseFrame);
    }

    @Override
    public void onFailure(Throwable error) {
      responses.offer(error);
    }

    @Override
    public boolean holdStreamId() {
      return holdStreamId;
    }

    @Override
    public void onStreamIdAssigned(int streamId) {
      this.streamId = streamId;
    }

    Frame getLastResponse() {
      return (Frame) responses.poll();
    }

    Throwable getFailure() {
      return (Throwable) responses.poll();
    }
  }
}
