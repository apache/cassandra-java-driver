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
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.internal.core.protocol.FrameDecodingException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.google.common.collect.ImmutableList;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.Mockito.never;

public class InFlightHandlerTest extends ChannelHandlerTestBase {
  private static final Query QUERY = new Query("select * from foo");
  private static final int SET_KEYSPACE_TIMEOUT_MILLIS = 100;
  private static final int MAX_ORPHAN_IDS = 10;

  @Mock private StreamIdGenerator streamIds;

  @Before
  @Override
  public void setup() {
    super.setup();
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void should_fail_if_connection_busy() throws Throwable {
    // Given
    addToPipeline();
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
    addToPipeline();
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
    addToPipeline();
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
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    RuntimeException mockCause = new RuntimeException("test");
    channel.pipeline().fireExceptionCaught(new FrameDecodingException(42, mockCause));

    // Then
    assertThat(responseCallback.getFailure()).isSameAs(mockCause);
    Mockito.verify(streamIds).release(42);
  }

  @Test
  public void should_delay_graceful_close_and_complete_when_last_pending_completes() {
    // Given
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    // not closed yet because there is one pending request
    assertThat(channel.closeFuture()).isNotDone();

    // When
    // completing pending request
    Frame requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, Void.INSTANCE);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_delay_graceful_close_and_complete_when_last_pending_cancelled() {
    // Given
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    // not closed yet because there is one pending request
    assertThat(channel.closeFuture()).isNotDone();

    // When
    // cancelling pending request
    channel.write(responseCallback);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_graceful_close_immediately_if_no_pending() {
    // Given
    addToPipeline();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_refuse_new_writes_during_graceful_close() {
    // Given
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.GRACEFUL_CLOSE_MESSAGE);

    // Then
    // not closed yet because there is one pending request
    assertThat(channel.closeFuture()).isNotDone();
    // should not allow other write
    ChannelFuture otherWriteFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    assertThat(otherWriteFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Channel is closing"));
  }

  @Test
  public void should_close_gracefully_if_orphan_ids_above_max_and_pending_requests() {
    // Given
    addToPipeline();
    // Generate n orphan ids by writing and cancelling the requests:
    for (int i = 0; i < MAX_ORPHAN_IDS; i++) {
      Mockito.when(streamIds.acquire()).thenReturn(i);
      MockResponseCallback responseCallback = new MockResponseCallback();
      channel
          .writeAndFlush(
              new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
          .awaitUninterruptibly();
      channel.writeAndFlush(responseCallback).awaitUninterruptibly();
    }
    // Generate another request that is pending and not cancelled:
    Mockito.when(streamIds.acquire()).thenReturn(MAX_ORPHAN_IDS);
    MockResponseCallback pendingResponseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(
                QUERY, false, Frame.NO_PAYLOAD, pendingResponseCallback))
        .awaitUninterruptibly();

    // When
    // Generate the n+1th orphan id that makes us go above the threshold
    Mockito.when(streamIds.acquire()).thenReturn(MAX_ORPHAN_IDS + 1);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();
    channel.writeAndFlush(responseCallback).awaitUninterruptibly();

    // Then
    // Channel should be closing gracefully. There's no way to observe that from the outside, so
    // write another request and check that it's rejected:
    assertThat(channel.closeFuture()).isNotDone();
    ChannelFuture otherWriteFuture =
        channel.writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback));
    assertThat(otherWriteFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("Channel is closing"));

    // When
    // Cancel the last pending request
    channel.writeAndFlush(pendingResponseCallback).awaitUninterruptibly();

    // Then
    // The graceful shutdown completes
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_close_immediately_if_orphan_ids_above_max_and_no_pending_requests() {
    // Given
    addToPipeline();
    // Generate n orphan ids by writing and cancelling the requests:
    for (int i = 0; i < MAX_ORPHAN_IDS; i++) {
      Mockito.when(streamIds.acquire()).thenReturn(i);
      MockResponseCallback responseCallback = new MockResponseCallback();
      channel
          .writeAndFlush(
              new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
          .awaitUninterruptibly();
      channel.writeAndFlush(responseCallback).awaitUninterruptibly();
    }

    // When
    // Generate the n+1th orphan id that makes us go above the threshold
    Mockito.when(streamIds.acquire()).thenReturn(MAX_ORPHAN_IDS);
    MockResponseCallback responseCallback = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();
    channel.writeAndFlush(responseCallback).awaitUninterruptibly();

    // Then
    // Channel should close immediately since no active pending requests.
    assertThat(channel.closeFuture()).isSuccess();
  }

  @Test
  public void should_fail_all_pending_when_force_closed() throws Throwable {
    // Given
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1))
        .awaitUninterruptibly();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2))
        .awaitUninterruptibly();

    // When
    channel.write(DriverChannel.FORCEFUL_CLOSE_MESSAGE);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      assertThat(callback.getFailure())
          .isInstanceOf(ClosedConnectionException.class)
          .hasMessageContaining("Channel was force-closed");
    }
  }

  @Test
  public void should_fail_all_pending_and_close_on_unexpected_inbound_exception() throws Throwable {
    // Given
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1))
        .awaitUninterruptibly();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2))
        .awaitUninterruptibly();

    // When
    RuntimeException mockException = new RuntimeException("test");
    channel.pipeline().fireExceptionCaught(mockException);

    // Then
    assertThat(channel.closeFuture()).isSuccess();
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      Throwable failure = callback.getFailure();
      assertThat(failure).isInstanceOf(ClosedConnectionException.class);
      assertThat(failure.getCause()).isSameAs(mockException);
    }
  }

  @Test
  public void should_fail_all_pending_if_connection_lost() {
    // Given
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42, 43);
    MockResponseCallback responseCallback1 = new MockResponseCallback();
    MockResponseCallback responseCallback2 = new MockResponseCallback();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback1))
        .awaitUninterruptibly();
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback2))
        .awaitUninterruptibly();

    // When
    channel.pipeline().fireChannelInactive();

    // Then
    for (MockResponseCallback callback : ImmutableList.of(responseCallback1, responseCallback2)) {
      assertThat(callback.getFailure())
          .isInstanceOf(ClosedConnectionException.class)
          .hasMessageContaining("Lost connection to remote peer");
    }
  }

  @Test
  public void should_hold_stream_id_if_required() {
    // Given
    addToPipeline();
    Mockito.when(streamIds.acquire()).thenReturn(42);
    MockResponseCallback responseCallback = new MockResponseCallback(true);

    // When
    channel
        .writeAndFlush(
            new DriverChannel.RequestMessage(QUERY, false, Frame.NO_PAYLOAD, responseCallback))
        .awaitUninterruptibly();

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
    addToPipeline();
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
    addToPipeline();
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

  @Test
  public void should_notify_callback_of_events() {
    // Given
    EventCallback eventCallback = Mockito.mock(EventCallback.class);
    addToPipelineWithEventCallback(eventCallback);

    // When
    StatusChangeEvent event =
        new StatusChangeEvent(
            ProtocolConstants.StatusChangeType.UP, new InetSocketAddress("127.0.0.1", 9042));
    Frame eventFrame =
        Frame.forResponse(
            CoreProtocolVersion.V3.getCode(),
            -1,
            null,
            Collections.emptyMap(),
            Collections.emptyList(),
            event);
    writeInboundFrame(eventFrame);

    // Then
    ArgumentCaptor<StatusChangeEvent> captor = ArgumentCaptor.forClass(StatusChangeEvent.class);
    Mockito.verify(eventCallback).onEvent(captor.capture());
    assertThat(captor.getValue()).isSameAs(event);
  }

  private void addToPipeline() {
    addToPipelineWithEventCallback(null);
  }

  private void addToPipelineWithEventCallback(EventCallback eventCallback) {
    channel
        .pipeline()
        .addLast(
            new InFlightHandler(
                CoreProtocolVersion.V3,
                streamIds,
                MAX_ORPHAN_IDS,
                SET_KEYSPACE_TIMEOUT_MILLIS,
                null,
                channel.newPromise(),
                eventCallback,
                "test"));
  }
}
