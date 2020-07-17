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
package com.datastax.oss.driver.internal.core.channel;

import static com.datastax.oss.driver.Assertions.assertThat;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.Void;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.util.concurrent.Future;
import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class DriverChannelTest extends ChannelHandlerTestBase {
  public static final int SET_KEYSPACE_TIMEOUT_MILLIS = 100;

  private DriverChannel driverChannel;
  private MockWriteCoalescer writeCoalescer;

  @Mock private StreamIdGenerator streamIds;

  @Before
  @Override
  public void setup() {
    super.setup();
    MockitoAnnotations.initMocks(this);
    channel
        .pipeline()
        .addLast(
            new InFlightHandler(
                DefaultProtocolVersion.V3,
                streamIds,
                Integer.MAX_VALUE,
                SET_KEYSPACE_TIMEOUT_MILLIS,
                channel.newPromise(),
                null,
                "test"));
    writeCoalescer = new MockWriteCoalescer();
    driverChannel =
        new DriverChannel(
            new EmbeddedEndPoint(), channel, writeCoalescer, DefaultProtocolVersion.V3);
  }

  /**
   * Ensures that the potential delay introduced by the write coalescer does not mess with the
   * graceful shutdown sequence: any write submitted before {@link DriverChannel#close()} is
   * guaranteed to complete.
   */
  @Test
  public void should_wait_for_coalesced_writes_when_closing_gracefully() {
    // Given
    MockResponseCallback responseCallback = new MockResponseCallback();
    driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, responseCallback);
    // nothing written yet because the coalescer hasn't flushed
    assertNoOutboundFrame();

    // When
    Future<java.lang.Void> closeFuture = driverChannel.close();

    // Then
    // not closed yet because there is still a pending write
    assertThat(closeFuture).isNotDone();
    assertNoOutboundFrame();

    // When
    // the coalescer finally runs
    writeCoalescer.triggerFlush();

    // Then
    // the pending write goes through
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame).isNotNull();
    // not closed yet because there is now a pending response
    assertThat(closeFuture).isNotDone();

    // When
    // the pending response arrives
    writeInboundFrame(requestFrame, Void.INSTANCE);
    assertThat(responseCallback.getLastResponse().message).isEqualTo(Void.INSTANCE);

    // Then
    assertThat(closeFuture).isSuccess();
  }

  /**
   * Ensures that the potential delay introduced by the write coalescer does not mess with the
   * forceful shutdown sequence: any write submitted before {@link DriverChannel#forceClose()}
   * should get the "Channel was force-closed" error, whether it had been flushed or not.
   */
  @Test
  public void should_wait_for_coalesced_writes_when_closing_forcefully() {
    // Given
    MockResponseCallback responseCallback = new MockResponseCallback();
    driverChannel.write(new Query("test"), false, Frame.NO_PAYLOAD, responseCallback);
    // nothing written yet because the coalescer hasn't flushed
    assertNoOutboundFrame();

    // When
    Future<java.lang.Void> closeFuture = driverChannel.forceClose();

    // Then
    // not closed yet because there is still a pending write
    assertThat(closeFuture).isNotDone();
    assertNoOutboundFrame();

    // When
    // the coalescer finally runs
    writeCoalescer.triggerFlush();
    // and the pending write goes through
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame).isNotNull();

    // Then
    assertThat(closeFuture).isSuccess();
    assertThat(responseCallback.getFailure())
        .isInstanceOf(ClosedConnectionException.class)
        .hasMessageContaining("Channel was force-closed");
  }

  // Simple implementation that holds all the writes, and flushes them when it's explicitly
  // triggered.
  private class MockWriteCoalescer implements WriteCoalescer {
    private Queue<Map.Entry<Object, ChannelPromise>> messages = new ArrayDeque<>();

    @Override
    public ChannelFuture writeAndFlush(Channel channel, Object message) {
      assertThat(channel).isEqualTo(DriverChannelTest.this.channel);
      ChannelPromise writePromise = channel.newPromise();
      messages.offer(new AbstractMap.SimpleEntry<>(message, writePromise));
      return writePromise;
    }

    void triggerFlush() {
      for (Map.Entry<Object, ChannelPromise> entry : messages) {
        channel.writeAndFlush(entry.getKey(), entry.getValue());
      }
    }
  }
}
