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

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import java.net.InetSocketAddress;
import org.junit.Before;
import org.junit.Test;

public class ConnectInitHandlerTest extends ChannelHandlerTestBase {

  private TestHandler handler;

  @Before
  @Override
  public void setup() {
    super.setup();
    handler = new TestHandler();
    channel.pipeline().addLast(handler);
  }

  @Test
  public void should_call_onRealConnect_when_connection_succeeds() {
    assertThat(handler.hasConnected).isFalse();

    // When
    channel.connect(new InetSocketAddress("localhost", 9042));

    // Then
    assertThat(handler.hasConnected).isTrue();
  }

  @Test
  public void should_not_complete_connect_future_before_triggered_by_handler() {
    // When
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    // Then
    assertThat(connectFuture.isDone()).isFalse();
  }

  @Test
  public void should_complete_connect_future_when_handler_completes() {
    // Given
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    // When
    handler.setConnectSuccess();

    // Then
    assertThat(connectFuture.isSuccess()).isTrue();
  }

  @Test
  public void should_remove_handler_from_pipeline_when_handler_completes() {
    // Given
    channel.connect(new InetSocketAddress("localhost", 9042));

    // When
    handler.setConnectSuccess();

    // Then
    assertThat(channel.pipeline().get(TestHandler.class)).isNull();
  }

  @Test
  public void should_fail_connect_future_when_handler_fails() {
    // Given
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));
    Exception exception = new Exception("test");

    // When
    handler.setConnectFailure(exception);

    // Then
    assertThat(connectFuture).isFailed(e -> assertThat(e).isEqualTo(exception));
  }

  /**
   * Well-behaved implementations should not call setConnect* multiple times in a row, but check
   * that we handle it gracefully if they do.
   */
  @Test
  public void should_ignore_subsequent_calls_if_handler_already_failed() {
    // Given
    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));
    Exception exception = new Exception("test");

    // When
    handler.setConnectFailure(exception);
    handler.setConnectFailure(new Exception("test2"));
    handler.setConnectSuccess();

    // Then
    assertThat(connectFuture).isFailed(e -> assertThat(e).isEqualTo(exception));
  }

  static class TestHandler extends ConnectInitHandler {
    boolean hasConnected;

    @Override
    protected void onRealConnect(ChannelHandlerContext ctx) {
      hasConnected = true;
    }
  }
}
