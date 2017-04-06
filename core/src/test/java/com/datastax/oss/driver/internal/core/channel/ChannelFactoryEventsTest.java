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
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.response.Ready;
import io.netty.channel.local.LocalAddress;
import java.util.concurrent.CompletionStage;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;

public class ChannelFactoryEventsTest extends ChannelFactoryTestBase {

  @Test
  public void should_not_fire_open_event_until_initialization_complete() {
    connect(SERVER_ADDRESS);
    Mockito.verify(eventBus, never()).fire(any(ChannelEvent.class));

    // Clean up
    completeChannelInit();
  }

  @Test
  public void should_fire_open_event_when_initialization_completes() {
    CompletionStage<DriverChannel> connectFuture = connect(SERVER_ADDRESS);
    completeChannelInit();
    assertThat(connectFuture)
        .isSuccess(
            channel ->
                Mockito.verify(eventBus, timeout(100))
                    .fire(new ChannelEvent(ChannelEvent.Type.OPENED, SERVER_ADDRESS)));
  }

  @Test
  public void should_fire_close_event_when_channel_closes() {
    CompletionStage<DriverChannel> connectFuture = connect(SERVER_ADDRESS);
    completeChannelInit();
    assertThat(connectFuture)
        .isSuccess(
            channel ->
                assertThat(channel.close())
                    .isSuccess(
                        (v) ->
                            Mockito.verify(eventBus, timeout(100))
                                .fire(new ChannelEvent(ChannelEvent.Type.CLOSED, SERVER_ADDRESS))));
  }

  private CompletionStage<DriverChannel> connect(LocalAddress address) {
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(true);
    Mockito.when(defaultConfigProfile.getString(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn("V4");
    Mockito.when(protocolVersionRegistry.fromName("V4")).thenReturn(CoreProtocolVersion.V4);

    return newChannelFactory().connect(address, null);
  }

  private void completeChannelInit() {
    Frame requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));
  }
}
