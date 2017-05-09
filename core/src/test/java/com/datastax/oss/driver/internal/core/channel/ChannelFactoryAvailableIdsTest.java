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
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.Void;
import io.netty.util.concurrent.Future;
import java.util.concurrent.CompletionStage;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;

public class ChannelFactoryAvailableIdsTest extends ChannelFactoryTestBase {

  @Mock private ResponseCallback responseCallback;

  @BeforeMethod
  public void setup() throws InterruptedException {
    super.setup();
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(true);
    Mockito.when(defaultConfigProfile.getString(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn("V4");
    Mockito.when(protocolVersionRegistry.fromName("V4")).thenReturn(CoreProtocolVersion.V4);

    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.CONNECTION_MAX_REQUESTS))
        .thenReturn(128);
  }

  @Test
  public void should_report_available_ids_if_requested() {
    // Given
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(node, DriverChannelOptions.builder().reportAvailableIds(true).build());
    completeSimpleChannelInit();

    // Then
    assertThat(channelFuture)
        .isSuccess(
            channel -> {
              assertThat(channel.availableIds()).isEqualTo(128);

              // Write a request, should decrease the count
              Future<java.lang.Void> writeFuture =
                  channel.write(new Query("test"), false, Frame.NO_PAYLOAD, responseCallback);
              assertThat(writeFuture)
                  .isSuccess(
                      v -> {
                        assertThat(channel.availableIds()).isEqualTo(127);

                        // Complete the request, should increase again
                        writeInboundFrame(readOutboundFrame(), Void.INSTANCE);
                        Mockito.verify(responseCallback, timeout(100))
                            .onResponse(any(Frame.class), any(Node.class));
                        assertThat(channel.availableIds()).isEqualTo(128);
                      });
            });
  }

  @Test
  public void should_not_report_available_ids_if_not_requested() {
    // Given
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(node, DriverChannelOptions.DEFAULT);
    completeSimpleChannelInit();

    // Then
    assertThat(channelFuture)
        .isSuccess(
            channel -> {
              assertThat(channel.availableIds()).isEqualTo(-1);

              // Write a request, complete it, count should never be updated
              Future<java.lang.Void> writeFuture =
                  channel.write(new Query("test"), false, Frame.NO_PAYLOAD, responseCallback);
              assertThat(writeFuture)
                  .isSuccess(
                      v -> {
                        assertThat(channel.availableIds()).isEqualTo(-1);

                        writeInboundFrame(readOutboundFrame(), Void.INSTANCE);
                        Mockito.verify(responseCallback, timeout(100))
                            .onResponse(any(Frame.class), any(Node.class));
                        assertThat(channel.availableIds()).isEqualTo(-1);
                      });
            });
  }
}
