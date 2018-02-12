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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.Void;
import io.netty.util.concurrent.Future;
import java.util.concurrent.CompletionStage;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ChannelFactoryAvailableIdsTest extends ChannelFactoryTestBase {

  @Mock private ResponseCallback responseCallback;

  @Before
  @Override
  public void setup() throws InterruptedException {
    super.setup();
    Mockito.when(defaultConfigProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION))
        .thenReturn(true);
    Mockito.when(defaultConfigProfile.getString(DefaultDriverOption.PROTOCOL_VERSION))
        .thenReturn("V4");
    Mockito.when(protocolVersionRegistry.fromName("V4")).thenReturn(DefaultProtocolVersion.V4);

    Mockito.when(defaultConfigProfile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS))
        .thenReturn(128);
  }

  @Test
  public void should_report_available_ids() {
    // Given
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, DriverChannelOptions.builder().build());
    completeSimpleChannelInit();

    // Then
    assertThat(channelFuture)
        .isSuccess(
            channel -> {
              assertThat(channel.getAvailableIds()).isEqualTo(128);

              // Write a request, should decrease the count
              Future<java.lang.Void> writeFuture =
                  channel.write(new Query("test"), false, Frame.NO_PAYLOAD, responseCallback);
              assertThat(writeFuture)
                  .isSuccess(
                      v -> {
                        assertThat(channel.getAvailableIds()).isEqualTo(127);

                        // Complete the request, should increase again
                        writeInboundFrame(readOutboundFrame(), Void.INSTANCE);
                        Mockito.verify(responseCallback, timeout(500)).onResponse(any(Frame.class));
                        assertThat(channel.getAvailableIds()).isEqualTo(128);
                      });
            });
  }
}
