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

import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.protocol.internal.response.Ready;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class ChannelFactorySupportedOptionsTest extends ChannelFactoryTestBase {

  @Test
  public void should_query_supported_options_on_first_channel() throws Throwable {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture1 =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);
    writeInboundFrame(
        readOutboundFrame(), TestResponses.supportedResponse("mock_key", "mock_value"));
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThatStage(channelFuture1).isSuccess();
    DriverChannel channel1 = channelFuture1.toCompletableFuture().get();
    assertThat(channel1.getOptions()).containsKey("mock_key");
    assertThat(channel1.getOptions().get("mock_key")).containsOnly("mock_value");

    // When
    CompletionStage<DriverChannel> channelFuture2 =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThatStage(channelFuture2).isSuccess();
    DriverChannel channel2 = channelFuture2.toCompletableFuture().get();
    assertThat(channel2.getOptions()).isNull();
  }
}
