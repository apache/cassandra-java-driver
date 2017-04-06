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
import com.datastax.oss.protocol.internal.response.Ready;
import java.util.concurrent.CompletionStage;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class ChannelFactoryClusterNameTest extends ChannelFactoryTestBase {

  @Test
  public void should_set_cluster_name_from_first_connection() {
    // Given
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(false);
    Mockito.when(protocolVersionRegistry.highestNonBeta()).thenReturn(CoreProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture = factory.connect(SERVER_ADDRESS, null, false);

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThat(channelFuture).isSuccess();
    assertThat(factory.clusterName).isEqualTo("mockClusterName");
  }

  @Test
  public void should_check_cluster_name_for_next_connections() throws Throwable {
    // Given
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(false);
    Mockito.when(protocolVersionRegistry.highestNonBeta()).thenReturn(CoreProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture = factory.connect(SERVER_ADDRESS, null, false);
    // open a first connection that will define the cluster name
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));
    assertThat(channelFuture).isSuccess();
    // open a second connection that returns the same cluster name
    channelFuture = factory.connect(SERVER_ADDRESS, null, false);
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThat(channelFuture).isSuccess();

    // When
    // open a third connection that returns a different cluster name
    channelFuture = factory.connect(SERVER_ADDRESS, null, false);
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("wrongClusterName"));

    // Then
    assertThat(channelFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(ClusterNameMismatchException.class)
                    .hasMessageContaining(
                        "reports cluster name 'wrongClusterName' that doesn't match "
                            + "our cluster name 'mockClusterName'."));
  }
}
