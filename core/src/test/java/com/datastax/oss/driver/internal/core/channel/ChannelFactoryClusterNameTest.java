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
package com.datastax.oss.driver.internal.core.channel;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.protocol.internal.response.Ready;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class ChannelFactoryClusterNameTest extends ChannelFactoryTestBase {

  @Test
  public void should_set_cluster_name_from_first_connection() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    writeInboundFrame(
        readOutboundFrame(), TestResponses.supportedResponse("mock_key", "mock_value"));
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThatStage(channelFuture).isSuccess();
    assertThat(factory.getClusterName()).isEqualTo("mockClusterName");
  }

  @Test
  public void should_check_cluster_name_for_next_connections() throws Throwable {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);
    // open a first connection that will define the cluster name
    writeInboundFrame(
        readOutboundFrame(), TestResponses.supportedResponse("mock_key", "mock_value"));
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));
    assertThatStage(channelFuture).isSuccess();
    // open a second connection that returns the same cluster name
    channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThatStage(channelFuture).isSuccess();

    // When
    // open a third connection that returns a different cluster name
    channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);
    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("wrongClusterName"));

    // Then
    assertThatStage(channelFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(ClusterNameMismatchException.class)
                    .hasMessageContaining(
                        "reports cluster name 'wrongClusterName' that doesn't match "
                            + "our cluster name 'mockClusterName'."));
  }
}
