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
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.Test;
import org.mockito.Mockito;

import static com.datastax.oss.driver.Assertions.assertThat;

public class ChannelFactoryProtocolNegotiationTest extends ChannelFactoryTestBase {

  @Test
  public void should_succeed_if_version_specified_and_supported_by_server() {
    // Given
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(true);
    Mockito.when(defaultConfigProfile.getString(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn("V4");
    Mockito.when(protocolVersionRegistry.fromName("V4")).thenReturn(CoreProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, DriverChannelOptions.DEFAULT);

    completeSimpleChannelInit();

    // Then
    assertThat(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(CoreProtocolVersion.V4);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_fail_if_version_specified_and_not_supported_by_server(int errorCode) {
    // Given
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(true);
    Mockito.when(defaultConfigProfile.getString(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn("V4");
    Mockito.when(protocolVersionRegistry.fromName("V4")).thenReturn(CoreProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, DriverChannelOptions.DEFAULT);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(CoreProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    assertThat(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining("Host does not support protocol version V4");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(CoreProtocolVersion.V4);
            });
  }

  @Test
  public void should_succeed_if_version_not_specified_and_server_supports_latest_supported() {
    // Given
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(false);
    Mockito.when(protocolVersionRegistry.highestNonBeta()).thenReturn(CoreProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, DriverChannelOptions.DEFAULT);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(CoreProtocolVersion.V4.getCode());
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThat(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(CoreProtocolVersion.V4);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_negotiate_if_version_not_specified_and_server_supports_legacy(int errorCode) {
    // Given
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(false);
    Mockito.when(protocolVersionRegistry.highestNonBeta()).thenReturn(CoreProtocolVersion.V4);
    Mockito.when(protocolVersionRegistry.downgrade(CoreProtocolVersion.V4))
        .thenReturn(Optional.of(CoreProtocolVersion.V3));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, DriverChannelOptions.DEFAULT);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(CoreProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    // Factory should initialize a new connection, that retries with the lower version
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(CoreProtocolVersion.V3.getCode());
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));
    assertThat(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(CoreProtocolVersion.V3);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_fail_if_negotiation_finds_no_matching_version(int errorCode) {
    // Given
    Mockito.when(defaultConfigProfile.isDefined(CoreDriverOption.PROTOCOL_VERSION))
        .thenReturn(false);
    Mockito.when(protocolVersionRegistry.highestNonBeta()).thenReturn(CoreProtocolVersion.V4);
    Mockito.when(protocolVersionRegistry.downgrade(CoreProtocolVersion.V4))
        .thenReturn(Optional.of(CoreProtocolVersion.V3));
    Mockito.when(protocolVersionRegistry.downgrade(CoreProtocolVersion.V3))
        .thenReturn(Optional.empty());
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(SERVER_ADDRESS, DriverChannelOptions.DEFAULT);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(CoreProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Client retries with v3
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(CoreProtocolVersion.V3.getCode());
    // Server does not support v3
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    assertThat(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining(
                      "Protocol negotiation failed: could not find a common version "
                          + "(attempted: [V4, V3])");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(CoreProtocolVersion.V4, CoreProtocolVersion.V3);
            });
  }

  /**
   * Depending on the Cassandra version, an "unsupported protocol" response can use different error
   * codes, so we test all of them.
   */
  @DataProvider
  public static Object[][] unsupportedProtocolCodes() {
    return new Object[][] {
      new Object[] {ProtocolConstants.ErrorCode.PROTOCOL_ERROR},
      // C* 2.1 reports a server error instead of protocol error, see CASSANDRA-9451.
      new Object[] {ProtocolConstants.ErrorCode.SERVER_ERROR}
    };
  }
}
