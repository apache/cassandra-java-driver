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
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.UseDataProvider;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.junit.Test;

public class ChannelFactoryProtocolNegotiationTest extends ChannelFactoryTestBase {

  @Test
  public void should_succeed_if_version_specified_and_supported_by_server() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn("V4");
    when(protocolVersionRegistry.fromName("V4")).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    completeSimpleChannelInit();

    // Then
    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(DefaultProtocolVersion.V4);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_fail_if_version_specified_and_not_supported_by_server(int errorCode) {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn("V4");
    when(protocolVersionRegistry.fromName("V4")).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining("Host does not support protocol version V4");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V4);
            });
  }

  @Test
  public void should_fail_if_version_specified_and_considered_beta_by_server() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(true);
    when(defaultProfile.getString(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn("V5");
    when(protocolVersionRegistry.fromName("V5")).thenReturn(DefaultProtocolVersion.V5);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V5.getCode());
    // Server considers v5 beta, e.g. C* 3.10 or 3.11
    writeInboundFrame(
        requestFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
            "Beta version of the protocol used (5/v5-beta), but USE_BETA flag is unset"));

    // Then
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining("Host does not support protocol version V5");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V5);
            });
  }

  @Test
  public void should_succeed_if_version_not_specified_and_server_supports_latest_supported() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));

    // Then
    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(DefaultProtocolVersion.V4);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_negotiate_if_version_not_specified_and_server_supports_legacy(int errorCode) {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V4))
        .thenReturn(Optional.of(DefaultProtocolVersion.V3));
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    // Factory should initialize a new connection, that retries with the lower version
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V3.getCode());
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));
    assertThatStage(channelFuture)
        .isSuccess(channel -> assertThat(channel.getClusterName()).isEqualTo("mockClusterName"));
    assertThat(factory.protocolVersion).isEqualTo(DefaultProtocolVersion.V3);
  }

  @Test
  @UseDataProvider("unsupportedProtocolCodes")
  public void should_fail_if_negotiation_finds_no_matching_version(int errorCode) {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(false);
    when(protocolVersionRegistry.highestNonBeta()).thenReturn(DefaultProtocolVersion.V4);
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V4))
        .thenReturn(Optional.of(DefaultProtocolVersion.V3));
    when(protocolVersionRegistry.downgrade(DefaultProtocolVersion.V3)).thenReturn(Optional.empty());
    ChannelFactory factory = newChannelFactory();

    // When
    CompletionStage<DriverChannel> channelFuture =
        factory.connect(
            SERVER_ADDRESS, DriverChannelOptions.DEFAULT, NoopNodeMetricUpdater.INSTANCE);

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V4.getCode());
    // Server does not support v4
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Client retries with v3
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.protocolVersion).isEqualTo(DefaultProtocolVersion.V3.getCode());
    // Server does not support v3
    writeInboundFrame(
        requestFrame, new Error(errorCode, "Invalid or unsupported protocol version"));

    // Then
    assertThatStage(channelFuture)
        .isFailed(
            e -> {
              assertThat(e)
                  .isInstanceOf(UnsupportedProtocolVersionException.class)
                  .hasMessageContaining(
                      "Protocol negotiation failed: could not find a common version "
                          + "(attempted: [V4, V3])");
              assertThat(((UnsupportedProtocolVersionException) e).getAttemptedVersions())
                  .containsExactly(DefaultProtocolVersion.V4, DefaultProtocolVersion.V3);
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
