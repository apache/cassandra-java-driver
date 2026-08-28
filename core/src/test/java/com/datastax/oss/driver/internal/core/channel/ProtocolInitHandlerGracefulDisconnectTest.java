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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.DefaultProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TestNodeFactory;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Coverage for GRACEFUL_DISCONNECT (CEP-59) registration during channel initialization: support is
 * checked against each channel's own SUPPORTED response, and the event type is only included in
 * REGISTER if the server advertises it.
 */
public class ProtocolInitHandlerGracefulDisconnectTest extends ChannelHandlerTestBase {

  private static final long QUERY_TIMEOUT_MILLIS = 100L;
  private static final EndPoint END_POINT = TestNodeFactory.newEndPoint(1);
  private static final Supported SUPPORTED_WITH_GRACEFUL_DISCONNECT =
      new Supported(
          ImmutableMap.of(
              ProtocolConstants.EventType.GRACEFUL_DISCONNECT,
              ImmutableList.of("true"),
              "CQL_VERSION",
              ImmutableList.of("3.4.7")));
  private static final Supported SUPPORTED_WITHOUT_GRACEFUL_DISCONNECT =
      new Supported(ImmutableMap.of("CQL_VERSION", ImmutableList.of("3.4.7")));

  @Mock private InternalDriverContext internalDriverContext;
  @Mock private DriverConfig driverConfig;
  @Mock private DriverExecutionProfile defaultProfile;

  private final ProtocolVersionRegistry protocolVersionRegistry =
      new DefaultProtocolVersionRegistry("test");
  private HeartbeatHandler heartbeatHandler;

  @Before
  @Override
  public void setup() {
    super.setup();
    MockitoAnnotations.initMocks(this);
    when(internalDriverContext.getConfig()).thenReturn(driverConfig);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .thenReturn(Duration.ofMillis(QUERY_TIMEOUT_MILLIS));
    when(defaultProfile.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL))
        .thenReturn(Duration.ofSeconds(30));
    when(internalDriverContext.getProtocolVersionRegistry()).thenReturn(protocolVersionRegistry);

    channel
        .pipeline()
        .addLast(
            ChannelFactory.INFLIGHT_HANDLER_NAME,
            new InFlightHandler(
                DefaultProtocolVersion.V4,
                new StreamIdGenerator(100),
                Integer.MAX_VALUE,
                100,
                channel.newPromise(),
                null,
                "test"));

    heartbeatHandler = new HeartbeatHandler(defaultProfile);
  }

  private ChannelFuture connectWithEvents(List<String> eventTypes) {
    DriverChannelOptions driverChannelOptions =
        DriverChannelOptions.builder().withEvents(eventTypes, mock(EventCallback.class)).build();
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                driverChannelOptions,
                heartbeatHandler,
                true));
    return channel.connect(new InetSocketAddress("localhost", 9042));
  }

  /** Completes the OPTIONS and STARTUP steps. */
  private void initUntilAfterClusterName(Supported supportedResponse) {
    Frame optionsFrame = readOutboundFrame();
    assertThat(optionsFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(optionsFrame, supportedResponse);
    Frame startupFrame = readOutboundFrame();
    assertThat(startupFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(startupFrame, new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("someClusterName"));
  }

  @Test
  public void should_register_graceful_disconnect_when_advertised_in_supported() {
    ChannelFuture connectFuture =
        connectWithEvents(
            ImmutableList.of("STATUS_CHANGE", ProtocolConstants.EventType.GRACEFUL_DISCONNECT));

    initUntilAfterClusterName(SUPPORTED_WITH_GRACEFUL_DISCONNECT);
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);

    assertThat(((Register) registerFrame.message).eventTypes)
        .containsExactly("STATUS_CHANGE", ProtocolConstants.EventType.GRACEFUL_DISCONNECT);
    writeInboundFrame(registerFrame, new Ready());
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_not_register_graceful_disconnect_when_server_does_not_advertise_it() {
    ChannelFuture connectFuture =
        connectWithEvents(
            ImmutableList.of("STATUS_CHANGE", ProtocolConstants.EventType.GRACEFUL_DISCONNECT));

    initUntilAfterClusterName(SUPPORTED_WITHOUT_GRACEFUL_DISCONNECT);
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);

    assertThat(((Register) registerFrame.message).eventTypes).containsExactly("STATUS_CHANGE");
    writeInboundFrame(registerFrame, new Ready());
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_not_register_graceful_disconnect_when_advertised_as_false() {
    ChannelFuture connectFuture =
        connectWithEvents(
            ImmutableList.of("STATUS_CHANGE", ProtocolConstants.EventType.GRACEFUL_DISCONNECT));

    initUntilAfterClusterName(
        new Supported(
            ImmutableMap.of(
                ProtocolConstants.EventType.GRACEFUL_DISCONNECT,
                ImmutableList.of("false"),
                "CQL_VERSION",
                ImmutableList.of("3.4.7"))));
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);

    assertThat(((Register) registerFrame.message).eventTypes).containsExactly("STATUS_CHANGE");
    writeInboundFrame(registerFrame, new Ready());
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_skip_register_when_graceful_disconnect_was_the_only_event_type() {
    // Pool channels only register for GRACEFUL_DISCONNECT; if the server does not support it,
    // there is nothing left to register for.
    ChannelFuture connectFuture =
        connectWithEvents(ImmutableList.of(ProtocolConstants.EventType.GRACEFUL_DISCONNECT));

    initUntilAfterClusterName(SUPPORTED_WITHOUT_GRACEFUL_DISCONNECT);

    assertThat(connectFuture).isSuccess();
    assertThat((Object) channel.readOutbound()).isNull();
  }

  @Test
  public void should_detect_capability_from_supported_options_map() {
    assertThat(ProtocolInitHandler.supportsGracefulDisconnect(null)).isFalse();
    assertThat(ProtocolInitHandler.supportsGracefulDisconnect(ImmutableMap.of())).isFalse();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of("CQL_VERSION", ImmutableList.of("3.4.7"))))
        .isFalse();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(
                    ProtocolConstants.EventType.GRACEFUL_DISCONNECT, ImmutableList.of())))
        .isTrue();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(
                    ProtocolConstants.EventType.GRACEFUL_DISCONNECT, ImmutableList.of("true"))))
        .isTrue();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(
                    ProtocolConstants.EventType.GRACEFUL_DISCONNECT, ImmutableList.of("false"))))
        .isFalse();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(
                    ProtocolConstants.EventType.GRACEFUL_DISCONNECT, ImmutableList.of("FALSE"))))
        .isFalse();
  }
}
