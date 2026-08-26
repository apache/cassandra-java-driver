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
import com.datastax.oss.protocol.internal.response.Error;
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
 * Coverage for the driver's tolerance to CEP-59 server-side variations during channel
 * initialization.
 *
 * <p>Capabilities are negotiated per connection: each channel checks its own SUPPORTED response
 * (nodes in a mixed-version cluster may differ). These tests pin down how the driver must behave
 * when the server:
 *
 * <ul>
 *   <li>advertises the capability in SUPPORTED (registers for the event),
 *   <li>does not advertise it, or advertises it with an explicit {@code false} value (never
 *       registers, so old or disabled servers never see an unknown event type),
 *   <li>rejects a REGISTER that includes the event type (degrades by retrying without it instead of
 *       failing the connection — the mixed-version-cluster / rolling-upgrade case).
 * </ul>
 */
public class ProtocolInitHandlerGracefulDisconnectTest extends ChannelHandlerTestBase {

  private static final long QUERY_TIMEOUT_MILLIS = 100L;
  private static final EndPoint END_POINT = TestNodeFactory.newEndPoint(1);
  private static final Supported SUPPORTED_WITH_GRACEFUL_DISCONNECT =
      new Supported(
          ImmutableMap.of(
              GracefulDisconnectEvent.EVENT_TYPE,
              ImmutableList.of("true"),
              "CQL_VERSION",
              ImmutableList.of("3.4.7")));

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

  private ChannelFuture connectWithEvents(boolean querySupportedOptions) {
    DriverChannelOptions driverChannelOptions =
        DriverChannelOptions.builder()
            .withEvents(
                ImmutableList.of("STATUS_CHANGE", GracefulDisconnectEvent.EVENT_TYPE),
                mock(EventCallback.class))
            .build();
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
                querySupportedOptions));
    return channel.connect(new InetSocketAddress("localhost", 9042));
  }

  /** Completes the OPTIONS and STARTUP steps, then returns the outbound REGISTER frame. */
  private Frame initUntilRegister(Supported supportedResponse) {
    if (supportedResponse != null) {
      Frame optionsFrame = readOutboundFrame();
      assertThat(optionsFrame.message).isInstanceOf(Options.class);
      writeInboundFrame(optionsFrame, supportedResponse);
    }
    Frame startupFrame = readOutboundFrame();
    assertThat(startupFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(startupFrame, new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("someClusterName"));
    Frame registerFrame = readOutboundFrame();
    assertThat(registerFrame.message).isInstanceOf(Register.class);
    return registerFrame;
  }

  @Test
  public void should_register_graceful_disconnect_when_advertised_in_supported() {
    ChannelFuture connectFuture = connectWithEvents(true);

    Frame registerFrame = initUntilRegister(SUPPORTED_WITH_GRACEFUL_DISCONNECT);

    List<String> eventTypes = ((Register) registerFrame.message).eventTypes;
    assertThat(eventTypes).containsExactly("STATUS_CHANGE", GracefulDisconnectEvent.EVENT_TYPE);
    writeInboundFrame(registerFrame, new Ready());
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_not_register_graceful_disconnect_when_server_does_not_advertise_it() {
    ChannelFuture connectFuture = connectWithEvents(true);

    Frame registerFrame =
        initUntilRegister(new Supported(ImmutableMap.of("CQL_VERSION", ImmutableList.of("3.4.7"))));

    List<String> eventTypes = ((Register) registerFrame.message).eventTypes;
    assertThat(eventTypes).containsExactly("STATUS_CHANGE");
    writeInboundFrame(registerFrame, new Ready());
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_not_register_graceful_disconnect_when_advertised_as_false() {
    // The pre-STARTUP OPTIONS path on the server sends the key with an explicit "false" value
    // when the feature is disabled.
    ChannelFuture connectFuture = connectWithEvents(true);

    Frame registerFrame =
        initUntilRegister(
            new Supported(
                ImmutableMap.of(
                    GracefulDisconnectEvent.EVENT_TYPE,
                    ImmutableList.of("false"),
                    "CQL_VERSION",
                    ImmutableList.of("3.4.7"))));

    List<String> eventTypes = ((Register) registerFrame.message).eventTypes;
    assertThat(eventTypes).containsExactly("STATUS_CHANGE");
    writeInboundFrame(registerFrame, new Ready());
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_not_register_graceful_disconnect_when_options_not_queried() {
    // Capability is strictly per-connection: if for any reason the channel did not run the
    // OPTIONS step, it must be conservative and not register for the event.
    ChannelFuture connectFuture = connectWithEvents(false);

    Frame registerFrame = initUntilRegister(null);

    List<String> eventTypes = ((Register) registerFrame.message).eventTypes;
    assertThat(eventTypes).containsExactly("STATUS_CHANGE");
    writeInboundFrame(registerFrame, new Ready());
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_retry_register_without_graceful_disconnect_when_server_rejects_it() {
    // Simulates a node that advertises the capability but rejects the event type (e.g. the
    // still-evolving server implementation changed the wire contract): the driver must degrade
    // (lose graceful disconnect on this connection) instead of failing channel init.
    ChannelFuture connectFuture = connectWithEvents(true);

    Frame registerFrame = initUntilRegister(SUPPORTED_WITH_GRACEFUL_DISCONNECT);
    assertThat(((Register) registerFrame.message).eventTypes)
        .contains(GracefulDisconnectEvent.EVENT_TYPE);
    writeInboundFrame(
        registerFrame,
        new Error(
            ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
            "Invalid value 'GRACEFUL_DISCONNECT' for Type"));

    // The driver retries REGISTER without the unsupported event type:
    Frame retryFrame = readOutboundFrame();
    assertThat(retryFrame.message).isInstanceOf(Register.class);
    assertThat(((Register) retryFrame.message).eventTypes).containsExactly("STATUS_CHANGE");
    writeInboundFrame(retryFrame, new Ready());

    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_fail_when_register_rejected_even_without_graceful_disconnect() {
    // The degradation retry must not loop: if the server keeps rejecting REGISTER after
    // GRACEFUL_DISCONNECT was removed, fail the connection like any other unexpected error.
    ChannelFuture connectFuture = connectWithEvents(true);

    Frame registerFrame = initUntilRegister(SUPPORTED_WITH_GRACEFUL_DISCONNECT);
    writeInboundFrame(
        registerFrame, new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, "Invalid event type"));

    Frame retryFrame = readOutboundFrame();
    assertThat(((Register) retryFrame.message).eventTypes)
        .doesNotContain(GracefulDisconnectEvent.EVENT_TYPE);
    writeInboundFrame(
        retryFrame, new Error(ProtocolConstants.ErrorCode.PROTOCOL_ERROR, "Invalid event type"));

    assertThat(connectFuture).isFailed();
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
                ImmutableMap.of(GracefulDisconnectEvent.EVENT_TYPE, ImmutableList.of())))
        .isTrue();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(GracefulDisconnectEvent.EVENT_TYPE, ImmutableList.of("true"))))
        .isTrue();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(GracefulDisconnectEvent.EVENT_TYPE, ImmutableList.of("false"))))
        .isFalse();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(GracefulDisconnectEvent.EVENT_TYPE, ImmutableList.of("FALSE"))))
        .isFalse();
    assertThat(
            ProtocolInitHandler.supportsGracefulDisconnect(
                ImmutableMap.of(
                    GracefulDisconnectEvent.EVENT_TYPE, ImmutableList.of("true", "false"))))
        .isFalse();
  }
}
