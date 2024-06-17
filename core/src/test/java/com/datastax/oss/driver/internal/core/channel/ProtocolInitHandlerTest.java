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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
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
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.channel.ChannelFuture;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

public class ProtocolInitHandlerTest extends ChannelHandlerTestBase {

  private static final long QUERY_TIMEOUT_MILLIS = 100L;
  // The handled only uses this to call the auth provider and for exception messages, so the actual
  // value doesn't matter:
  private static final EndPoint END_POINT = TestNodeFactory.newEndPoint(1);

  @Mock private InternalDriverContext internalDriverContext;
  @Mock private DriverConfig driverConfig;
  @Mock private DriverExecutionProfile defaultProfile;
  @Mock private Appender<ILoggingEvent> appender;

  private ProtocolVersionRegistry protocolVersionRegistry =
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

  @Test
  public void should_initialize() {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    // It should send a STARTUP message
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    assertThat(connectFuture).isNotDone();

    // Simulate a READY response
    writeInboundFrame(buildInboundFrame(requestFrame, new Ready()));

    // Simulate the cluster name check
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("someClusterName"));

    // Init should complete
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_query_supported_options() {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                true));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    // It should send an OPTIONS message
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    assertThat(connectFuture).isNotDone();

    // Simulate the SUPPORTED response
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    Map<String, List<String>> supportedOptions = channel.attr(DriverChannel.OPTIONS_KEY).get();
    assertThat(supportedOptions).containsKey("mock_key");
    assertThat(supportedOptions.get("mock_key")).containsOnly("mock_value");

    // It should send a STARTUP message
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    assertThat(connectFuture).isNotDone();

    // Simulate a READY response
    writeInboundFrame(buildInboundFrame(requestFrame, new Ready()));

    // Simulate the cluster name check
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("someClusterName"));

    // Init should complete
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_add_heartbeat_handler_to_pipeline_on_success() {
    ProtocolInitHandler protocolInitHandler =
        new ProtocolInitHandler(
            internalDriverContext,
            DefaultProtocolVersion.V4,
            null,
            END_POINT,
            DriverChannelOptions.DEFAULT,
            heartbeatHandler,
            false);

    channel.pipeline().addLast(ChannelFactory.INIT_HANDLER_NAME, protocolInitHandler);

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    // heartbeat should initially not be in pipeline
    assertThat(channel.pipeline().get(ChannelFactory.HEARTBEAT_HANDLER_NAME)).isNull();

    // It should send a STARTUP message
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    assertThat(connectFuture).isNotDone();

    // Simulate a READY response
    writeInboundFrame(buildInboundFrame(requestFrame, new Ready()));

    // Simulate the cluster name check
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("someClusterName"));

    // Init should complete
    assertThat(connectFuture).isSuccess();

    // should have added heartbeat handler to pipeline.
    assertThat(channel.pipeline().get(ChannelFactory.HEARTBEAT_HANDLER_NAME))
        .isEqualTo(heartbeatHandler);
    // should have removed itself from pipeline.
    assertThat(channel.pipeline().last()).isNotEqualTo(protocolInitHandler);
  }

  @Test
  public void should_fail_to_initialize_if_init_query_times_out() throws InterruptedException {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    readOutboundFrame();

    // Simulate a pause longer than the timeout
    TimeUnit.MILLISECONDS.sleep(QUERY_TIMEOUT_MILLIS * 2);
    channel.runPendingTasks();

    assertThat(connectFuture).isFailed();
  }

  @Test
  public void should_initialize_with_authentication() {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    String serverAuthenticator = "mockServerAuthenticator";
    AuthProvider authProvider = mock(AuthProvider.class);
    MockAuthenticator authenticator = new MockAuthenticator();
    when(authProvider.newAuthenticator(END_POINT, serverAuthenticator)).thenReturn(authenticator);
    when(internalDriverContext.getAuthProvider()).thenReturn(Optional.of(authProvider));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    assertThat(connectFuture).isNotDone();

    // Simulate a response that says that the server requires authentication
    writeInboundFrame(requestFrame, new Authenticate(serverAuthenticator));

    // The connection should have created an authenticator from the auth provider
    verify(authProvider).newAuthenticator(END_POINT, serverAuthenticator);

    // And sent an auth response
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(AuthResponse.class);
    AuthResponse authResponse = (AuthResponse) requestFrame.message;
    assertThat(Bytes.toHexString(authResponse.token)).isEqualTo(MockAuthenticator.INITIAL_RESPONSE);
    assertThat(connectFuture).isNotDone();

    // As long as the server sends an auth challenge, the client should reply with another
    // auth_response
    String mockToken = "0xabcd";
    for (int i = 0; i < 5; i++) {
      writeInboundFrame(requestFrame, new AuthChallenge(Bytes.fromHexString(mockToken)));

      requestFrame = readOutboundFrame();
      assertThat(requestFrame.message).isInstanceOf(AuthResponse.class);
      authResponse = (AuthResponse) requestFrame.message;
      // Our mock impl happens to send back the same token
      assertThat(Bytes.toHexString(authResponse.token)).isEqualTo(mockToken);
      assertThat(connectFuture).isNotDone();
    }

    // When the server finally sends back a success message, should proceed to the cluster name
    // check and succeed
    writeInboundFrame(requestFrame, new AuthSuccess(Bytes.fromHexString(mockToken)));
    assertThat(authenticator.successToken).isEqualTo(mockToken);

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("someClusterName"));

    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_invoke_auth_provider_when_server_does_not_send_challenge() {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    AuthProvider authProvider = mock(AuthProvider.class);
    when(internalDriverContext.getAuthProvider()).thenReturn(Optional.of(authProvider));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);

    // Simulate a READY response, the provider should be notified
    writeInboundFrame(buildInboundFrame(requestFrame, new Ready()));
    verify(authProvider).onMissingChallenge(END_POINT);

    // Since our mock does nothing, init should proceed normally
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("someClusterName"));
    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_fail_to_initialize_if_server_sends_auth_error() throws Throwable {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    String serverAuthenticator = "mockServerAuthenticator";
    AuthProvider authProvider = mock(AuthProvider.class);
    MockAuthenticator authenticator = new MockAuthenticator();
    when(authProvider.newAuthenticator(END_POINT, serverAuthenticator)).thenReturn(authenticator);
    when(internalDriverContext.getAuthProvider()).thenReturn(Optional.of(authProvider));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    assertThat(connectFuture).isNotDone();

    writeInboundFrame(requestFrame, new Authenticate("mockServerAuthenticator"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(AuthResponse.class);
    assertThat(connectFuture).isNotDone();

    writeInboundFrame(
        requestFrame, new Error(ProtocolConstants.ErrorCode.AUTH_ERROR, "mock error"));

    assertThat(connectFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(AuthenticationException.class)
                    .hasMessage(
                        String.format(
                            "Authentication error on node %s: server replied with 'mock error' to AuthResponse request",
                            END_POINT)));
  }

  @Test
  public void should_check_cluster_name_if_provided() {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                "expectedClusterName",
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    Frame requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    Query query = (Query) requestFrame.message;
    assertThat(query.query).isEqualTo("SELECT cluster_name FROM system.local");
    assertThat(connectFuture).isNotDone();

    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("expectedClusterName"));

    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_fail_to_initialize_if_cluster_name_does_not_match() throws Throwable {
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                "expectedClusterName",
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(
        readOutboundFrame(), TestResponses.clusterNameResponse("differentClusterName"));

    assertThat(connectFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(ClusterNameMismatchException.class)
                    .hasMessageContaining(
                        String.format(
                            "Node %s reports cluster name 'differentClusterName' that doesn't match our cluster name 'expectedClusterName'.",
                            END_POINT)));
  }

  @Test
  public void should_initialize_with_keyspace() {
    DriverChannelOptions options =
        DriverChannelOptions.builder().withKeyspace(CqlIdentifier.fromCql("ks")).build();
    channel
        .pipeline()
        .addLast(
            ChannelFactory.INIT_HANDLER_NAME,
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                options,
                heartbeatHandler,
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("someClusterName"));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    assertThat(((Query) requestFrame.message).query).isEqualTo("USE \"ks\"");
    writeInboundFrame(requestFrame, new SetKeyspace("ks"));

    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_initialize_with_events() {
    List<String> eventTypes = ImmutableList.of("foo", "bar");
    EventCallback eventCallback = mock(EventCallback.class);
    DriverChannelOptions driverChannelOptions =
        DriverChannelOptions.builder().withEvents(eventTypes, eventCallback).build();
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
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("someClusterName"));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Register.class);
    assertThat(((Register) requestFrame.message).eventTypes).containsExactly("foo", "bar");
    writeInboundFrame(requestFrame, new Ready());

    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_initialize_with_keyspace_and_events() {
    List<String> eventTypes = ImmutableList.of("foo", "bar");
    EventCallback eventCallback = mock(EventCallback.class);
    DriverChannelOptions driverChannelOptions =
        DriverChannelOptions.builder()
            .withKeyspace(CqlIdentifier.fromCql("ks"))
            .withEvents(eventTypes, eventCallback)
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
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("someClusterName"));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    assertThat(((Query) requestFrame.message).query).isEqualTo("USE \"ks\"");
    writeInboundFrame(requestFrame, new SetKeyspace("ks"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Register.class);
    assertThat(((Register) requestFrame.message).eventTypes).containsExactly("foo", "bar");
    writeInboundFrame(requestFrame, new Ready());

    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_fail_to_initialize_if_keyspace_is_invalid() {
    DriverChannelOptions driverChannelOptions =
        DriverChannelOptions.builder().withKeyspace(CqlIdentifier.fromCql("ks")).build();
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
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("someClusterName"));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    assertThat(((Query) requestFrame.message).query).isEqualTo("USE \"ks\"");
    writeInboundFrame(
        requestFrame, new Error(ProtocolConstants.ErrorCode.INVALID, "invalid keyspace"));

    assertThat(connectFuture)
        .isFailed(
            error ->
                assertThat(error)
                    .isInstanceOf(InvalidKeyspaceException.class)
                    .hasMessage("invalid keyspace"));
  }

  /**
   * This covers a corner case where {@code abortAllInFlight} was recursing into itself, causing a
   * {@link ConcurrentModificationException}. This was recoverable but caused Netty to generate a
   * warning log.
   *
   * @see <a href="https://datastax-oss.atlassian.net/browse/JAVA-2838">JAVA-2838</a>
   */
  @Test
  public void should_fail_pending_requests_only_once_if_init_fails() {
    Logger logger =
        (Logger) LoggerFactory.getLogger("io.netty.channel.AbstractChannelHandlerContext");
    Level levelBefore = logger.getLevel();
    logger.setLevel(Level.WARN);
    logger.addAppender(appender);

    channel
        .pipeline()
        .addLast(
            "init",
            new ProtocolInitHandler(
                internalDriverContext,
                DefaultProtocolVersion.V4,
                null,
                END_POINT,
                DriverChannelOptions.DEFAULT,
                heartbeatHandler,
                false));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));
    channel.pipeline().fireExceptionCaught(new IOException("Mock I/O exception"));
    assertThat(connectFuture).isFailed();

    verify(appender, never()).doAppend(any(ILoggingEvent.class));

    logger.detachAppender(appender);
    logger.setLevel(levelBefore);
  }
}
