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
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.auth.AuthenticationException;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.channel.ChannelFuture;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;

public class ProtocolInitHandlerTest extends ChannelHandlerTestBase {

  private static final long QUERY_TIMEOUT_MILLIS = 100L;

  @Mock private InternalDriverContext internalDriverContext;
  @Mock private DriverConfig driverConfig;
  @Mock private DriverConfigProfile defaultConfigProfile;
  private ProtocolVersionRegistry protocolVersionRegistry = new ProtocolVersionRegistry();

  @BeforeMethod
  @Override
  public void setup() {
    super.setup();
    MockitoAnnotations.initMocks(this);
    Mockito.when(internalDriverContext.config()).thenReturn(driverConfig);
    Mockito.when(driverConfig.defaultProfile()).thenReturn(defaultConfigProfile);
    Mockito.when(
            defaultConfigProfile.getDuration(
                CoreDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, TimeUnit.MILLISECONDS))
        .thenReturn(QUERY_TIMEOUT_MILLIS);
    Mockito.when(internalDriverContext.protocolVersionRegistry())
        .thenReturn(protocolVersionRegistry);

    channel
        .pipeline()
        .addLast(
            "inflight",
            new InFlightHandler(CoreProtocolVersion.V4, new StreamIdGenerator(100), 100, null));
  }

  @Test
  public void should_initialize_without_authentication() {
    channel
        .pipeline()
        .addLast(
            "init",
            new ProtocolInitHandler(internalDriverContext, CoreProtocolVersion.V4, null, null));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    // It should send a STARTUP message
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    Startup startup = (Startup) requestFrame.message;
    assertThat(startup.options).doesNotContainKey("COMPRESSION");
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
  public void should_fail_to_initialize_if_init_query_times_out() throws InterruptedException {
    channel
        .pipeline()
        .addLast(
            "init",
            new ProtocolInitHandler(internalDriverContext, CoreProtocolVersion.V4, null, null));

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
            "init",
            new ProtocolInitHandler(internalDriverContext, CoreProtocolVersion.V4, null, null));

    String serverAuthenticator = "mockServerAuthenticator";
    AuthProvider authProvider = Mockito.mock(AuthProvider.class);
    MockAuthenticator authenticator = new MockAuthenticator();
    Mockito.when(authProvider.newAuthenticator(channel.remoteAddress(), serverAuthenticator))
        .thenReturn(authenticator);
    Mockito.when(internalDriverContext.authProvider()).thenReturn(Optional.of(authProvider));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    assertThat(connectFuture).isNotDone();

    // Simulate a response that says that the server requires authentication
    writeInboundFrame(requestFrame, new Authenticate(serverAuthenticator));

    // The connection should have created an authenticator from the auth provider
    Mockito.verify(authProvider).newAuthenticator(channel.remoteAddress(), serverAuthenticator);

    // And sent an auth response
    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(AuthResponse.class);
    AuthResponse authResponse = (AuthResponse) requestFrame.message;
    assertThat(Bytes.toHexString(authResponse.token)).isEqualTo(MockAuthenticator.INITIAL_RESPONSE);
    assertThat(connectFuture).isNotDone();

    // As long as the server sends an auth challenge, the client should reply with another auth_response
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

    // When the server finally sends back a success message, should proceed to the cluster name check and succeed
    writeInboundFrame(requestFrame, new AuthSuccess(Bytes.fromHexString(mockToken)));
    assertThat(authenticator.successToken).isEqualTo(mockToken);

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("someClusterName"));

    assertThat(connectFuture).isSuccess();
  }

  @Test
  public void should_fail_to_initialize_if_server_sends_auth_error() throws Throwable {
    channel
        .pipeline()
        .addLast(
            "init",
            new ProtocolInitHandler(internalDriverContext, CoreProtocolVersion.V4, null, null));

    String serverAuthenticator = "mockServerAuthenticator";
    AuthProvider authProvider = Mockito.mock(AuthProvider.class);
    MockAuthenticator authenticator = new MockAuthenticator();
    Mockito.when(authProvider.newAuthenticator(channel.remoteAddress(), serverAuthenticator))
        .thenReturn(authenticator);
    Mockito.when(internalDriverContext.authProvider()).thenReturn(Optional.of(authProvider));

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
                        "Authentication error on host embedded: server replied 'mock error'"));
  }

  @Test
  public void should_check_cluster_name_if_provided() {
    channel
        .pipeline()
        .addLast(
            "init",
            new ProtocolInitHandler(
                internalDriverContext, CoreProtocolVersion.V4, "expectedClusterName", null));

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
            "init",
            new ProtocolInitHandler(
                internalDriverContext, CoreProtocolVersion.V4, "expectedClusterName", null));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(
        readOutboundFrame(), TestResponses.clusterNameResponse("differentClusterName"));

    assertThat(connectFuture)
        .isFailed(
            e ->
                assertThat(e)
                    .isInstanceOf(ClusterNameMismatchException.class)
                    .hasMessage(
                        "Host embedded reports cluster name 'differentClusterName' that doesn't match our cluster name 'expectedClusterName'."));
  }

  @Test
  public void should_initialize_with_keyspace() {
    channel
        .pipeline()
        .addLast(
            "init",
            new ProtocolInitHandler(
                internalDriverContext, CoreProtocolVersion.V4, null, CqlIdentifier.fromCql("ks")));

    ChannelFuture connectFuture = channel.connect(new InetSocketAddress("localhost", 9042));

    writeInboundFrame(readOutboundFrame(), new Ready());
    writeInboundFrame(readOutboundFrame(), TestResponses.clusterNameResponse("someClusterName"));

    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Query.class);
    assertThat(((Query) requestFrame.message).query).isEqualTo("USE \"ks\"");
    writeInboundFrame(requestFrame, new SetKeyspace("ks"));

    assertThat(connectFuture).isSuccess();
  }
}
