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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.TestResponses;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

/**
 * Sets up the infrastructure for channel factory tests.
 *
 * <p>Because the factory manages channel creation itself, {@link
 * io.netty.channel.embedded.EmbeddedChannel} is not suitable. Instead, we launch an embedded server
 * and connect to it with the local transport.
 *
 * <p>The current implementation assumes that only one connection will be tested at a time, but
 * support for multiple simultaneous connections could easily be added: store multiple instances of
 * requestFrameExchanger and serverResponseChannel, and add a parameter to readOutboundFrame and
 * writeInboundFrame (for instance the position of the connection in creation order) to specify
 * which instance to use.
 */
@RunWith(DataProviderRunner.class)
public abstract class ChannelFactoryTestBase {
  static final EndPoint SERVER_ADDRESS =
      new LocalEndPoint(ChannelFactoryTestBase.class.getSimpleName() + "-server");

  private static final int TIMEOUT_MILLIS = 500;

  DefaultEventLoopGroup serverGroup;
  DefaultEventLoopGroup clientGroup;

  @Mock InternalDriverContext context;
  @Mock DriverConfig driverConfig;
  @Mock DriverExecutionProfile defaultProfile;
  @Mock NettyOptions nettyOptions;
  @Mock ProtocolVersionRegistry protocolVersionRegistry;
  @Mock EventBus eventBus;
  @Mock Compressor<ByteBuf> compressor;

  // The server's I/O thread will store the last received request here, and block until the test
  // thread retrieves it. This assumes readOutboundFrame() is called for each actual request, else
  // the test will hang forever.
  private final Exchanger<Frame> requestFrameExchanger = new Exchanger<>();

  // The channel that accepts incoming connections on the server
  private LocalServerChannel serverAcceptChannel;
  // The channel to send responses to the last open connection
  private volatile LocalChannel serverResponseChannel;

  @Before
  public void setup() throws InterruptedException {
    MockitoAnnotations.initMocks(this);

    serverGroup = new DefaultEventLoopGroup(1);
    clientGroup = new DefaultEventLoopGroup(1);

    when(context.getConfig()).thenReturn(driverConfig);
    when(driverConfig.getDefaultProfile()).thenReturn(defaultProfile);
    when(defaultProfile.isDefined(DefaultDriverOption.AUTH_PROVIDER_CLASS)).thenReturn(false);
    when(defaultProfile.getDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT))
        .thenReturn(Duration.ofMillis(TIMEOUT_MILLIS));
    when(defaultProfile.getDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT))
        .thenReturn(Duration.ofMillis(TIMEOUT_MILLIS));
    when(defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS)).thenReturn(1);
    when(defaultProfile.getDuration(DefaultDriverOption.HEARTBEAT_INTERVAL))
        .thenReturn(Duration.ofSeconds(30));
    when(defaultProfile.getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT))
        .thenReturn(Duration.ofSeconds(5));

    when(context.getProtocolVersionRegistry()).thenReturn(protocolVersionRegistry);
    when(context.getNettyOptions()).thenReturn(nettyOptions);
    when(nettyOptions.ioEventLoopGroup()).thenReturn(clientGroup);
    when(nettyOptions.channelClass()).thenAnswer((Answer<Object>) i -> LocalChannel.class);
    when(nettyOptions.allocator()).thenReturn(ByteBufAllocator.DEFAULT);
    when(context.getFrameCodec())
        .thenReturn(
            FrameCodec.defaultClient(
                new ByteBufPrimitiveCodec(ByteBufAllocator.DEFAULT), Compressor.none()));
    when(context.getSslHandlerFactory()).thenReturn(Optional.empty());
    when(context.getEventBus()).thenReturn(eventBus);
    when(context.getWriteCoalescer()).thenReturn(new PassThroughWriteCoalescer(null));
    when(context.getCompressor()).thenReturn(compressor);

    // Start local server
    ServerBootstrap serverBootstrap =
        new ServerBootstrap()
            .group(serverGroup)
            .channel(LocalServerChannel.class)
            .localAddress(SERVER_ADDRESS.resolve())
            .childHandler(new ServerInitializer());
    ChannelFuture channelFuture = serverBootstrap.bind().sync();
    serverAcceptChannel = (LocalServerChannel) channelFuture.sync().channel();
  }

  // Sets up the pipeline for our local server
  private class ServerInitializer extends ChannelInitializer<LocalChannel> {
    @Override
    protected void initChannel(LocalChannel ch) throws Exception {
      // Install a single handler that stores received requests, so that the test can check what
      // the client sent
      ch.pipeline()
          .addLast(
              new ChannelInboundHandlerAdapter() {
                @Override
                @SuppressWarnings("unchecked")
                public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
                  super.channelRead(ctx, msg);
                  requestFrameExchanger.exchange((Frame) msg);
                }
              });

      // Store the channel so that the test can send responses back to the client
      serverResponseChannel = ch;
    }
  }

  protected Frame readOutboundFrame() {
    try {
      return requestFrameExchanger.exchange(null, TIMEOUT_MILLIS, MILLISECONDS);
    } catch (InterruptedException e) {
      fail("unexpected interruption while waiting for outbound frame", e);
    } catch (TimeoutException e) {
      fail("Timed out reading outbound frame");
    }
    return null; // never reached
  }

  protected void writeInboundFrame(Frame requestFrame, Message response) {
    writeInboundFrame(requestFrame, response, requestFrame.protocolVersion);
  }

  private void writeInboundFrame(Frame requestFrame, Message response, int protocolVersion) {
    serverResponseChannel.writeAndFlush(
        Frame.forResponse(
            protocolVersion,
            requestFrame.streamId,
            null,
            Frame.NO_PAYLOAD,
            Collections.emptyList(),
            response));
  }

  /**
   * Simulate the sequence of roundtrips to initialize a simple channel without authentication or
   * keyspace (avoids repeating it in subclasses).
   */
  protected void completeSimpleChannelInit() {
    Frame requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Options.class);
    writeInboundFrame(requestFrame, TestResponses.supportedResponse("mock_key", "mock_value"));

    requestFrame = readOutboundFrame();
    assertThat(requestFrame.message).isInstanceOf(Startup.class);
    writeInboundFrame(requestFrame, new Ready());

    requestFrame = readOutboundFrame();
    writeInboundFrame(requestFrame, TestResponses.clusterNameResponse("mockClusterName"));
  }

  ChannelFactory newChannelFactory() {
    return new TestChannelFactory(context);
  }

  // A simplified channel factory to use in the tests.
  // It only installs high-level handlers on the pipeline, not the frame codecs. So we'll receive
  // Frame objects on the server side, which is simpler to test.
  private static class TestChannelFactory extends ChannelFactory {

    private TestChannelFactory(InternalDriverContext internalDriverContext) {
      super(internalDriverContext);
    }

    @Override
    ChannelInitializer<Channel> initializer(
        EndPoint endPoint,
        ProtocolVersion protocolVersion,
        DriverChannelOptions options,
        NodeMetricUpdater nodeMetricUpdater,
        CompletableFuture<DriverChannel> resultFuture) {
      return new ChannelInitializer<Channel>() {
        @Override
        protected void initChannel(Channel channel) throws Exception {
          try {
            DriverExecutionProfile defaultProfile = context.getConfig().getDefaultProfile();

            long setKeyspaceTimeoutMillis =
                defaultProfile
                    .getDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT)
                    .toMillis();
            int maxRequestsPerConnection =
                defaultProfile.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS);

            InFlightHandler inFlightHandler =
                new InFlightHandler(
                    protocolVersion,
                    new StreamIdGenerator(maxRequestsPerConnection),
                    Integer.MAX_VALUE,
                    setKeyspaceTimeoutMillis,
                    channel.newPromise(),
                    null,
                    "test");

            HeartbeatHandler heartbeatHandler = new HeartbeatHandler(defaultProfile);
            ProtocolInitHandler initHandler =
                new ProtocolInitHandler(
                    context,
                    protocolVersion,
                    getClusterName(),
                    endPoint,
                    options,
                    heartbeatHandler,
                    productType == null);
            channel
                .pipeline()
                .addLast(ChannelFactory.INFLIGHT_HANDLER_NAME, inFlightHandler)
                .addLast(ChannelFactory.INIT_HANDLER_NAME, initHandler);
          } catch (Throwable t) {
            resultFuture.completeExceptionally(t);
          }
        }
      };
    }
  }

  @After
  public void tearDown() throws InterruptedException {
    serverAcceptChannel.close();

    serverGroup
        .shutdownGracefully(TIMEOUT_MILLIS, TIMEOUT_MILLIS * 2, TimeUnit.MILLISECONDS)
        .sync();
    clientGroup
        .shutdownGracefully(TIMEOUT_MILLIS, TIMEOUT_MILLIS * 2, TimeUnit.MILLISECONDS)
        .sync();
  }
}
