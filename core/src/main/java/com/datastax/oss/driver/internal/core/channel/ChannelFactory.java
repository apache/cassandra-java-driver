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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.DriverContext;
import com.datastax.oss.driver.internal.core.NettyOptions;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameEncoder;
import com.google.common.annotations.VisibleForTesting;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/** Builds {@link DriverChannel} objects for an instance of the driver. */
public class ChannelFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelFactory.class);

  protected final DriverContext driverContext;

  /** either set from the configuration, or null and will be negotiated */
  @VisibleForTesting ProtocolVersion protocolVersion;

  @VisibleForTesting volatile String clusterName;

  public ChannelFactory(DriverContext driverContext) {
    this.driverContext = driverContext;

    DriverConfigProfile defaultConfig = driverContext.config().defaultProfile();
    if (defaultConfig.isDefined(CoreDriverOption.PROTOCOL_VERSION)) {
      String versionName = defaultConfig.getString(CoreDriverOption.PROTOCOL_VERSION);
      this.protocolVersion = driverContext.protocolVersionRegistry().fromName(versionName);
    } // else it will be negotiated with the first opened connection
  }

  public CompletionStage<DriverChannel> connect(
      final SocketAddress address, CqlIdentifier keyspace) {
    CompletableFuture<DriverChannel> resultFuture = new CompletableFuture<>();

    ProtocolVersion currentVersion;
    boolean isNegotiating;
    List<ProtocolVersion> attemptedVersions = new CopyOnWriteArrayList<>();
    if (this.protocolVersion != null) {
      currentVersion = protocolVersion;
      isNegotiating = false;
    } else {
      currentVersion = driverContext.protocolVersionRegistry().highestNonBeta();
      isNegotiating = true;
    }

    connect(address, keyspace, currentVersion, isNegotiating, attemptedVersions, resultFuture);
    return resultFuture;
  }

  private void connect(
      SocketAddress address,
      CqlIdentifier keyspace,
      final ProtocolVersion currentVersion,
      boolean isNegotiating,
      List<ProtocolVersion> attemptedVersions,
      CompletableFuture<DriverChannel> resultFuture) {

    NettyOptions nettyOptions = driverContext.nettyOptions();

    Bootstrap bootstrap =
        new Bootstrap()
            .group(nettyOptions.ioEventLoopGroup())
            .channel(nettyOptions.channelClass())
            .option(ChannelOption.ALLOCATOR, nettyOptions.allocator())
            .handler(initializer(address, currentVersion, keyspace));

    nettyOptions.afterBootstrapInitialized(bootstrap);

    ChannelFuture connectFuture = bootstrap.connect(address);

    connectFuture.addListener(
        cf -> {
          if (connectFuture.isSuccess()) {
            DriverChannel driverChannel =
                new DriverChannel(connectFuture.channel(), driverContext.writeCoalescer());
            // If this is the first successful connection, remember the protocol version and
            // cluster name for future connections.
            if (isNegotiating) {
              ChannelFactory.this.protocolVersion = currentVersion;
            }
            if (ChannelFactory.this.clusterName == null) {
              ChannelFactory.this.clusterName = driverChannel.getClusterName();
            }
            resultFuture.complete(driverChannel);
          } else {
            Throwable error = connectFuture.cause();
            if (error instanceof UnsupportedProtocolVersionException && isNegotiating) {
              attemptedVersions.add(currentVersion);
              Optional<ProtocolVersion> downgraded =
                  driverContext.protocolVersionRegistry().downgrade(currentVersion);
              if (downgraded.isPresent()) {
                LOG.info(
                    "Failed to connect with protocol {}, retrying with {}",
                    currentVersion,
                    downgraded.get());
                connect(address, keyspace, downgraded.get(), true, attemptedVersions, resultFuture);
              } else {
                resultFuture.completeExceptionally(
                    UnsupportedProtocolVersionException.forNegotiation(address, attemptedVersions));
              }
            } else {
              resultFuture.completeExceptionally(error);
            }
          }
        });
  }

  @VisibleForTesting
  ChannelInitializer<Channel> initializer(
      SocketAddress address, final ProtocolVersion protocolVersion, final CqlIdentifier keyspace) {
    return new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel channel) throws Exception {
        DriverConfigProfile defaultConfigProfile = driverContext.config().defaultProfile();

        long setKeyspaceTimeoutMillis =
            defaultConfigProfile.getDuration(
                CoreDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT, MILLISECONDS);
        int maxFrameLength =
            (int) defaultConfigProfile.getBytes(CoreDriverOption.CONNECTION_MAX_FRAME_LENGTH);
        int maxRequestsPerConnection =
            defaultConfigProfile.getInt(CoreDriverOption.CONNECTION_MAX_REQUESTS);

        // TODO SSL
        InFlightHandler inFlightHandler =
            new InFlightHandler(
                protocolVersion,
                new StreamIdGenerator(maxRequestsPerConnection),
                setKeyspaceTimeoutMillis);
        ProtocolInitHandler initHandler =
            new ProtocolInitHandler(driverContext, protocolVersion, clusterName, keyspace);

        ChannelPipeline pipeline = channel.pipeline();
        driverContext
            .sslHandlerFactory()
            .newSslHandler(channel, address)
            .map(h -> pipeline.addLast("ssl", h));
        pipeline
            .addLast("encoder", new FrameEncoder(driverContext.frameCodec()))
            .addLast("decoder", new FrameDecoder(driverContext.frameCodec(), maxFrameLength))
            .addLast("inflight", inFlightHandler)
            .addLast("heartbeat", new HeartbeatHandler(defaultConfigProfile))
            .addLast("init", initHandler);

        driverContext.nettyOptions().afterChannelInitialized(channel);
      }
    };
  }
}
