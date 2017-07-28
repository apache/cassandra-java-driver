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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameEncoder;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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

/** Builds {@link DriverChannel} objects for an instance of the driver. */
public class ChannelFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelFactory.class);

  protected final InternalDriverContext context;

  /** either set from the configuration, or null and will be negotiated */
  @VisibleForTesting ProtocolVersion protocolVersion;

  @VisibleForTesting volatile String clusterName;

  public ChannelFactory(InternalDriverContext context) {
    this.context = context;

    DriverConfigProfile defaultConfig = context.config().getDefaultProfile();
    if (defaultConfig.isDefined(CoreDriverOption.PROTOCOL_VERSION)) {
      String versionName = defaultConfig.getString(CoreDriverOption.PROTOCOL_VERSION);
      this.protocolVersion = context.protocolVersionRegistry().fromName(versionName);
    } // else it will be negotiated with the first opened connection
  }

  public ProtocolVersion protocolVersion() {
    ProtocolVersion result = this.protocolVersion;
    Preconditions.checkState(
        result != null, "Protocol version not known yet, this should only be called after init");
    return result;
  }

  public CompletionStage<DriverChannel> connect(
      final SocketAddress address, DriverChannelOptions options) {
    CompletableFuture<DriverChannel> resultFuture = new CompletableFuture<>();

    AvailableIdsHolder availableIdsHolder =
        options.reportAvailableIds ? new AvailableIdsHolder() : null;

    ProtocolVersion currentVersion;
    boolean isNegotiating;
    List<ProtocolVersion> attemptedVersions = new CopyOnWriteArrayList<>();
    if (this.protocolVersion != null) {
      currentVersion = protocolVersion;
      isNegotiating = false;
    } else {
      currentVersion = context.protocolVersionRegistry().highestNonBeta();
      isNegotiating = true;
    }

    connect(
        address,
        options,
        availableIdsHolder,
        currentVersion,
        isNegotiating,
        attemptedVersions,
        resultFuture);
    return resultFuture;
  }

  private void connect(
      SocketAddress address,
      DriverChannelOptions options,
      AvailableIdsHolder availableIdsHolder,
      final ProtocolVersion currentVersion,
      boolean isNegotiating,
      List<ProtocolVersion> attemptedVersions,
      CompletableFuture<DriverChannel> resultFuture) {

    NettyOptions nettyOptions = context.nettyOptions();

    Bootstrap bootstrap =
        new Bootstrap()
            .group(nettyOptions.ioEventLoopGroup())
            .channel(nettyOptions.channelClass())
            .option(ChannelOption.ALLOCATOR, nettyOptions.allocator())
            .handler(initializer(address, currentVersion, options, availableIdsHolder));

    nettyOptions.afterBootstrapInitialized(bootstrap);

    ChannelFuture connectFuture = bootstrap.connect(address);

    connectFuture.addListener(
        cf -> {
          if (connectFuture.isSuccess()) {
            Channel channel = connectFuture.channel();
            DriverChannel driverChannel =
                new DriverChannel(
                    channel, context.writeCoalescer(), availableIdsHolder, currentVersion);
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
                  context.protocolVersionRegistry().downgrade(currentVersion);
              if (downgraded.isPresent()) {
                LOG.info(
                    "Failed to connect with protocol {}, retrying with {}",
                    currentVersion,
                    downgraded.get());
                connect(
                    address,
                    options,
                    availableIdsHolder,
                    downgraded.get(),
                    true,
                    attemptedVersions,
                    resultFuture);
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
      SocketAddress address,
      final ProtocolVersion protocolVersion,
      final DriverChannelOptions options,
      AvailableIdsHolder availableIdsHolder) {
    return new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel channel) throws Exception {
        DriverConfigProfile defaultConfigProfile = context.config().getDefaultProfile();

        long setKeyspaceTimeoutMillis =
            defaultConfigProfile
                .getDuration(CoreDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT)
                .toMillis();
        int maxFrameLength =
            (int) defaultConfigProfile.getBytes(CoreDriverOption.PROTOCOL_MAX_FRAME_LENGTH);
        int maxRequestsPerConnection =
            defaultConfigProfile.getInt(CoreDriverOption.CONNECTION_MAX_REQUESTS);
        int maxOrphanRequests =
            defaultConfigProfile.getInt(CoreDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS);

        InFlightHandler inFlightHandler =
            new InFlightHandler(
                protocolVersion,
                new StreamIdGenerator(maxRequestsPerConnection),
                maxOrphanRequests,
                setKeyspaceTimeoutMillis,
                availableIdsHolder,
                channel.newPromise(),
                options.eventCallback,
                options.ownerLogPrefix);
        HeartbeatHandler heartbeatHandler = new HeartbeatHandler(defaultConfigProfile);
        ProtocolInitHandler initHandler =
            new ProtocolInitHandler(
                context, protocolVersion, clusterName, options, heartbeatHandler);

        ChannelPipeline pipeline = channel.pipeline();
        context
            .sslHandlerFactory()
            .map(f -> f.newSslHandler(channel, address))
            .map(h -> pipeline.addLast("ssl", h));
        pipeline
            .addLast("encoder", new FrameEncoder(context.frameCodec(), maxFrameLength))
            .addLast("decoder", new FrameDecoder(context.frameCodec(), maxFrameLength))
            // Note: HeartbeatHandler is inserted here once init completes
            .addLast("inflight", inFlightHandler)
            .addLast("init", initHandler);

        context.nettyOptions().afterChannelInitialized(channel);
      }
    };
  }
}
