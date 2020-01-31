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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metrics.DefaultNodeMetric;
import com.datastax.oss.driver.api.core.metrics.DefaultSessionMetric;
import com.datastax.oss.driver.internal.core.config.typesafe.TypesafeDriverConfig;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metrics.NodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.NoopNodeMetricUpdater;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.protocol.FrameDecoder;
import com.datastax.oss.driver.internal.core.protocol.FrameEncoder;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builds {@link DriverChannel} objects for an instance of the driver. */
@ThreadSafe
public class ChannelFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelFactory.class);

  /** A value for {@link #productType} that indicates that we are connected to Datastax Cloud. */
  private static final String DATASTAX_CLOUD_PRODUCT_TYPE = "DATASTAX_APOLLO";

  /**
   * A value for {@link #productType} that indicates that the server does not report any product
   * type.
   */
  private static final String UNKNOWN_PRODUCT_TYPE = "UNKNOWN";

  private final String logPrefix;
  protected final InternalDriverContext context;

  /** either set from the configuration, or null and will be negotiated */
  @VisibleForTesting volatile ProtocolVersion protocolVersion;

  @VisibleForTesting volatile String clusterName;

  /**
   * The value of the {@code PRODUCT_TYPE} option reported by the first channel we opened, in
   * response to a {@code SUPPORTED} request.
   *
   * <p>If the server does not return that option, the value will be {@link #UNKNOWN_PRODUCT_TYPE}.
   */
  @VisibleForTesting volatile String productType;

  public ChannelFactory(InternalDriverContext context) {
    this.logPrefix = context.getSessionName();
    this.context = context;

    DriverExecutionProfile defaultConfig = context.getConfig().getDefaultProfile();
    if (defaultConfig.isDefined(DefaultDriverOption.PROTOCOL_VERSION)) {
      String versionName = defaultConfig.getString(DefaultDriverOption.PROTOCOL_VERSION);
      this.protocolVersion = context.getProtocolVersionRegistry().fromName(versionName);
    } // else it will be negotiated with the first opened connection
  }

  public ProtocolVersion getProtocolVersion() {
    ProtocolVersion result = this.protocolVersion;
    Preconditions.checkState(
        result != null, "Protocol version not known yet, this should only be called after init");
    return result;
  }

  /**
   * WARNING: this is only used at the very beginning of the init process (when we just refreshed
   * the list of nodes for the first time, and found out that one of them requires a lower version
   * than was negotiated with the first contact point); it's safe at this time because we are in a
   * controlled state (only the control connection is open, it's not executing queries and we're
   * going to reconnect immediately after). Calling this method at any other time will likely wreak
   * havoc.
   */
  public void setProtocolVersion(ProtocolVersion newVersion) {
    this.protocolVersion = newVersion;
  }

  public CompletionStage<DriverChannel> connect(Node node, DriverChannelOptions options) {
    NodeMetricUpdater nodeMetricUpdater;
    if (node instanceof DefaultNode) {
      nodeMetricUpdater = ((DefaultNode) node).getMetricUpdater();
    } else {
      nodeMetricUpdater = NoopNodeMetricUpdater.INSTANCE;
    }
    return connect(node.getEndPoint(), options, nodeMetricUpdater);
  }

  @VisibleForTesting
  CompletionStage<DriverChannel> connect(
      EndPoint endPoint, DriverChannelOptions options, NodeMetricUpdater nodeMetricUpdater) {
    CompletableFuture<DriverChannel> resultFuture = new CompletableFuture<>();

    ProtocolVersion currentVersion;
    boolean isNegotiating;
    List<ProtocolVersion> attemptedVersions = new CopyOnWriteArrayList<>();
    if (this.protocolVersion != null) {
      currentVersion = protocolVersion;
      isNegotiating = false;
    } else {
      currentVersion = context.getProtocolVersionRegistry().highestNonBeta();
      isNegotiating = true;
    }

    connect(
        endPoint,
        options,
        nodeMetricUpdater,
        currentVersion,
        isNegotiating,
        attemptedVersions,
        resultFuture);
    return resultFuture;
  }

  private void connect(
      EndPoint endPoint,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      ProtocolVersion currentVersion,
      boolean isNegotiating,
      List<ProtocolVersion> attemptedVersions,
      CompletableFuture<DriverChannel> resultFuture) {

    NettyOptions nettyOptions = context.getNettyOptions();
    Duration connectTimeout =
        context
            .getConfig()
            .getDefaultProfile()
            .getDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT);

    Bootstrap bootstrap =
        new Bootstrap()
            .group(nettyOptions.ioEventLoopGroup())
            .channel(nettyOptions.channelClass())
            .option(ChannelOption.ALLOCATOR, nettyOptions.allocator())
            .option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                Long.valueOf(connectTimeout.toMillis()).intValue())
            .handler(
                initializer(endPoint, currentVersion, options, nodeMetricUpdater, resultFuture));

    nettyOptions.afterBootstrapInitialized(bootstrap);

    ChannelFuture connectFuture = bootstrap.connect(endPoint.resolve());

    connectFuture.addListener(
        cf -> {
          if (connectFuture.isSuccess()) {
            Channel channel = connectFuture.channel();
            DriverChannel driverChannel =
                new DriverChannel(endPoint, channel, context.getWriteCoalescer(), currentVersion);
            // If this is the first successful connection, remember the protocol version and
            // cluster name for future connections.
            if (isNegotiating) {
              ChannelFactory.this.protocolVersion = currentVersion;
            }
            if (ChannelFactory.this.clusterName == null) {
              ChannelFactory.this.clusterName = driverChannel.getClusterName();
            }
            Map<String, List<String>> supportedOptions = driverChannel.getOptions();
            if (ChannelFactory.this.productType == null && supportedOptions != null) {
              List<String> productTypes = supportedOptions.get("PRODUCT_TYPE");
              String productType =
                  productTypes != null && !productTypes.isEmpty()
                      ? productTypes.get(0)
                      : UNKNOWN_PRODUCT_TYPE;
              ChannelFactory.this.productType = productType;
              DriverConfig driverConfig = context.getConfig();
              if (driverConfig instanceof TypesafeDriverConfig
                  && productType.equals(DATASTAX_CLOUD_PRODUCT_TYPE)) {
                ((TypesafeDriverConfig) driverConfig)
                    .overrideDefaults(
                        ImmutableMap.of(
                            DefaultDriverOption.REQUEST_CONSISTENCY,
                            ConsistencyLevel.LOCAL_QUORUM.name()));
              }
            }
            resultFuture.complete(driverChannel);
          } else {
            Throwable error = connectFuture.cause();
            if (error instanceof UnsupportedProtocolVersionException && isNegotiating) {
              attemptedVersions.add(currentVersion);
              Optional<ProtocolVersion> downgraded =
                  context.getProtocolVersionRegistry().downgrade(currentVersion);
              if (downgraded.isPresent()) {
                LOG.info(
                    "[{}] Failed to connect with protocol {}, retrying with {}",
                    logPrefix,
                    currentVersion,
                    downgraded.get());
                connect(
                    endPoint,
                    options,
                    nodeMetricUpdater,
                    downgraded.get(),
                    true,
                    attemptedVersions,
                    resultFuture);
              } else {
                resultFuture.completeExceptionally(
                    UnsupportedProtocolVersionException.forNegotiation(
                        endPoint, attemptedVersions));
              }
            } else {
              // Note: might be completed already if the failure happened in initializer(), this is
              // fine
              resultFuture.completeExceptionally(error);
            }
          }
        });
  }

  @VisibleForTesting
  ChannelInitializer<Channel> initializer(
      EndPoint endPoint,
      ProtocolVersion protocolVersion,
      DriverChannelOptions options,
      NodeMetricUpdater nodeMetricUpdater,
      CompletableFuture<DriverChannel> resultFuture) {
    return new ChannelInitializer<Channel>() {
      @Override
      protected void initChannel(Channel channel) {
        try {
          DriverExecutionProfile defaultConfig = context.getConfig().getDefaultProfile();

          long setKeyspaceTimeoutMillis =
              defaultConfig
                  .getDuration(DefaultDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT)
                  .toMillis();
          int maxFrameLength =
              (int) defaultConfig.getBytes(DefaultDriverOption.PROTOCOL_MAX_FRAME_LENGTH);
          int maxRequestsPerConnection =
              defaultConfig.getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS);
          int maxOrphanRequests =
              defaultConfig.getInt(DefaultDriverOption.CONNECTION_MAX_ORPHAN_REQUESTS);

          InFlightHandler inFlightHandler =
              new InFlightHandler(
                  protocolVersion,
                  new StreamIdGenerator(maxRequestsPerConnection),
                  maxOrphanRequests,
                  setKeyspaceTimeoutMillis,
                  channel.newPromise(),
                  options.eventCallback,
                  options.ownerLogPrefix);
          HeartbeatHandler heartbeatHandler = new HeartbeatHandler(defaultConfig);
          ProtocolInitHandler initHandler =
              new ProtocolInitHandler(
                  context,
                  protocolVersion,
                  clusterName,
                  endPoint,
                  options,
                  heartbeatHandler,
                  productType == null);

          ChannelPipeline pipeline = channel.pipeline();
          context
              .getSslHandlerFactory()
              .map(f -> f.newSslHandler(channel, endPoint))
              .map(h -> pipeline.addLast("ssl", h));

          // Only add meter handlers on the pipeline if metrics are enabled.
          SessionMetricUpdater sessionMetricUpdater =
              context.getMetricsFactory().getSessionUpdater();
          if (nodeMetricUpdater.isEnabled(DefaultNodeMetric.BYTES_RECEIVED, null)
              || sessionMetricUpdater.isEnabled(DefaultSessionMetric.BYTES_RECEIVED, null)) {
            pipeline.addLast(
                "inboundTrafficMeter",
                new InboundTrafficMeter(nodeMetricUpdater, sessionMetricUpdater));
          }

          if (nodeMetricUpdater.isEnabled(DefaultNodeMetric.BYTES_SENT, null)
              || sessionMetricUpdater.isEnabled(DefaultSessionMetric.BYTES_SENT, null)) {
            pipeline.addLast(
                "outboundTrafficMeter",
                new OutboundTrafficMeter(nodeMetricUpdater, sessionMetricUpdater));
          }

          pipeline
              .addLast("encoder", new FrameEncoder(context.getFrameCodec(), maxFrameLength))
              .addLast("decoder", new FrameDecoder(context.getFrameCodec(), maxFrameLength))
              // Note: HeartbeatHandler is inserted here once init completes
              .addLast("inflight", inFlightHandler)
              .addLast("init", initHandler);

          context.getNettyOptions().afterChannelInitialized(channel);
        } catch (Throwable t) {
          // If the init handler throws an exception, Netty swallows it and closes the channel. We
          // want to propagate it instead, so fail the outer future (the result of connect()).
          resultFuture.completeExceptionally(t);
          throw t;
        }
      }
    };
  }
}
