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
package com.datastax.oss.driver.internal.core.context;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.WriteCoalescer;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.servererrors.WriteTypeRegistry;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import java.util.Optional;
import java.util.function.Predicate;

/** Extends the driver context with additional components that are not exposed by our public API. */
public interface InternalDriverContext extends DriverContext {

  EventBus eventBus();

  Compressor<ByteBuf> compressor();

  FrameCodec<ByteBuf> frameCodec();

  ProtocolVersionRegistry protocolVersionRegistry();

  ConsistencyLevelRegistry consistencyLevelRegistry();

  WriteTypeRegistry writeTypeRegistry();

  NettyOptions nettyOptions();

  WriteCoalescer writeCoalescer();

  Optional<SslHandlerFactory> sslHandlerFactory();

  ChannelFactory channelFactory();

  ChannelPoolFactory channelPoolFactory();

  TopologyMonitor topologyMonitor();

  MetadataManager metadataManager();

  LoadBalancingPolicyWrapper loadBalancingPolicyWrapper();

  ControlConnection controlConnection();

  RequestProcessorRegistry requestProcessorRegistry();

  DriverConfigLoader configLoader();

  SchemaQueriesFactory schemaQueriesFactory();

  SchemaParserFactory schemaParserFactory();

  TokenFactoryRegistry tokenFactoryRegistry();

  ReplicationStrategyFactory replicationStrategyFactory();

  PoolManager poolManager();

  MetricsFactory metricsFactory();

  RequestThrottler requestThrottler();

  NodeStateListener nodeStateListener();

  SchemaChangeListener schemaChangeListener();

  RequestTracker requestTracker();

  /**
   * This is the filter from {@link SessionBuilder#withNodeFilter(String, Predicate)}. If the filter
   * for this profile was specified through the configuration instead, this method will return
   * {@code null}.
   */
  Predicate<Node> nodeFilter(String profileName);
}
