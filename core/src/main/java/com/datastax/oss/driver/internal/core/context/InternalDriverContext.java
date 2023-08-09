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

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.SessionBuilder;
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
import com.datastax.oss.driver.internal.core.metrics.MetricIdGenerator;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.servererrors.WriteTypeRegistry;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.internal.core.tracker.RequestLogFormatter;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.SegmentCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.buffer.ByteBuf;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/** Extends the driver context with additional components that are not exposed by our public API. */
public interface InternalDriverContext extends DriverContext {

  @NonNull
  EventBus getEventBus();

  @NonNull
  Compressor<ByteBuf> getCompressor();

  @NonNull
  PrimitiveCodec<ByteBuf> getPrimitiveCodec();

  @NonNull
  FrameCodec<ByteBuf> getFrameCodec();

  @NonNull
  SegmentCodec<ByteBuf> getSegmentCodec();

  @NonNull
  ProtocolVersionRegistry getProtocolVersionRegistry();

  @NonNull
  ConsistencyLevelRegistry getConsistencyLevelRegistry();

  @NonNull
  WriteTypeRegistry getWriteTypeRegistry();

  @NonNull
  NettyOptions getNettyOptions();

  @NonNull
  WriteCoalescer getWriteCoalescer();

  @NonNull
  Optional<SslHandlerFactory> getSslHandlerFactory();

  @NonNull
  ChannelFactory getChannelFactory();

  @NonNull
  ChannelPoolFactory getChannelPoolFactory();

  @NonNull
  TopologyMonitor getTopologyMonitor();

  @NonNull
  MetadataManager getMetadataManager();

  @NonNull
  LoadBalancingPolicyWrapper getLoadBalancingPolicyWrapper();

  @NonNull
  ControlConnection getControlConnection();

  @NonNull
  RequestProcessorRegistry getRequestProcessorRegistry();

  @NonNull
  SchemaQueriesFactory getSchemaQueriesFactory();

  @NonNull
  SchemaParserFactory getSchemaParserFactory();

  @NonNull
  TokenFactoryRegistry getTokenFactoryRegistry();

  @NonNull
  ReplicationStrategyFactory getReplicationStrategyFactory();

  @NonNull
  PoolManager getPoolManager();

  @NonNull
  MetricsFactory getMetricsFactory();

  @NonNull
  MetricIdGenerator getMetricIdGenerator();

  /**
   * The value that was passed to {@link SessionBuilder#withLocalDatacenter(String,String)} for this
   * particular profile. If it was specified through the configuration instead, this method will
   * return {@code null}.
   */
  @Nullable
  String getLocalDatacenter(@NonNull String profileName);

  /**
   * This is the filter from {@link SessionBuilder#withNodeFilter(String, Predicate)}. If the filter
   * for this profile was specified through the configuration instead, this method will return
   * {@code null}.
   *
   * @deprecated Use {@link #getNodeDistanceEvaluator(String)} instead.
   */
  @Nullable
  @Deprecated
  Predicate<Node> getNodeFilter(@NonNull String profileName);

  /**
   * This is the node distance evaluator from {@link
   * SessionBuilder#withNodeDistanceEvaluator(String, NodeDistanceEvaluator)}. If the evaluator for
   * this profile was specified through the configuration instead, this method will return {@code
   * null}.
   */
  @Nullable
  NodeDistanceEvaluator getNodeDistanceEvaluator(@NonNull String profileName);

  /**
   * The {@link ClassLoader} to use to reflectively load class names defined in configuration. If
   * null, the driver attempts to use the same {@link ClassLoader} that loaded the core driver
   * classes.
   */
  @Nullable
  ClassLoader getClassLoader();

  /**
   * Retrieves the map of options to send in a Startup message. The returned map will be used to
   * construct a {@link com.datastax.oss.protocol.internal.request.Startup} instance when
   * initializing the native protocol handshake.
   */
  @NonNull
  Map<String, String> getStartupOptions();

  /**
   * A list of additional components to notify of session lifecycle events.
   *
   * <p>For historical reasons, this method has a default implementation that returns an empty list.
   * The built-in {@link DefaultDriverContext} overrides it to plug in the Insights monitoring
   * listener. Custom driver extensions might override this method to add their own components.
   *
   * <p>Note that the driver assumes that the returned list is constant; there is no way to add
   * listeners dynamically.
   */
  @NonNull
  default List<LifecycleListener> getLifecycleListeners() {
    return Collections.emptyList();
  }

  /**
   * A {@link RequestLogFormatter} instance based on this {@link DriverContext}.
   *
   * <p>The {@link RequestLogFormatter} instance returned here will use the settings in
   * advanced.request-tracker when formatting requests.
   */
  @NonNull
  RequestLogFormatter getRequestLogFormatter();

  /**
   * A metric registry for storing metrics.
   *
   * <p>This will return the object from {@link
   * SessionBuilder#withMetricRegistry(java.lang.Object)}. Access to this registry object is only
   * intended for {@link MetricsFactory} implementations that need to expose a way to specify the
   * registry external to the Factory implementation itself.
   *
   * <p>The default metrics framework used by the Driver is DropWizard and does not need an external
   * metrics registry object.
   */
  @Nullable
  default Object getMetricRegistry() {
    return null;
  }
}
