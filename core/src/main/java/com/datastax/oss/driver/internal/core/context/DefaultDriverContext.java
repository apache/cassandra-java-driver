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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.internal.core.CassandraProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DefaultWriteCoalescer;
import com.datastax.oss.driver.internal.core.channel.WriteCoalescer;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DefaultSchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.DefaultSchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenFactoryRegistry;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.internal.core.metrics.DropwizardMetricsFactory;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.servererrors.DefaultWriteTypeRegistry;
import com.datastax.oss.driver.internal.core.servererrors.WriteTypeRegistry;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.core.util.concurrent.CycleDetector;
import com.datastax.oss.driver.internal.core.util.concurrent.LazyReference;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;

/**
 * Default implementation of the driver context.
 *
 * <p>All non-constant components are initialized lazily. Some components depend on others, so there
 * might be deadlocks or stack overflows if the dependency graph is badly designed. This can be
 * checked automatically with the system property {@code
 * -Dcom.datastax.oss.driver.DETECT_CYCLES=true} (this might have a slight impact on startup time,
 * so the check is disabled by default).
 *
 * <p>This is DIY dependency injection. We stayed away from DI frameworks for simplicity, to avoid
 * an extra dependency, and because end users might want to access some of these components in their
 * own implementations (which wouldn't work well with compile-time approaches like Dagger).
 *
 * <p>This also provides extension points for stuff that is too low-level for the driver
 * configuration: the intent is that someone can extend this class, override one (or more) of the
 * buildXxx methods, and initialize the cluster with this new implementation.
 */
@ThreadSafe
public class DefaultDriverContext implements InternalDriverContext {

  private static final AtomicInteger SESSION_NAME_COUNTER = new AtomicInteger();

  private final CycleDetector cycleDetector =
      new CycleDetector("Detected cycle in context initialization");

  private final LazyReference<Map<String, LoadBalancingPolicy>> loadBalancingPoliciesRef =
      new LazyReference<>("loadBalancingPolicies", this::buildLoadBalancingPolicies, cycleDetector);
  private final LazyReference<ReconnectionPolicy> reconnectionPolicyRef =
      new LazyReference<>("reconnectionPolicy", this::buildReconnectionPolicy, cycleDetector);
  private final LazyReference<Map<String, RetryPolicy>> retryPoliciesRef =
      new LazyReference<>("retryPolicies", this::buildRetryPolicies, cycleDetector);
  private final LazyReference<Map<String, SpeculativeExecutionPolicy>>
      speculativeExecutionPoliciesRef =
          new LazyReference<>(
              "speculativeExecutionPolicies",
              this::buildSpeculativeExecutionPolicies,
              cycleDetector);
  private final LazyReference<TimestampGenerator> timestampGeneratorRef =
      new LazyReference<>("timestampGenerator", this::buildTimestampGenerator, cycleDetector);
  private final LazyReference<AddressTranslator> addressTranslatorRef =
      new LazyReference<>("addressTranslator", this::buildAddressTranslator, cycleDetector);
  private final LazyReference<Optional<AuthProvider>> authProviderRef =
      new LazyReference<>("authProvider", this::buildAuthProvider, cycleDetector);
  private final LazyReference<Optional<SslEngineFactory>> sslEngineFactoryRef =
      new LazyReference<>("sslEngineFactory", this::buildSslEngineFactory, cycleDetector);

  private final LazyReference<EventBus> eventBusRef =
      new LazyReference<>("eventBus", this::buildEventBus, cycleDetector);
  private final LazyReference<Compressor<ByteBuf>> compressorRef =
      new LazyReference<>("compressor", this::buildCompressor, cycleDetector);
  private final LazyReference<FrameCodec<ByteBuf>> frameCodecRef =
      new LazyReference<>("frameCodec", this::buildFrameCodec, cycleDetector);
  private final LazyReference<ProtocolVersionRegistry> protocolVersionRegistryRef =
      new LazyReference<>(
          "protocolVersionRegistry", this::buildProtocolVersionRegistry, cycleDetector);
  private final LazyReference<ConsistencyLevelRegistry> consistencyLevelRegistryRef =
      new LazyReference<>(
          "consistencyLevelRegistry", this::buildConsistencyLevelRegistry, cycleDetector);
  private final LazyReference<WriteTypeRegistry> writeTypeRegistryRef =
      new LazyReference<>("writeTypeRegistry", this::buildWriteTypeRegistry, cycleDetector);
  private final LazyReference<NettyOptions> nettyOptionsRef =
      new LazyReference<>("nettyOptions", this::buildNettyOptions, cycleDetector);
  private final LazyReference<WriteCoalescer> writeCoalescerRef =
      new LazyReference<>("writeCoalescer", this::buildWriteCoalescer, cycleDetector);
  private final LazyReference<Optional<SslHandlerFactory>> sslHandlerFactoryRef =
      new LazyReference<>("sslHandlerFactory", this::buildSslHandlerFactory, cycleDetector);
  private final LazyReference<ChannelFactory> channelFactoryRef =
      new LazyReference<>("channelFactory", this::buildChannelFactory, cycleDetector);
  private final LazyReference<TopologyMonitor> topologyMonitorRef =
      new LazyReference<>("topologyMonitor", this::buildTopologyMonitor, cycleDetector);
  private final LazyReference<MetadataManager> metadataManagerRef =
      new LazyReference<>("metadataManager", this::buildMetadataManager, cycleDetector);
  private final LazyReference<LoadBalancingPolicyWrapper> loadBalancingPolicyWrapperRef =
      new LazyReference<>(
          "loadBalancingPolicyWrapper", this::buildLoadBalancingPolicyWrapper, cycleDetector);
  private final LazyReference<ControlConnection> controlConnectionRef =
      new LazyReference<>("controlConnection", this::buildControlConnection, cycleDetector);
  private final LazyReference<RequestProcessorRegistry> requestProcessorRegistryRef =
      new LazyReference<>(
          "requestProcessorRegistry", this::buildRequestProcessorRegistry, cycleDetector);
  private final LazyReference<SchemaQueriesFactory> schemaQueriesFactoryRef =
      new LazyReference<>("schemaQueriesFactory", this::buildSchemaQueriesFactory, cycleDetector);
  private final LazyReference<SchemaParserFactory> schemaParserFactoryRef =
      new LazyReference<>("schemaParserFactory", this::buildSchemaParserFactory, cycleDetector);
  private final LazyReference<TokenFactoryRegistry> tokenFactoryRegistryRef =
      new LazyReference<>("tokenFactoryRegistry", this::buildTokenFactoryRegistry, cycleDetector);
  private final LazyReference<ReplicationStrategyFactory> replicationStrategyFactoryRef =
      new LazyReference<>(
          "replicationStrategyFactory", this::buildReplicationStrategyFactory, cycleDetector);
  private final LazyReference<PoolManager> poolManagerRef =
      new LazyReference<>("poolManager", this::buildPoolManager, cycleDetector);
  private final LazyReference<MetricsFactory> metricsFactoryRef =
      new LazyReference<>("metricsFactory", this::buildMetricsFactory, cycleDetector);
  private final LazyReference<RequestThrottler> requestThrottlerRef =
      new LazyReference<>("requestThrottler", this::buildRequestThrottler, cycleDetector);
  private final LazyReference<NodeStateListener> nodeStateListenerRef;
  private final LazyReference<SchemaChangeListener> schemaChangeListenerRef;
  private final LazyReference<RequestTracker> requestTrackerRef;

  private final DriverConfig config;
  private final DriverConfigLoader configLoader;
  private final ChannelPoolFactory channelPoolFactory = new ChannelPoolFactory();
  private final CodecRegistry codecRegistry;
  private final String sessionName;
  private final NodeStateListener nodeStateListenerFromBuilder;
  private final SchemaChangeListener schemaChangeListenerFromBuilder;
  private final RequestTracker requestTrackerFromBuilder;
  private final Map<String, Predicate<Node>> nodeFiltersFromBuilder;

  public DefaultDriverContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, Predicate<Node>> nodeFilters) {
    this.config = configLoader.getInitialConfig();
    this.configLoader = configLoader;
    DriverConfigProfile defaultProfile = config.getDefaultProfile();
    if (defaultProfile.isDefined(DefaultDriverOption.SESSION_NAME)) {
      this.sessionName = defaultProfile.getString(DefaultDriverOption.SESSION_NAME);
    } else {
      this.sessionName = "s" + SESSION_NAME_COUNTER.getAndIncrement();
    }
    this.codecRegistry = buildCodecRegistry(this.sessionName, typeCodecs);
    this.nodeStateListenerFromBuilder = nodeStateListener;
    this.nodeStateListenerRef =
        new LazyReference<>(
            "nodeStateListener",
            () -> buildNodeStateListener(nodeStateListenerFromBuilder),
            cycleDetector);
    this.schemaChangeListenerFromBuilder = schemaChangeListener;
    this.schemaChangeListenerRef =
        new LazyReference<>(
            "schemaChangeListener",
            () -> buildSchemaChangeListener(schemaChangeListenerFromBuilder),
            cycleDetector);
    this.requestTrackerFromBuilder = requestTracker;
    this.requestTrackerRef =
        new LazyReference<>(
            "requestTracker", () -> buildRequestTracker(requestTrackerFromBuilder), cycleDetector);
    this.nodeFiltersFromBuilder = nodeFilters;
  }

  protected Map<String, LoadBalancingPolicy> buildLoadBalancingPolicies() {
    return Reflection.buildFromConfigProfiles(
        this,
        DefaultDriverOption.LOAD_BALANCING_POLICY,
        LoadBalancingPolicy.class,
        "com.datastax.oss.driver.internal.core.loadbalancing");
  }

  protected Map<String, RetryPolicy> buildRetryPolicies() {
    return Reflection.buildFromConfigProfiles(
        this,
        DefaultDriverOption.RETRY_POLICY,
        RetryPolicy.class,
        "com.datastax.oss.driver.internal.core.retry");
  }

  protected Map<String, SpeculativeExecutionPolicy> buildSpeculativeExecutionPolicies() {
    return Reflection.buildFromConfigProfiles(
        this,
        DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY,
        SpeculativeExecutionPolicy.class,
        "com.datastax.oss.driver.internal.core.specex");
  }

  protected TimestampGenerator buildTimestampGenerator() {
    return Reflection.buildFromConfig(
            this,
            DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS,
            TimestampGenerator.class,
            "com.datastax.oss.driver.internal.core.time")
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Missing timestamp generator, check your configuration (%s)",
                        DefaultDriverOption.TIMESTAMP_GENERATOR_CLASS)));
  }

  protected ReconnectionPolicy buildReconnectionPolicy() {
    return Reflection.buildFromConfig(
            this,
            DefaultDriverOption.RECONNECTION_POLICY_CLASS,
            ReconnectionPolicy.class,
            "com.datastax.oss.driver.internal.core.connection")
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Missing reconnection policy, check your configuration (%s)",
                        DefaultDriverOption.RECONNECTION_POLICY_CLASS)));
  }

  protected AddressTranslator buildAddressTranslator() {
    return Reflection.buildFromConfig(
            this,
            DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS,
            AddressTranslator.class,
            "com.datastax.oss.driver.internal.core.addresstranslation")
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Missing address translator, check your configuration (%s)",
                        DefaultDriverOption.ADDRESS_TRANSLATOR_CLASS)));
  }

  protected Optional<AuthProvider> buildAuthProvider() {
    return Reflection.buildFromConfig(
        this,
        DefaultDriverOption.AUTH_PROVIDER_CLASS,
        AuthProvider.class,
        "com.datastax.oss.driver.internal.core.auth");
  }

  protected Optional<SslEngineFactory> buildSslEngineFactory() {
    return Reflection.buildFromConfig(
        this,
        DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS,
        SslEngineFactory.class,
        "com.datastax.oss.driver.internal.core.ssl");
  }

  protected EventBus buildEventBus() {
    return new EventBus(sessionName());
  }

  @SuppressWarnings("unchecked")
  protected Compressor<ByteBuf> buildCompressor() {
    return (Compressor<ByteBuf>)
        Reflection.buildFromConfig(
                this,
                DefaultDriverOption.PROTOCOL_COMPRESSOR_CLASS,
                Compressor.class,
                "com.datastax.oss.driver.internal.core.protocol")
            .orElse(Compressor.none());
  }

  protected FrameCodec<ByteBuf> buildFrameCodec() {
    return FrameCodec.defaultClient(
        new ByteBufPrimitiveCodec(nettyOptions().allocator()), compressor());
  }

  protected ProtocolVersionRegistry buildProtocolVersionRegistry() {
    return new CassandraProtocolVersionRegistry(sessionName());
  }

  protected ConsistencyLevelRegistry buildConsistencyLevelRegistry() {
    return new DefaultConsistencyLevelRegistry();
  }

  protected WriteTypeRegistry buildWriteTypeRegistry() {
    return new DefaultWriteTypeRegistry();
  }

  protected NettyOptions buildNettyOptions() {
    return new DefaultNettyOptions(this);
  }

  protected Optional<SslHandlerFactory> buildSslHandlerFactory() {
    // If a JDK-based factory was provided through the public API, syncWrapper it
    return buildSslEngineFactory().map(JdkSslHandlerFactory::new);

    // For more advanced options (like using Netty's native OpenSSL support instead of the JDK),
    // extend DefaultDriverContext and override this method
  }

  protected WriteCoalescer buildWriteCoalescer() {
    return new DefaultWriteCoalescer(this);
  }

  protected ChannelFactory buildChannelFactory() {
    return new ChannelFactory(this);
  }

  protected TopologyMonitor buildTopologyMonitor() {
    return new DefaultTopologyMonitor(this);
  }

  protected MetadataManager buildMetadataManager() {
    return new MetadataManager(this);
  }

  protected LoadBalancingPolicyWrapper buildLoadBalancingPolicyWrapper() {
    return new LoadBalancingPolicyWrapper(this, loadBalancingPolicies());
  }

  protected ControlConnection buildControlConnection() {
    return new ControlConnection(this);
  }

  protected RequestProcessorRegistry buildRequestProcessorRegistry() {
    return RequestProcessorRegistry.defaultCqlProcessors(sessionName());
  }

  protected CodecRegistry buildCodecRegistry(String logPrefix, List<TypeCodec<?>> codecs) {
    TypeCodec<?>[] array = new TypeCodec<?>[codecs.size()];
    return new DefaultCodecRegistry(logPrefix, codecs.toArray(array));
  }

  protected SchemaQueriesFactory buildSchemaQueriesFactory() {
    return new DefaultSchemaQueriesFactory(this);
  }

  protected SchemaParserFactory buildSchemaParserFactory() {
    return new DefaultSchemaParserFactory(this);
  }

  protected TokenFactoryRegistry buildTokenFactoryRegistry() {
    return new DefaultTokenFactoryRegistry(this);
  }

  protected ReplicationStrategyFactory buildReplicationStrategyFactory() {
    return new DefaultReplicationStrategyFactory(this);
  }

  protected PoolManager buildPoolManager() {
    return new PoolManager(this);
  }

  protected MetricsFactory buildMetricsFactory() {
    return new DropwizardMetricsFactory(this);
  }

  protected RequestThrottler buildRequestThrottler() {
    return Reflection.buildFromConfig(
            this,
            DefaultDriverOption.REQUEST_THROTTLER_CLASS,
            RequestThrottler.class,
            "com.datastax.oss.driver.internal.core.session.throttling")
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Missing request throttler, check your configuration (%s)",
                        DefaultDriverOption.REQUEST_THROTTLER_CLASS)));
  }

  protected NodeStateListener buildNodeStateListener(
      NodeStateListener nodeStateListenerFromBuilder) {
    return (nodeStateListenerFromBuilder != null)
        ? nodeStateListenerFromBuilder
        : Reflection.buildFromConfig(
                this,
                DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASS,
                NodeStateListener.class,
                "com.datastax.oss.driver.internal.core.metadata")
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Missing node state listener, check your configuration (%s)",
                            DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASS)));
  }

  protected SchemaChangeListener buildSchemaChangeListener(
      SchemaChangeListener schemaChangeListenerFromBuilder) {
    return (schemaChangeListenerFromBuilder != null)
        ? schemaChangeListenerFromBuilder
        : Reflection.buildFromConfig(
                this,
                DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASS,
                SchemaChangeListener.class,
                "com.datastax.oss.driver.internal.core.metadata.schema")
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Missing schema change listener, check your configuration (%s)",
                            DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASS)));
  }

  protected RequestTracker buildRequestTracker(RequestTracker requestTrackerFromBuilder) {
    return (requestTrackerFromBuilder != null)
        ? requestTrackerFromBuilder
        : Reflection.buildFromConfig(
                this,
                DefaultDriverOption.REQUEST_TRACKER_CLASS,
                RequestTracker.class,
                "com.datastax.oss.driver.internal.core.tracker")
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Missing request tracker, check your configuration (%s)",
                            DefaultDriverOption.REQUEST_TRACKER_CLASS)));
  }

  @Override
  public String sessionName() {
    return sessionName;
  }

  @Override
  public DriverConfig config() {
    return config;
  }

  @Override
  public DriverConfigLoader configLoader() {
    return configLoader;
  }

  @Override
  public Map<String, LoadBalancingPolicy> loadBalancingPolicies() {
    return loadBalancingPoliciesRef.get();
  }

  @Override
  public Map<String, RetryPolicy> retryPolicies() {
    return retryPoliciesRef.get();
  }

  @Override
  public Map<String, SpeculativeExecutionPolicy> speculativeExecutionPolicies() {
    return speculativeExecutionPoliciesRef.get();
  }

  @Override
  public TimestampGenerator timestampGenerator() {
    return timestampGeneratorRef.get();
  }

  @Override
  public ReconnectionPolicy reconnectionPolicy() {
    return reconnectionPolicyRef.get();
  }

  @Override
  public AddressTranslator addressTranslator() {
    return addressTranslatorRef.get();
  }

  @Override
  public Optional<AuthProvider> authProvider() {
    return authProviderRef.get();
  }

  @Override
  public Optional<SslEngineFactory> sslEngineFactory() {
    return sslEngineFactoryRef.get();
  }

  @Override
  public EventBus eventBus() {
    return eventBusRef.get();
  }

  @Override
  public Compressor<ByteBuf> compressor() {
    return compressorRef.get();
  }

  @Override
  public FrameCodec<ByteBuf> frameCodec() {
    return frameCodecRef.get();
  }

  @Override
  public ProtocolVersionRegistry protocolVersionRegistry() {
    return protocolVersionRegistryRef.get();
  }

  @Override
  public ConsistencyLevelRegistry consistencyLevelRegistry() {
    return consistencyLevelRegistryRef.get();
  }

  @Override
  public WriteTypeRegistry writeTypeRegistry() {
    return writeTypeRegistryRef.get();
  }

  @Override
  public NettyOptions nettyOptions() {
    return nettyOptionsRef.get();
  }

  @Override
  public WriteCoalescer writeCoalescer() {
    return writeCoalescerRef.get();
  }

  @Override
  public Optional<SslHandlerFactory> sslHandlerFactory() {
    return sslHandlerFactoryRef.get();
  }

  @Override
  public ChannelFactory channelFactory() {
    return channelFactoryRef.get();
  }

  @Override
  public ChannelPoolFactory channelPoolFactory() {
    return channelPoolFactory;
  }

  @Override
  public TopologyMonitor topologyMonitor() {
    return topologyMonitorRef.get();
  }

  @Override
  public MetadataManager metadataManager() {
    return metadataManagerRef.get();
  }

  @Override
  public LoadBalancingPolicyWrapper loadBalancingPolicyWrapper() {
    return loadBalancingPolicyWrapperRef.get();
  }

  @Override
  public ControlConnection controlConnection() {
    return controlConnectionRef.get();
  }

  @Override
  public RequestProcessorRegistry requestProcessorRegistry() {
    return requestProcessorRegistryRef.get();
  }

  @Override
  public SchemaQueriesFactory schemaQueriesFactory() {
    return schemaQueriesFactoryRef.get();
  }

  @Override
  public SchemaParserFactory schemaParserFactory() {
    return schemaParserFactoryRef.get();
  }

  @Override
  public TokenFactoryRegistry tokenFactoryRegistry() {
    return tokenFactoryRegistryRef.get();
  }

  @Override
  public ReplicationStrategyFactory replicationStrategyFactory() {
    return replicationStrategyFactoryRef.get();
  }

  @Override
  public PoolManager poolManager() {
    return poolManagerRef.get();
  }

  @Override
  public MetricsFactory metricsFactory() {
    return metricsFactoryRef.get();
  }

  @Override
  public RequestThrottler requestThrottler() {
    return requestThrottlerRef.get();
  }

  @Override
  public NodeStateListener nodeStateListener() {
    return nodeStateListenerRef.get();
  }

  @Override
  public SchemaChangeListener schemaChangeListener() {
    return schemaChangeListenerRef.get();
  }

  @Override
  public RequestTracker requestTracker() {
    return requestTrackerRef.get();
  }

  @Override
  public Predicate<Node> nodeFilter(String profileName) {
    return nodeFiltersFromBuilder.get(profileName);
  }

  @Override
  public CodecRegistry codecRegistry() {
    return codecRegistry;
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return channelFactory().getProtocolVersion();
  }
}
