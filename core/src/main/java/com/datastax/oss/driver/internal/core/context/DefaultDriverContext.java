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
package com.datastax.oss.driver.internal.core.context;

import static com.datastax.oss.driver.internal.core.util.Dependency.JACKSON;

import com.datastax.dse.driver.api.core.config.DseDriverOption;
import com.datastax.dse.driver.internal.core.InsightsClientLifecycleListener;
import com.datastax.dse.driver.internal.core.type.codec.DseTypeCodecsRegistrar;
import com.datastax.dse.protocol.internal.DseProtocolV1ClientCodecs;
import com.datastax.dse.protocol.internal.DseProtocolV2ClientCodecs;
import com.datastax.dse.protocol.internal.ProtocolV4ClientCodecsForDse;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.api.core.time.TimestampGenerator;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.internal.core.ConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.DefaultConsistencyLevelRegistry;
import com.datastax.oss.driver.internal.core.DefaultProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.channel.ChannelFactory;
import com.datastax.oss.driver.internal.core.channel.DefaultWriteCoalescer;
import com.datastax.oss.driver.internal.core.channel.WriteCoalescer;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.CloudTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.DefaultTopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.MultiplexingNodeStateListener;
import com.datastax.oss.driver.internal.core.metadata.NoopNodeStateListener;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.metadata.schema.MultiplexingSchemaChangeListener;
import com.datastax.oss.driver.internal.core.metadata.schema.NoopSchemaChangeListener;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.DefaultSchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.parsing.SchemaParserFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.DefaultSchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.schema.queries.SchemaQueriesFactory;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenFactoryRegistry;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.internal.core.metrics.MetricIdGenerator;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.protocol.BuiltInCompressors;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.servererrors.DefaultWriteTypeRegistry;
import com.datastax.oss.driver.internal.core.servererrors.WriteTypeRegistry;
import com.datastax.oss.driver.internal.core.session.BuiltInRequestProcessors;
import com.datastax.oss.driver.internal.core.session.PoolManager;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.driver.internal.core.tracker.MultiplexingRequestTracker;
import com.datastax.oss.driver.internal.core.tracker.NoopRequestTracker;
import com.datastax.oss.driver.internal.core.tracker.RequestLogFormatter;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.internal.core.util.DefaultDependencyChecker;
import com.datastax.oss.driver.internal.core.util.Reflection;
import com.datastax.oss.driver.internal.core.util.concurrent.CycleDetector;
import com.datastax.oss.driver.internal.core.util.concurrent.LazyReference;
import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.PrimitiveCodec;
import com.datastax.oss.protocol.internal.ProtocolV3ClientCodecs;
import com.datastax.oss.protocol.internal.ProtocolV5ClientCodecs;
import com.datastax.oss.protocol.internal.ProtocolV6ClientCodecs;
import com.datastax.oss.protocol.internal.SegmentCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.buffer.ByteBuf;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  private static final Logger LOG = LoggerFactory.getLogger(InternalDriverContext.class);
  private static final AtomicInteger SESSION_NAME_COUNTER = new AtomicInteger();

  protected final CycleDetector cycleDetector =
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
  private final LazyReference<Optional<SslEngineFactory>> sslEngineFactoryRef;

  private final LazyReference<EventBus> eventBusRef =
      new LazyReference<>("eventBus", this::buildEventBus, cycleDetector);
  private final LazyReference<Compressor<ByteBuf>> compressorRef =
      new LazyReference<>("compressor", this::buildCompressor, cycleDetector);
  private final LazyReference<PrimitiveCodec<ByteBuf>> primitiveCodecRef =
      new LazyReference<>("primitiveCodec", this::buildPrimitiveCodec, cycleDetector);
  private final LazyReference<FrameCodec<ByteBuf>> frameCodecRef =
      new LazyReference<>("frameCodec", this::buildFrameCodec, cycleDetector);
  private final LazyReference<SegmentCodec<ByteBuf>> segmentCodecRef =
      new LazyReference<>("segmentCodec", this::buildSegmentCodec, cycleDetector);
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
  private final LazyReference<MetricIdGenerator> metricIdGeneratorRef =
      new LazyReference<>("metricIdGenerator", this::buildMetricIdGenerator, cycleDetector);
  private final LazyReference<RequestThrottler> requestThrottlerRef =
      new LazyReference<>("requestThrottler", this::buildRequestThrottler, cycleDetector);
  private final LazyReference<Map<String, String>> startupOptionsRef =
      new LazyReference<>("startupOptions", this::buildStartupOptions, cycleDetector);
  private final LazyReference<NodeStateListener> nodeStateListenerRef;
  private final LazyReference<SchemaChangeListener> schemaChangeListenerRef;
  private final LazyReference<RequestTracker> requestTrackerRef;
  private final LazyReference<Optional<AuthProvider>> authProviderRef;
  private final LazyReference<List<LifecycleListener>> lifecycleListenersRef =
      new LazyReference<>("lifecycleListeners", this::buildLifecycleListeners, cycleDetector);

  private final DriverConfig config;
  private final DriverConfigLoader configLoader;
  private final ChannelPoolFactory channelPoolFactory = new ChannelPoolFactory();
  private final CodecRegistry codecRegistry;
  private final String sessionName;
  private final NodeStateListener nodeStateListenerFromBuilder;
  private final SchemaChangeListener schemaChangeListenerFromBuilder;
  private final RequestTracker requestTrackerFromBuilder;
  private final Map<String, String> localDatacentersFromBuilder;
  private final Map<String, Predicate<Node>> nodeFiltersFromBuilder;
  private final Map<String, NodeDistanceEvaluator> nodeDistanceEvaluatorsFromBuilder;
  private final ClassLoader classLoader;
  private final InetSocketAddress cloudProxyAddress;
  private final LazyReference<RequestLogFormatter> requestLogFormatterRef =
      new LazyReference<>("requestLogFormatter", this::buildRequestLogFormatter, cycleDetector);
  private final UUID startupClientId;
  private final String startupApplicationName;
  private final String startupApplicationVersion;
  private final Object metricRegistry;
  // A stack trace captured in the constructor. Used to extract information about the client
  // application.
  private final StackTraceElement[] initStackTrace;

  public DefaultDriverContext(
      DriverConfigLoader configLoader, ProgrammaticArguments programmaticArguments) {
    this.config = configLoader.getInitialConfig();
    this.configLoader = configLoader;
    DriverExecutionProfile defaultProfile = config.getDefaultProfile();
    if (defaultProfile.isDefined(DefaultDriverOption.SESSION_NAME)) {
      this.sessionName = defaultProfile.getString(DefaultDriverOption.SESSION_NAME);
    } else {
      this.sessionName = "s" + SESSION_NAME_COUNTER.getAndIncrement();
    }
    this.localDatacentersFromBuilder = programmaticArguments.getLocalDatacenters();
    this.codecRegistry = buildCodecRegistry(programmaticArguments);
    this.nodeStateListenerFromBuilder = programmaticArguments.getNodeStateListener();
    this.nodeStateListenerRef =
        new LazyReference<>(
            "nodeStateListener",
            () -> buildNodeStateListener(nodeStateListenerFromBuilder),
            cycleDetector);
    this.schemaChangeListenerFromBuilder = programmaticArguments.getSchemaChangeListener();
    this.schemaChangeListenerRef =
        new LazyReference<>(
            "schemaChangeListener",
            () -> buildSchemaChangeListener(schemaChangeListenerFromBuilder),
            cycleDetector);
    this.requestTrackerFromBuilder = programmaticArguments.getRequestTracker();

    this.authProviderRef =
        new LazyReference<>(
            "authProvider",
            () -> buildAuthProvider(programmaticArguments.getAuthProvider()),
            cycleDetector);
    this.requestTrackerRef =
        new LazyReference<>(
            "requestTracker", () -> buildRequestTracker(requestTrackerFromBuilder), cycleDetector);
    this.sslEngineFactoryRef =
        new LazyReference<>(
            "sslEngineFactory",
            () -> buildSslEngineFactory(programmaticArguments.getSslEngineFactory()),
            cycleDetector);
    @SuppressWarnings("deprecation")
    Map<String, Predicate<Node>> nodeFilters = programmaticArguments.getNodeFilters();
    this.nodeFiltersFromBuilder = nodeFilters;
    this.nodeDistanceEvaluatorsFromBuilder = programmaticArguments.getNodeDistanceEvaluators();
    this.classLoader = programmaticArguments.getClassLoader();
    this.cloudProxyAddress = programmaticArguments.getCloudProxyAddress();
    this.startupClientId = programmaticArguments.getStartupClientId();
    this.startupApplicationName = programmaticArguments.getStartupApplicationName();
    this.startupApplicationVersion = programmaticArguments.getStartupApplicationVersion();
    StackTraceElement[] stackTrace;
    try {
      stackTrace = Thread.currentThread().getStackTrace();
    } catch (Exception ex) {
      // ignore and use empty
      stackTrace = new StackTraceElement[] {};
    }
    this.initStackTrace = stackTrace;
    this.metricRegistry = programmaticArguments.getMetricRegistry();
  }

  /**
   * @deprecated this constructor only exists for backward compatibility. Please use {@link
   *     #DefaultDriverContext(DriverConfigLoader, ProgrammaticArguments)} instead.
   */
  @Deprecated
  public DefaultDriverContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, String> localDatacenters,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader) {
    this(
        configLoader,
        ProgrammaticArguments.builder()
            .addTypeCodecs(typeCodecs.toArray(new TypeCodec<?>[0]))
            .withNodeStateListener(nodeStateListener)
            .withSchemaChangeListener(schemaChangeListener)
            .withRequestTracker(requestTracker)
            .withLocalDatacenters(localDatacenters)
            .withNodeFilters(nodeFilters)
            .withClassLoader(classLoader)
            .build());
  }

  /**
   * Builds a map of options to send in a Startup message.
   *
   * @see #getStartupOptions()
   */
  protected Map<String, String> buildStartupOptions() {
    return new StartupOptionsBuilder(this)
        .withClientId(startupClientId)
        .withApplicationName(startupApplicationName)
        .withApplicationVersion(startupApplicationVersion)
        .build();
  }

  protected Map<String, LoadBalancingPolicy> buildLoadBalancingPolicies() {
    return Reflection.buildFromConfigProfiles(
        this,
        DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
        DefaultDriverOption.LOAD_BALANCING_POLICY,
        LoadBalancingPolicy.class,
        "com.datastax.oss.driver.internal.core.loadbalancing",
        "com.datastax.dse.driver.internal.core.loadbalancing");
  }

  protected Map<String, RetryPolicy> buildRetryPolicies() {
    return Reflection.buildFromConfigProfiles(
        this,
        DefaultDriverOption.RETRY_POLICY_CLASS,
        DefaultDriverOption.RETRY_POLICY,
        RetryPolicy.class,
        "com.datastax.oss.driver.internal.core.retry");
  }

  protected Map<String, SpeculativeExecutionPolicy> buildSpeculativeExecutionPolicies() {
    return Reflection.buildFromConfigProfiles(
        this,
        DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
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

  protected Optional<SslEngineFactory> buildSslEngineFactory(SslEngineFactory factoryFromBuilder) {
    return (factoryFromBuilder != null)
        ? Optional.of(factoryFromBuilder)
        : Reflection.buildFromConfig(
            this,
            DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS,
            SslEngineFactory.class,
            "com.datastax.oss.driver.internal.core.ssl");
  }

  protected EventBus buildEventBus() {
    return new EventBus(getSessionName());
  }

  protected Compressor<ByteBuf> buildCompressor() {
    DriverExecutionProfile defaultProfile = getConfig().getDefaultProfile();
    String name = defaultProfile.getString(DefaultDriverOption.PROTOCOL_COMPRESSION, "none");
    assert name != null : "should use default value";
    return BuiltInCompressors.newInstance(name, this);
  }

  protected PrimitiveCodec<ByteBuf> buildPrimitiveCodec() {
    return new ByteBufPrimitiveCodec(getNettyOptions().allocator());
  }

  protected FrameCodec<ByteBuf> buildFrameCodec() {
    return new FrameCodec<>(
        getPrimitiveCodec(),
        getCompressor(),
        new ProtocolV3ClientCodecs(),
        new ProtocolV4ClientCodecsForDse(),
        new ProtocolV5ClientCodecs(),
        new ProtocolV6ClientCodecs(),
        new DseProtocolV1ClientCodecs(),
        new DseProtocolV2ClientCodecs());
  }

  protected SegmentCodec<ByteBuf> buildSegmentCodec() {
    return new SegmentCodec<>(getPrimitiveCodec(), getCompressor());
  }

  protected ProtocolVersionRegistry buildProtocolVersionRegistry() {
    return new DefaultProtocolVersionRegistry(getSessionName());
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
    // If a JDK-based factory was provided through the public API, wrap it
    return getSslEngineFactory().map(JdkSslHandlerFactory::new);

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
    if (cloudProxyAddress == null) {
      return new DefaultTopologyMonitor(this);
    }
    return new CloudTopologyMonitor(this, cloudProxyAddress);
  }

  protected MetadataManager buildMetadataManager() {
    return new MetadataManager(this);
  }

  protected LoadBalancingPolicyWrapper buildLoadBalancingPolicyWrapper() {
    return new LoadBalancingPolicyWrapper(this, getLoadBalancingPolicies());
  }

  protected ControlConnection buildControlConnection() {
    return new ControlConnection(this);
  }

  protected RequestProcessorRegistry buildRequestProcessorRegistry() {
    List<RequestProcessor<?, ?>> processors =
        BuiltInRequestProcessors.createDefaultProcessors(this);
    return new RequestProcessorRegistry(
        getSessionName(), processors.toArray(new RequestProcessor[0]));
  }

  protected CodecRegistry buildCodecRegistry(ProgrammaticArguments arguments) {
    MutableCodecRegistry registry = arguments.getCodecRegistry();
    if (registry == null) {
      registry = new DefaultCodecRegistry(this.sessionName);
    }
    registry.register(arguments.getTypeCodecs());
    DseTypeCodecsRegistrar.registerDseCodecs(registry);
    return registry;
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
    return Reflection.buildFromConfig(
            this,
            DefaultDriverOption.METRICS_FACTORY_CLASS,
            MetricsFactory.class,
            "com.datastax.oss.driver.internal.core.metrics",
            "com.datastax.oss.driver.internal.metrics.microprofile",
            "com.datastax.oss.driver.internal.metrics.micrometer")
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Missing metrics factory, check your config (%s)",
                        DefaultDriverOption.METRICS_FACTORY_CLASS)));
  }

  protected MetricIdGenerator buildMetricIdGenerator() {
    return Reflection.buildFromConfig(
            this,
            DefaultDriverOption.METRICS_ID_GENERATOR_CLASS,
            MetricIdGenerator.class,
            "com.datastax.oss.driver.internal.core.metrics")
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    String.format(
                        "Missing metric descriptor, check your config (%s)",
                        DefaultDriverOption.METRICS_ID_GENERATOR_CLASS)));
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
    List<NodeStateListener> listeners = new ArrayList<>();
    if (nodeStateListenerFromBuilder != null) {
      listeners.add(nodeStateListenerFromBuilder);
    }
    DefaultDriverOption newOption = DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASSES;
    @SuppressWarnings("deprecation")
    DefaultDriverOption legacyOption = DefaultDriverOption.METADATA_NODE_STATE_LISTENER_CLASS;
    DriverExecutionProfile profile = config.getDefaultProfile();
    if (profile.isDefined(newOption)) {
      listeners.addAll(
          Reflection.buildFromConfigList(
              this,
              newOption,
              NodeStateListener.class,
              "com.datastax.oss.driver.internal.core.metadata"));
    }
    if (profile.isDefined(legacyOption)) {
      LOG.warn(
          "Option {} has been deprecated and will be removed in a future release; please use option {} instead.",
          legacyOption,
          newOption);
      Reflection.buildFromConfig(
              this,
              legacyOption,
              NodeStateListener.class,
              "com.datastax.oss.driver.internal.core.metadata")
          .ifPresent(listeners::add);
    }
    if (listeners.isEmpty()) {
      return new NoopNodeStateListener(this);
    } else if (listeners.size() == 1) {
      return listeners.get(0);
    } else {
      return new MultiplexingNodeStateListener(listeners);
    }
  }

  protected SchemaChangeListener buildSchemaChangeListener(
      SchemaChangeListener schemaChangeListenerFromBuilder) {
    List<SchemaChangeListener> listeners = new ArrayList<>();
    if (schemaChangeListenerFromBuilder != null) {
      listeners.add(schemaChangeListenerFromBuilder);
    }
    DefaultDriverOption newOption = DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASSES;
    @SuppressWarnings("deprecation")
    DefaultDriverOption legacyOption = DefaultDriverOption.METADATA_SCHEMA_CHANGE_LISTENER_CLASS;
    DriverExecutionProfile profile = config.getDefaultProfile();
    if (profile.isDefined(newOption)) {
      listeners.addAll(
          Reflection.buildFromConfigList(
              this,
              newOption,
              SchemaChangeListener.class,
              "com.datastax.oss.driver.internal.core.metadata.schema"));
    }
    if (profile.isDefined(legacyOption)) {
      LOG.warn(
          "Option {} has been deprecated and will be removed in a future release; please use option {} instead.",
          legacyOption,
          newOption);
      Reflection.buildFromConfig(
              this,
              legacyOption,
              SchemaChangeListener.class,
              "com.datastax.oss.driver.internal.core.metadata.schema")
          .ifPresent(listeners::add);
    }
    if (listeners.isEmpty()) {
      return new NoopSchemaChangeListener(this);
    } else if (listeners.size() == 1) {
      return listeners.get(0);
    } else {
      return new MultiplexingSchemaChangeListener(listeners);
    }
  }

  protected RequestTracker buildRequestTracker(RequestTracker requestTrackerFromBuilder) {
    List<RequestTracker> trackers = new ArrayList<>();
    if (requestTrackerFromBuilder != null) {
      trackers.add(requestTrackerFromBuilder);
    }
    for (LoadBalancingPolicy lbp : this.getLoadBalancingPolicies().values()) {
      lbp.getRequestTracker().ifPresent(trackers::add);
    }
    DefaultDriverOption newOption = DefaultDriverOption.REQUEST_TRACKER_CLASSES;
    @SuppressWarnings("deprecation")
    DefaultDriverOption legacyOption = DefaultDriverOption.REQUEST_TRACKER_CLASS;
    DriverExecutionProfile profile = config.getDefaultProfile();
    if (profile.isDefined(newOption)) {
      trackers.addAll(
          Reflection.buildFromConfigList(
              this,
              newOption,
              RequestTracker.class,
              "com.datastax.oss.driver.internal.core.tracker"));
    }
    if (profile.isDefined(legacyOption)) {
      LOG.warn(
          "Option {} has been deprecated and will be removed in a future release; please use option {} instead.",
          legacyOption,
          newOption);
      Reflection.buildFromConfig(
              this,
              legacyOption,
              RequestTracker.class,
              "com.datastax.oss.driver.internal.core.tracker")
          .ifPresent(trackers::add);
    }
    if (trackers.isEmpty()) {
      return new NoopRequestTracker(this);
    } else if (trackers.size() == 1) {
      return trackers.get(0);
    } else {
      return new MultiplexingRequestTracker(trackers);
    }
  }

  protected Optional<AuthProvider> buildAuthProvider(AuthProvider authProviderFromBuilder) {
    return (authProviderFromBuilder != null)
        ? Optional.of(authProviderFromBuilder)
        : Reflection.buildFromConfig(
            this,
            DefaultDriverOption.AUTH_PROVIDER_CLASS,
            AuthProvider.class,
            "com.datastax.oss.driver.internal.core.auth",
            "com.datastax.dse.driver.internal.core.auth");
  }

  protected List<LifecycleListener> buildLifecycleListeners() {
    if (DefaultDependencyChecker.isPresent(JACKSON)) {
      return Collections.singletonList(new InsightsClientLifecycleListener(this, initStackTrace));
    } else {
      if (config.getDefaultProfile().getBoolean(DseDriverOption.MONITOR_REPORTING_ENABLED)) {
        LOG.info(
            "Could not initialize Insights monitoring; "
                + "this is normal if Jackson was explicitly excluded from classpath");
      }
      return Collections.emptyList();
    }
  }

  @NonNull
  @Override
  public String getSessionName() {
    return sessionName;
  }

  @NonNull
  @Override
  public DriverConfig getConfig() {
    return config;
  }

  @NonNull
  @Override
  public DriverConfigLoader getConfigLoader() {
    return configLoader;
  }

  @NonNull
  @Override
  public Map<String, LoadBalancingPolicy> getLoadBalancingPolicies() {
    return loadBalancingPoliciesRef.get();
  }

  @NonNull
  @Override
  public Map<String, RetryPolicy> getRetryPolicies() {
    return retryPoliciesRef.get();
  }

  @NonNull
  @Override
  public Map<String, SpeculativeExecutionPolicy> getSpeculativeExecutionPolicies() {
    return speculativeExecutionPoliciesRef.get();
  }

  @NonNull
  @Override
  public TimestampGenerator getTimestampGenerator() {
    return timestampGeneratorRef.get();
  }

  @NonNull
  @Override
  public ReconnectionPolicy getReconnectionPolicy() {
    return reconnectionPolicyRef.get();
  }

  @NonNull
  @Override
  public AddressTranslator getAddressTranslator() {
    return addressTranslatorRef.get();
  }

  @NonNull
  @Override
  public Optional<AuthProvider> getAuthProvider() {
    return authProviderRef.get();
  }

  @NonNull
  @Override
  public Optional<SslEngineFactory> getSslEngineFactory() {
    return sslEngineFactoryRef.get();
  }

  @NonNull
  @Override
  public EventBus getEventBus() {
    return eventBusRef.get();
  }

  @NonNull
  @Override
  public Compressor<ByteBuf> getCompressor() {
    return compressorRef.get();
  }

  @NonNull
  @Override
  public PrimitiveCodec<ByteBuf> getPrimitiveCodec() {
    return primitiveCodecRef.get();
  }

  @NonNull
  @Override
  public FrameCodec<ByteBuf> getFrameCodec() {
    return frameCodecRef.get();
  }

  @NonNull
  @Override
  public SegmentCodec<ByteBuf> getSegmentCodec() {
    return segmentCodecRef.get();
  }

  @NonNull
  @Override
  public ProtocolVersionRegistry getProtocolVersionRegistry() {
    return protocolVersionRegistryRef.get();
  }

  @NonNull
  @Override
  public ConsistencyLevelRegistry getConsistencyLevelRegistry() {
    return consistencyLevelRegistryRef.get();
  }

  @NonNull
  @Override
  public WriteTypeRegistry getWriteTypeRegistry() {
    return writeTypeRegistryRef.get();
  }

  @NonNull
  @Override
  public NettyOptions getNettyOptions() {
    return nettyOptionsRef.get();
  }

  @NonNull
  @Override
  public WriteCoalescer getWriteCoalescer() {
    return writeCoalescerRef.get();
  }

  @NonNull
  @Override
  public Optional<SslHandlerFactory> getSslHandlerFactory() {
    return sslHandlerFactoryRef.get();
  }

  @NonNull
  @Override
  public ChannelFactory getChannelFactory() {
    return channelFactoryRef.get();
  }

  @NonNull
  @Override
  public ChannelPoolFactory getChannelPoolFactory() {
    return channelPoolFactory;
  }

  @NonNull
  @Override
  public TopologyMonitor getTopologyMonitor() {
    return topologyMonitorRef.get();
  }

  @NonNull
  @Override
  public MetadataManager getMetadataManager() {
    return metadataManagerRef.get();
  }

  @NonNull
  @Override
  public LoadBalancingPolicyWrapper getLoadBalancingPolicyWrapper() {
    return loadBalancingPolicyWrapperRef.get();
  }

  @NonNull
  @Override
  public ControlConnection getControlConnection() {
    return controlConnectionRef.get();
  }

  @NonNull
  @Override
  public RequestProcessorRegistry getRequestProcessorRegistry() {
    return requestProcessorRegistryRef.get();
  }

  @NonNull
  @Override
  public SchemaQueriesFactory getSchemaQueriesFactory() {
    return schemaQueriesFactoryRef.get();
  }

  @NonNull
  @Override
  public SchemaParserFactory getSchemaParserFactory() {
    return schemaParserFactoryRef.get();
  }

  @NonNull
  @Override
  public TokenFactoryRegistry getTokenFactoryRegistry() {
    return tokenFactoryRegistryRef.get();
  }

  @NonNull
  @Override
  public ReplicationStrategyFactory getReplicationStrategyFactory() {
    return replicationStrategyFactoryRef.get();
  }

  @NonNull
  @Override
  public PoolManager getPoolManager() {
    return poolManagerRef.get();
  }

  @NonNull
  @Override
  public MetricsFactory getMetricsFactory() {
    return metricsFactoryRef.get();
  }

  @NonNull
  @Override
  public MetricIdGenerator getMetricIdGenerator() {
    return metricIdGeneratorRef.get();
  }

  @NonNull
  @Override
  public RequestThrottler getRequestThrottler() {
    return requestThrottlerRef.get();
  }

  @NonNull
  @Override
  public NodeStateListener getNodeStateListener() {
    return nodeStateListenerRef.get();
  }

  @NonNull
  @Override
  public SchemaChangeListener getSchemaChangeListener() {
    return schemaChangeListenerRef.get();
  }

  @NonNull
  @Override
  public RequestTracker getRequestTracker() {
    return requestTrackerRef.get();
  }

  @Nullable
  @Override
  public String getLocalDatacenter(@NonNull String profileName) {
    return localDatacentersFromBuilder.get(profileName);
  }

  @Nullable
  @Override
  @Deprecated
  public Predicate<Node> getNodeFilter(@NonNull String profileName) {
    return nodeFiltersFromBuilder.get(profileName);
  }

  @Nullable
  @Override
  public NodeDistanceEvaluator getNodeDistanceEvaluator(@NonNull String profileName) {
    return nodeDistanceEvaluatorsFromBuilder.get(profileName);
  }

  @Nullable
  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }

  @NonNull
  @Override
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  @NonNull
  @Override
  public ProtocolVersion getProtocolVersion() {
    return getChannelFactory().getProtocolVersion();
  }

  @NonNull
  @Override
  public Map<String, String> getStartupOptions() {
    return startupOptionsRef.get();
  }

  protected RequestLogFormatter buildRequestLogFormatter() {
    return new RequestLogFormatter(this);
  }

  @NonNull
  @Override
  public RequestLogFormatter getRequestLogFormatter() {
    return requestLogFormatterRef.get();
  }

  @NonNull
  @Override
  public List<LifecycleListener> getLifecycleListeners() {
    return lifecycleListenersRef.get();
  }

  @Nullable
  @Override
  public Object getMetricRegistry() {
    return metricRegistry;
  }
}
