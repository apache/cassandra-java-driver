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
package com.datastax.dse.driver.internal.core.context;

import com.datastax.dse.driver.api.core.session.DseProgrammaticArguments;
import com.datastax.dse.driver.internal.core.DseProtocolVersionRegistry;
import com.datastax.dse.driver.internal.core.InsightsClientLifecycleListener;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.cql.continuous.ContinuousCqlRequestSyncProcessor;
import com.datastax.dse.driver.internal.core.cql.continuous.reactive.ContinuousCqlRequestReactiveProcessor;
import com.datastax.dse.driver.internal.core.cql.reactive.CqlRequestReactiveProcessor;
import com.datastax.dse.driver.internal.core.graph.GraphRequestAsyncProcessor;
import com.datastax.dse.driver.internal.core.graph.GraphRequestSyncProcessor;
import com.datastax.dse.driver.internal.core.metrics.DseDropwizardMetricsFactory;
import com.datastax.dse.driver.internal.core.tracker.MultiplexingRequestTracker;
import com.datastax.dse.protocol.internal.DseProtocolV1ClientCodecs;
import com.datastax.dse.protocol.internal.DseProtocolV2ClientCodecs;
import com.datastax.dse.protocol.internal.ProtocolV4ClientCodecsForDse;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.internal.core.ProtocolVersionRegistry;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.LifecycleListener;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlPrepareSyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestAsyncProcessor;
import com.datastax.oss.driver.internal.core.cql.CqlRequestSyncProcessor;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.protocol.ByteBufPrimitiveCodec;
import com.datastax.oss.driver.internal.core.session.RequestProcessor;
import com.datastax.oss.driver.internal.core.session.RequestProcessorRegistry;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolV3ClientCodecs;
import com.datastax.oss.protocol.internal.ProtocolV5ClientCodecs;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Extends the default driver context to plug-in DSE-specific implementations. */
@ThreadSafe
public class DseDriverContext extends DefaultDriverContext {

  private static final Logger LOG = LoggerFactory.getLogger(DseDriverContext.class);

  private final UUID startupClientId;
  private final String startupApplicationName;
  private final String startupApplicationVersion;
  private final List<LifecycleListener> listeners;

  public DseDriverContext(
      DriverConfigLoader configLoader,
      ProgrammaticArguments programmaticArguments,
      DseProgrammaticArguments dseProgrammaticArguments) {
    super(configLoader, programmaticArguments);
    this.startupClientId = dseProgrammaticArguments.getStartupClientId();
    this.startupApplicationName = dseProgrammaticArguments.getStartupApplicationName();
    this.startupApplicationVersion = dseProgrammaticArguments.getStartupApplicationVersion();
    StackTraceElement[] stackTrace = {};
    try {
      stackTrace = Thread.currentThread().getStackTrace();
    } catch (Exception ex) {
      // ignore and use empty
    }
    this.listeners =
        Collections.singletonList(new InsightsClientLifecycleListener(this, stackTrace));
  }
  /**
   * @deprecated this constructor only exists for backward compatibility. Please use {@link
   *     #DseDriverContext(DriverConfigLoader, ProgrammaticArguments, DseProgrammaticArguments)}
   *     instead.
   */
  public DseDriverContext(
      DriverConfigLoader configLoader,
      List<TypeCodec<?>> typeCodecs,
      NodeStateListener nodeStateListener,
      SchemaChangeListener schemaChangeListener,
      RequestTracker requestTracker,
      Map<String, String> localDatacenters,
      Map<String, Predicate<Node>> nodeFilters,
      ClassLoader classLoader,
      UUID clientId,
      String applicationName,
      String applicationVersion) {
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
            .build(),
        DseProgrammaticArguments.builder()
            .withStartupClientId(clientId)
            .withStartupApplicationName(applicationName)
            .withStartupApplicationVersion(applicationVersion)
            .build());
  }

  @Override
  protected ProtocolVersionRegistry buildProtocolVersionRegistry() {
    return new DseProtocolVersionRegistry(getSessionName());
  }

  @Override
  protected FrameCodec<ByteBuf> buildFrameCodec() {
    return new FrameCodec<>(
        new ByteBufPrimitiveCodec(getNettyOptions().allocator()),
        getCompressor(),
        new ProtocolV3ClientCodecs(),
        new ProtocolV4ClientCodecsForDse(),
        new ProtocolV5ClientCodecs(),
        new DseProtocolV1ClientCodecs(),
        new DseProtocolV2ClientCodecs());
  }

  @Override
  protected RequestProcessorRegistry buildRequestProcessorRegistry() {
    String logPrefix = getSessionName();

    List<RequestProcessor<?, ?>> processors = new ArrayList<>();

    // regular requests (sync and async)
    CqlRequestAsyncProcessor cqlRequestAsyncProcessor = new CqlRequestAsyncProcessor();
    CqlRequestSyncProcessor cqlRequestSyncProcessor =
        new CqlRequestSyncProcessor(cqlRequestAsyncProcessor);
    processors.add(cqlRequestAsyncProcessor);
    processors.add(cqlRequestSyncProcessor);

    // prepare requests (sync and async)
    CqlPrepareAsyncProcessor cqlPrepareAsyncProcessor = new CqlPrepareAsyncProcessor();
    CqlPrepareSyncProcessor cqlPrepareSyncProcessor =
        new CqlPrepareSyncProcessor(cqlPrepareAsyncProcessor);
    processors.add(cqlPrepareAsyncProcessor);
    processors.add(cqlPrepareSyncProcessor);

    // continuous requests (sync and async)
    ContinuousCqlRequestAsyncProcessor continuousCqlRequestAsyncProcessor =
        new ContinuousCqlRequestAsyncProcessor();
    ContinuousCqlRequestSyncProcessor continuousCqlRequestSyncProcessor =
        new ContinuousCqlRequestSyncProcessor(continuousCqlRequestAsyncProcessor);
    processors.add(continuousCqlRequestAsyncProcessor);
    processors.add(continuousCqlRequestSyncProcessor);

    // graph requests (sync and async)
    try {
      Class.forName("org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal");
      GraphRequestAsyncProcessor graphRequestAsyncProcessor = new GraphRequestAsyncProcessor();
      GraphRequestSyncProcessor graphRequestSyncProcessor =
          new GraphRequestSyncProcessor(graphRequestAsyncProcessor);
      processors.add(graphRequestAsyncProcessor);
      processors.add(graphRequestSyncProcessor);
    } catch (ClassNotFoundException | LinkageError error) {
      Loggers.warnWithException(
          LOG,
          "Could not register Graph extensions; Tinkerpop API might be missing from classpath",
          error);
    }

    // reactive requests (regular and continuous)
    try {
      Class.forName("org.reactivestreams.Publisher");
      CqlRequestReactiveProcessor cqlRequestReactiveProcessor =
          new CqlRequestReactiveProcessor(cqlRequestAsyncProcessor);
      ContinuousCqlRequestReactiveProcessor continuousCqlRequestReactiveProcessor =
          new ContinuousCqlRequestReactiveProcessor(continuousCqlRequestAsyncProcessor);
      processors.add(cqlRequestReactiveProcessor);
      processors.add(continuousCqlRequestReactiveProcessor);
    } catch (ClassNotFoundException | LinkageError error) {
      Loggers.warnWithException(
          LOG,
          "Could not register Reactive extensions; Reactive Streams API might be missing from classpath",
          error);
    }

    return new RequestProcessorRegistry(logPrefix, processors.toArray(new RequestProcessor[0]));
  }

  @Override
  protected MetricsFactory buildMetricsFactory() {
    return new DseDropwizardMetricsFactory(this);
  }

  @Override
  protected Map<String, String> buildStartupOptions() {
    return new DseStartupOptionsBuilder(this)
        .withClientId(startupClientId)
        .withApplicationName(startupApplicationName)
        .withApplicationVersion(startupApplicationVersion)
        .build();
  }

  @Override
  protected RequestTracker buildRequestTracker(RequestTracker requestTrackerFromBuilder) {
    RequestTracker requestTrackerFromConfig = super.buildRequestTracker(requestTrackerFromBuilder);
    if (requestTrackerFromConfig instanceof MultiplexingRequestTracker) {
      return requestTrackerFromConfig;
    } else {
      MultiplexingRequestTracker multiplexingRequestTracker = new MultiplexingRequestTracker();
      multiplexingRequestTracker.register(requestTrackerFromConfig);
      return multiplexingRequestTracker;
    }
  }

  @NonNull
  @Override
  public List<LifecycleListener> getLifecycleListeners() {
    return listeners;
  }
}
