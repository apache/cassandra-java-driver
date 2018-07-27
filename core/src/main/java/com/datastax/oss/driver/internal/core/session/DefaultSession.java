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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeStateManager;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The session implementation.
 *
 * <p>It maintains a {@link ChannelPool} to each node that the {@link LoadBalancingPolicy} set to a
 * non-ignored distance. It listens for distance events and node state events, in order to adjust
 * the pools accordingly.
 *
 * <p>It executes requests by:
 *
 * <ul>
 *   <li>picking the appropriate processor to convert the request into a protocol message.
 *   <li>getting a query plan from the load balancing policy
 *   <li>trying to send the message on each pool, in the order of the query plan
 * </ul>
 */
@ThreadSafe
public class DefaultSession implements CqlSession {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSession.class);

  public static CompletionStage<CqlSession> init(
      InternalDriverContext context, Set<InetSocketAddress> contactPoints, CqlIdentifier keyspace) {
    return new DefaultSession(context, contactPoints).init(keyspace);
  }

  private final InternalDriverContext context;
  private final EventExecutor adminExecutor;
  private final String logPrefix;
  private final SingleThreaded singleThreaded;
  private final MetadataManager metadataManager;
  private final RequestProcessorRegistry processorRegistry;
  private final PoolManager poolManager;
  private final SessionMetricUpdater metricUpdater;

  private DefaultSession(InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
    LOG.debug("Creating new session {}", context.getSessionName());
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.context = context;
    this.singleThreaded = new SingleThreaded(context, contactPoints);
    this.metadataManager = context.getMetadataManager();
    this.processorRegistry = context.getRequestProcessorRegistry();
    this.poolManager = context.getPoolManager();
    this.logPrefix = context.getSessionName();
    this.metricUpdater = context.getMetricsFactory().getSessionUpdater();
  }

  private CompletionStage<CqlSession> init(CqlIdentifier keyspace) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.init(keyspace));
    return singleThreaded.initFuture;
  }

  @NonNull
  @Override
  public String getName() {
    return context.getSessionName();
  }

  @NonNull
  @Override
  public Metadata getMetadata() {
    return metadataManager.getMetadata();
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    return metadataManager.isSchemaEnabled();
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(@Nullable Boolean newValue) {
    return metadataManager.setSchemaEnabled(newValue);
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return metadataManager.refreshSchema(null, true, true);
  }

  @NonNull
  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return context.getTopologyMonitor().checkSchemaAgreement();
  }

  @NonNull
  @Override
  public DriverContext getContext() {
    return context;
  }

  @NonNull
  @Override
  public Optional<CqlIdentifier> getKeyspace() {
    return Optional.ofNullable(poolManager.getKeyspace());
  }

  @NonNull
  @Override
  public Optional<? extends Metrics> getMetrics() {
    return context.getMetricsFactory().getMetrics();
  }

  /**
   * <b>INTERNAL USE ONLY</b> -- switches the session to a new keyspace.
   *
   * <p>This is called by the driver when a {@code USE} query is successfully executed through the
   * session. Calling it from anywhere else is highly discouraged, as an invalid keyspace would
   * wreak havoc (close all connections and make the session unusable).
   */
  @NonNull
  public CompletionStage<Void> setKeyspace(@NonNull CqlIdentifier newKeyspace) {
    return poolManager.setKeyspace(newKeyspace);
  }

  @NonNull
  public Map<Node, ChannelPool> getPools() {
    return poolManager.getPools();
  }

  @Nullable
  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      @NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
    return processorRegistry
        .processorFor(request, resultType)
        .newHandler(request, this, context, logPrefix)
        .handle();
  }

  @Nullable
  public DriverChannel getChannel(@NonNull Node node, @NonNull String logPrefix) {
    ChannelPool pool = poolManager.getPools().get(node);
    if (pool == null) {
      LOG.trace("[{}] No pool to {}, skipping", logPrefix, node);
      return null;
    } else {
      DriverChannel channel = pool.next();
      if (channel == null) {
        LOG.trace("[{}] Pool returned no channel for {}, skipping", logPrefix, node);
        return null;
      } else if (channel.closeFuture().isDone()) {
        LOG.trace("[{}] Pool returned closed connection to {}, skipping", logPrefix, node);
        return null;
      } else {
        return channel;
      }
    }
  }

  @NonNull
  public ConcurrentMap<ByteBuffer, RepreparePayload> getRepreparePayloads() {
    return poolManager.getRepreparePayloads();
  }

  @NonNull
  public SessionMetricUpdater getMetricUpdater() {
    return metricUpdater;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  private class SingleThreaded {

    private final InternalDriverContext context;
    private final Set<InetSocketAddress> initialContactPoints;
    private final NodeStateManager nodeStateManager;
    private final CompletableFuture<CqlSession> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private boolean forceCloseWasCalled;

    private SingleThreaded(InternalDriverContext context, Set<InetSocketAddress> contactPoints) {
      this.context = context;
      this.nodeStateManager = new NodeStateManager(context);
      this.initialContactPoints = contactPoints;
      new SchemaListenerNotifier(
          context.getSchemaChangeListener(), context.getEventBus(), adminExecutor);
      context
          .getEventBus()
          .register(
              NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onNodeStateChanged));
      CompletableFutures.propagateCancellation(
          this.initFuture, context.getTopologyMonitor().initFuture());
    }

    private void init(CqlIdentifier keyspace) {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;
      LOG.debug("[{}] Starting initialization", logPrefix);

      // Eagerly fetch user-facing policies right now, no need to start opening connections if
      // something is wrong in the configuration.
      try {
        context.getLoadBalancingPolicies();
        context.getRetryPolicies();
        context.getSpeculativeExecutionPolicies();
        context.getReconnectionPolicy();
        context.getAddressTranslator();
        context.getNodeStateListener();
        context.getSchemaChangeListener();
        context.getRequestTracker();
        context.getRequestThrottler();
        context.getAuthProvider();
        context.getSslHandlerFactory();
        context.getTimestampGenerator();
      } catch (Throwable error) {
        initFuture.completeExceptionally(error);
        RunOrSchedule.on(adminExecutor, this::closePolicies);
        return;
      }

      MetadataManager metadataManager = context.getMetadataManager();
      metadataManager
          // Store contact points in the metadata right away, the control connection will need them
          // if it has to initialize (if the set is empty, 127.0.0.1 is used as a default).
          .addContactPoints(initialContactPoints)
          .thenCompose(v -> context.getTopologyMonitor().init())
          .thenCompose(v -> metadataManager.refreshNodes())
          .thenAccept(v -> afterInitialNodeListRefresh(keyspace))
          .exceptionally(
              error -> {
                initFuture.completeExceptionally(error);
                RunOrSchedule.on(adminExecutor, this::close);
                return null;
              });
    }

    private void afterInitialNodeListRefresh(CqlIdentifier keyspace) {
      try {
        boolean protocolWasForced =
            context.getConfig().getDefaultProfile().isDefined(DefaultDriverOption.PROTOCOL_VERSION);
        boolean needSchemaRefresh = true;
        if (!protocolWasForced) {
          ProtocolVersion currentVersion = context.getProtocolVersion();
          ProtocolVersion bestVersion =
              context
                  .getProtocolVersionRegistry()
                  .highestCommon(metadataManager.getMetadata().getNodes().values());
          if (!currentVersion.equals(bestVersion)) {
            LOG.info(
                "[{}] Negotiated protocol version {} for the initial contact point, "
                    + "but other nodes only support {}, downgrading",
                logPrefix,
                currentVersion,
                bestVersion);
            context.getChannelFactory().setProtocolVersion(bestVersion);
            ControlConnection controlConnection = context.getControlConnection();
            // Might not have initialized yet if there is a custom TopologyMonitor
            if (controlConnection.isInit()) {
              controlConnection.reconnectNow();
              // Reconnection already triggers a full schema refresh
              needSchemaRefresh = false;
            }
          }
        }
        if (needSchemaRefresh) {
          metadataManager.refreshSchema(null, false, true);
        }
        metadataManager
            .firstSchemaRefreshFuture()
            .thenAccept(v -> afterInitialSchemaRefresh(keyspace));

      } catch (Throwable throwable) {
        initFuture.completeExceptionally(throwable);
      }
    }

    private void afterInitialSchemaRefresh(CqlIdentifier keyspace) {
      try {
        nodeStateManager.markInitialized();
        context.getLoadBalancingPolicyWrapper().init();
        context.getConfigLoader().onDriverInit(context);
        LOG.debug("[{}] Initialization complete, ready", logPrefix);
        poolManager
            .init(keyspace)
            .whenComplete(
                (v, error) -> {
                  if (error != null) {
                    initFuture.completeExceptionally(error);
                  } else {
                    initFuture.complete(DefaultSession.this);
                  }
                });
      } catch (Throwable throwable) {
        initFuture.completeExceptionally(throwable);
      }
    }

    private void onNodeStateChanged(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      if (event.newState == null) {
        context.getNodeStateListener().onRemove(event.node);
      } else if (event.oldState == null && event.newState == NodeState.UNKNOWN) {
        context.getNodeStateListener().onAdd(event.node);
      } else if (event.newState == NodeState.UP) {
        context.getNodeStateListener().onUp(event.node);
      } else if (event.newState == NodeState.DOWN || event.newState == NodeState.FORCED_DOWN) {
        context.getNodeStateListener().onDown(event.node);
      }
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Starting shutdown", logPrefix);

      closePolicies();

      List<CompletionStage<Void>> childrenCloseStages = new ArrayList<>();
      for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
        childrenCloseStages.add(closeable.closeAsync());
      }
      CompletableFutures.whenAllDone(
          childrenCloseStages, () -> onChildrenClosed(childrenCloseStages), adminExecutor);
    }

    private void forceClose() {
      assert adminExecutor.inEventLoop();
      if (forceCloseWasCalled) {
        return;
      }
      forceCloseWasCalled = true;
      LOG.debug(
          "[{}] Starting forced shutdown (was {}closed before)",
          logPrefix,
          (closeWasCalled ? "" : "not "));

      if (closeWasCalled) {
        // onChildrenClosed has already been scheduled
        for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
          closeable.forceCloseAsync();
        }
      } else {
        closePolicies();
        List<CompletionStage<Void>> childrenCloseStages = new ArrayList<>();
        for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
          childrenCloseStages.add(closeable.forceCloseAsync());
        }
        CompletableFutures.whenAllDone(
            childrenCloseStages, () -> onChildrenClosed(childrenCloseStages), adminExecutor);
      }
    }

    private void onChildrenClosed(List<CompletionStage<Void>> childrenCloseStages) {
      assert adminExecutor.inEventLoop();
      for (CompletionStage<Void> stage : childrenCloseStages) {
        warnIfFailed(stage);
      }
      context
          .getNettyOptions()
          .onClose()
          .addListener(
              f -> {
                if (!f.isSuccess()) {
                  closeFuture.completeExceptionally(f.cause());
                } else {
                  LOG.debug("[{}] Shutdown complete", logPrefix);
                  closeFuture.complete(null);
                }
              });
    }

    private void warnIfFailed(CompletionStage<Void> stage) {
      CompletableFuture<Void> future = stage.toCompletableFuture();
      assert future.isDone();
      if (future.isCompletedExceptionally()) {
        Loggers.warnWithException(
            LOG,
            "[{}] Unexpected error while closing",
            logPrefix,
            CompletableFutures.getFailed(future));
      }
    }

    private void closePolicies() {
      // This is a bit tricky: we might be closing the session because of an initialization error.
      // This error might have been triggered by a policy failing to initialize. If we try to access
      // the policy here to close it, it will fail again. So make sure we ignore that error and
      // proceed to close the other policies.
      List<AutoCloseable> policies = new ArrayList<>();
      for (Supplier<AutoCloseable> supplier :
          ImmutableList.<Supplier<AutoCloseable>>of(
              context::getReconnectionPolicy,
              context::getLoadBalancingPolicyWrapper,
              context::getAddressTranslator,
              context::getConfigLoader,
              context::getNodeStateListener,
              context::getSchemaChangeListener,
              context::getRequestTracker,
              context::getRequestThrottler,
              context::getTimestampGenerator)) {
        try {
          policies.add(supplier.get());
        } catch (Throwable t) {
          // Assume the policy had failed to initialize, and we don't need to close it => ignore
        }
      }
      try {
        context.getAuthProvider().ifPresent(policies::add);
      } catch (Throwable t) {
        // ignore
      }
      try {
        context.getSslHandlerFactory().ifPresent(policies::add);
      } catch (Throwable t) {
        // ignore
      }
      try {
        policies.addAll(context.getRetryPolicies().values());
      } catch (Throwable t) {
        // ignore
      }
      try {
        policies.addAll(context.getSpeculativeExecutionPolicies().values());
      } catch (Throwable t) {
        // ignore
      }

      // Finally we have a list of all the policies that initialized successfully, close them:
      for (AutoCloseable policy : policies) {
        try {
          policy.close();
        } catch (Throwable t) {
          Loggers.warnWithException(LOG, "[{}] Error while closing {}", logPrefix, policy, t);
        }
      }
    }

    private List<AsyncAutoCloseable> internalComponentsToClose() {
      ImmutableList.Builder<AsyncAutoCloseable> components =
          ImmutableList.<AsyncAutoCloseable>builder()
              .add(poolManager, nodeStateManager, metadataManager);

      // Same as closePolicies(): make sure we don't trigger errors by accessing context components
      // that had failed to initialize:
      try {
        components.add(context.getTopologyMonitor());
      } catch (Throwable t) {
        // ignore
      }
      try {
        components.add(context.getControlConnection());
      } catch (Throwable t) {
        // ignore
      }
      return components.build();
    }
  }
}
