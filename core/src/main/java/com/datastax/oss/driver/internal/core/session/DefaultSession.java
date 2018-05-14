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
    LOG.debug("Creating new session {}", context.sessionName());
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.context = context;
    this.singleThreaded = new SingleThreaded(context, contactPoints);
    this.metadataManager = context.metadataManager();
    this.processorRegistry = context.requestProcessorRegistry();
    this.poolManager = context.poolManager();
    this.logPrefix = context.sessionName();
    this.metricUpdater = context.metricsFactory().getSessionUpdater();
  }

  private CompletionStage<CqlSession> init(CqlIdentifier keyspace) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.init(keyspace));
    return singleThreaded.initFuture;
  }

  @Override
  public String getName() {
    return context.sessionName();
  }

  @Override
  public Metadata getMetadata() {
    return metadataManager.getMetadata();
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    return metadataManager.isSchemaEnabled();
  }

  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(Boolean newValue) {
    return metadataManager.setSchemaEnabled(newValue);
  }

  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return metadataManager.refreshSchema(null, true, true);
  }

  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return context.topologyMonitor().checkSchemaAgreement();
  }

  @Override
  public DriverContext getContext() {
    return context;
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return poolManager.getKeyspace();
  }

  @Override
  public Optional<? extends Metrics> getMetrics() {
    return context.metricsFactory().getMetrics();
  }

  /**
   * <b>INTERNAL USE ONLY</b> -- switches the session to a new keyspace.
   *
   * <p>This is called by the driver when a {@code USE} query is successfully executed through the
   * session. Calling it from anywhere else is highly discouraged, as an invalid keyspace would
   * wreak havoc (close all connections and make the session unusable).
   */
  public CompletionStage<Void> setKeyspace(CqlIdentifier newKeyspace) {
    return poolManager.setKeyspace(newKeyspace);
  }

  public Map<Node, ChannelPool> getPools() {
    return poolManager.getPools();
  }

  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      RequestT request, GenericType<ResultT> resultType) {
    return processorRegistry
        .processorFor(request, resultType)
        .newHandler(request, this, context, logPrefix)
        .handle();
  }

  public DriverChannel getChannel(Node node, String logPrefix) {
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

  public ConcurrentMap<ByteBuffer, RepreparePayload> getRepreparePayloads() {
    return poolManager.getRepreparePayloads();
  }

  public SessionMetricUpdater getMetricUpdater() {
    return metricUpdater;
  }

  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @Override
  public CompletionStage<Void> closeAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::close);
    return singleThreaded.closeFuture;
  }

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
      new SchemaListenerNotifier(context.schemaChangeListener(), context.eventBus(), adminExecutor);
      context
          .eventBus()
          .register(
              NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onNodeStateChanged));
    }

    private void init(CqlIdentifier keyspace) {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;
      LOG.debug("[{}] Starting initialization", logPrefix);

      MetadataManager metadataManager = context.metadataManager();
      metadataManager
          // Store contact points in the metadata right away, the control connection will need them
          // if it has to initialize (if the set is empty, 127.0.0.1 is used as a default).
          .addContactPoints(initialContactPoints)
          .thenCompose(v -> context.topologyMonitor().init())
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
            context.config().getDefaultProfile().isDefined(DefaultDriverOption.PROTOCOL_VERSION);
        boolean needSchemaRefresh = true;
        if (!protocolWasForced) {
          ProtocolVersion currentVersion = context.protocolVersion();
          ProtocolVersion bestVersion =
              context
                  .protocolVersionRegistry()
                  .highestCommon(metadataManager.getMetadata().getNodes().values());
          if (!currentVersion.equals(bestVersion)) {
            LOG.info(
                "[{}] Negotiated protocol version {} for the initial contact point, "
                    + "but other nodes only support {}, downgrading",
                logPrefix,
                currentVersion,
                bestVersion);
            context.channelFactory().setProtocolVersion(bestVersion);
            ControlConnection controlConnection = context.controlConnection();
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
        context.loadBalancingPolicyWrapper().init();
        context.configLoader().onDriverInit(context);
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
        context.nodeStateListener().onRemove(event.node);
      } else if (event.oldState == null && event.newState == NodeState.UNKNOWN) {
        context.nodeStateListener().onAdd(event.node);
      } else if (event.newState == NodeState.UP) {
        context.nodeStateListener().onUp(event.node);
      } else if (event.newState == NodeState.DOWN || event.newState == NodeState.FORCED_DOWN) {
        context.nodeStateListener().onDown(event.node);
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
          .nettyOptions()
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
      for (AutoCloseable closeable :
          ImmutableList.<AutoCloseable>builder()
              .add(
                  context.reconnectionPolicy(),
                  context.loadBalancingPolicyWrapper(),
                  context.addressTranslator(),
                  context.configLoader(),
                  context.nodeStateListener(),
                  context.schemaChangeListener(),
                  context.requestTracker())
              .addAll(context.retryPolicies().values())
              .addAll(context.speculativeExecutionPolicies().values())
              .build()) {
        try {
          closeable.close();
        } catch (Throwable t) {
          Loggers.warnWithException(LOG, "[{}] Error while closing {}", logPrefix, closeable, t);
        }
      }
    }

    private List<AsyncAutoCloseable> internalComponentsToClose() {
      return ImmutableList.<AsyncAutoCloseable>builder()
          .add(
              poolManager,
              nodeStateManager,
              metadataManager,
              context.topologyMonitor(),
              context.controlConnection())
          .build();
    }
  }
}
