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
package com.datastax.oss.driver.internal.core;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeStateManager;
import com.datastax.oss.driver.internal.core.session.DefaultSession;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.google.common.collect.ImmutableList;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCluster implements Cluster<CqlSession> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCluster.class);

  public static CompletableFuture<Cluster<CqlSession>> init(
      InternalDriverContext context,
      Set<InetSocketAddress> contactPoints,
      Set<NodeStateListener> nodeStateListeners) {
    DefaultCluster cluster = new DefaultCluster(context, contactPoints, nodeStateListeners);
    return cluster.init();
  }

  private final InternalDriverContext context;
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;
  private final MetadataManager metadataManager;
  private final String logPrefix;

  private DefaultCluster(
      InternalDriverContext context,
      Set<InetSocketAddress> contactPoints,
      Set<NodeStateListener> nodeStateListeners) {
    LOG.debug("Creating new cluster {}", context.clusterName());
    this.context = context;
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context, contactPoints, nodeStateListeners);
    this.metadataManager = context.metadataManager();
    this.logPrefix = context.clusterName();
  }

  private CompletableFuture<Cluster<CqlSession>> init() {
    RunOrSchedule.on(adminExecutor, singleThreaded::init);
    return singleThreaded.initFuture;
  }

  @Override
  public String getName() {
    return context.clusterName();
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
  public CompletionStage<CqlSession> connectAsync(CqlIdentifier keyspace) {
    CompletableFuture<CqlSession> connectFuture = new CompletableFuture<>();
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.connect(keyspace, connectFuture));
    return connectFuture;
  }

  @Override
  public Cluster<CqlSession> register(SchemaChangeListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.register(listener));
    return this;
  }

  @Override
  public Cluster<CqlSession> unregister(SchemaChangeListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.unregister(listener));
    return this;
  }

  @Override
  public Cluster<CqlSession> register(NodeStateListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.register(listener));
    return this;
  }

  @Override
  public Cluster<CqlSession> unregister(NodeStateListener listener) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.unregister(listener));
    return this;
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
    private final CompletableFuture<Cluster<CqlSession>> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private boolean forceCloseWasCalled;
    // Note: closed sessions are not removed from the list. If this creates a memory issue, there
    // is something really wrong in the client program
    private List<Session> sessions;
    private int sessionCounter;
    private Set<SchemaChangeListener> schemaChangeListeners = new HashSet<>();
    private Set<NodeStateListener> nodeStateListeners;

    private SingleThreaded(
        InternalDriverContext context,
        Set<InetSocketAddress> contactPoints,
        Set<NodeStateListener> nodeStateListeners) {
      this.context = context;
      this.nodeStateManager = new NodeStateManager(context);
      this.initialContactPoints = contactPoints;
      this.nodeStateListeners = nodeStateListeners;
      this.sessions = new ArrayList<>();
      new SchemaListenerNotifier(schemaChangeListeners, context.eventBus(), adminExecutor);
      context
          .eventBus()
          .register(
              NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onNodeStateChanged));
    }

    private void init() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;
      LOG.debug("[{}] Starting initialization", logPrefix);

      nodeStateListeners.forEach(l -> l.onRegister(DefaultCluster.this));

      // If any contact points were provided, store them in the metadata right away (the
      // control connection will need them if it has to initialize)
      MetadataManager metadataManager = context.metadataManager();
      metadataManager
          .addContactPoints(initialContactPoints)
          .thenCompose(v -> context.topologyMonitor().init())
          .thenCompose(v -> metadataManager.refreshNodes())
          .thenAccept(this::afterInitialNodeListRefresh)
          .exceptionally(
              error -> {
                close();
                initFuture.completeExceptionally(error);
                return null;
              });
    }

    private void afterInitialNodeListRefresh(@SuppressWarnings("unused") Void ignored) {
      try {
        boolean protocolWasForced =
            context.config().getDefaultProfile().isDefined(CoreDriverOption.PROTOCOL_VERSION);
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
        metadataManager.firstSchemaRefreshFuture().thenAccept(this::afterInitialSchemaRefresh);

      } catch (Throwable throwable) {
        initFuture.completeExceptionally(throwable);
      }
    }

    private void afterInitialSchemaRefresh(@SuppressWarnings("unused") Void ignored) {
      try {
        nodeStateManager.markInitialized();
        context.loadBalancingPolicyWrapper().init();
        context.configLoader().onDriverInit(context);
        LOG.debug("[{}] Initialization complete, ready", logPrefix);
        initFuture.complete(DefaultCluster.this);
      } catch (Throwable throwable) {
        initFuture.completeExceptionally(throwable);
      }
    }

    private void connect(CqlIdentifier keyspace, CompletableFuture<CqlSession> connectFuture) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        connectFuture.completeExceptionally(new IllegalStateException("Cluster was closed"));
      } else {
        String sessionLogPrefix = logPrefix + "|s" + sessionCounter++;
        LOG.debug(
            "[{}] Opening new session {} to keyspace {}", logPrefix, sessionLogPrefix, keyspace);
        DefaultSession.init(context, keyspace, sessionLogPrefix)
            .whenCompleteAsync(
                (session, error) -> {
                  if (error != null) {
                    connectFuture.completeExceptionally(error);
                  } else if (closeWasCalled) {
                    connectFuture.completeExceptionally(
                        new IllegalStateException(
                            "Cluster was closed while session was initializing"));
                    session.forceCloseAsync();
                  } else {
                    sessions.add(session);
                    connectFuture.complete(session);
                  }
                },
                adminExecutor);
      }
    }

    private void register(SchemaChangeListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      // We want onRegister to be called before any event. We can add the listener before, because
      // schema events are processed on this same thread.
      if (schemaChangeListeners.add(listener)) {
        listener.onRegister(DefaultCluster.this);
      }
    }

    private void unregister(SchemaChangeListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      if (schemaChangeListeners.remove(listener)) {
        listener.onUnregister(DefaultCluster.this);
      }
    }

    private void register(NodeStateListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      if (nodeStateListeners.add(listener)) {
        listener.onRegister(DefaultCluster.this);
      }
    }

    private void unregister(NodeStateListener listener) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      if (nodeStateListeners.remove(listener)) {
        listener.onUnregister(DefaultCluster.this);
      }
    }

    private void onNodeStateChanged(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      if (event.newState == null) {
        nodeStateListeners.forEach(listener -> listener.onRemove(event.node));
      } else if (event.oldState == null && event.newState == NodeState.UNKNOWN) {
        nodeStateListeners.forEach(listener -> listener.onAdd(event.node));
      } else if (event.newState == NodeState.UP) {
        nodeStateListeners.forEach(listener -> listener.onUp(event.node));
      } else if (event.newState == NodeState.DOWN || event.newState == NodeState.FORCED_DOWN) {
        nodeStateListeners.forEach(listener -> listener.onDown(event.node));
      }
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;

      LOG.debug("[{}] Starting shutdown", logPrefix);
      for (SchemaChangeListener listener : schemaChangeListeners) {
        listener.onUnregister(DefaultCluster.this);
      }
      schemaChangeListeners.clear();
      for (NodeStateListener listener : nodeStateListeners) {
        listener.onUnregister(DefaultCluster.this);
      }
      nodeStateListeners.clear();
      List<CompletionStage<Void>> childrenCloseStages = new ArrayList<>();
      closePolicies();
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
        // onChildrenClosed has already been called
        for (AsyncAutoCloseable closeable : internalComponentsToClose()) {
          closeable.forceCloseAsync();
        }
      } else {
        closeWasCalled = true;
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
          ImmutableList.of(
              context.reconnectionPolicy(),
              context.retryPolicy(),
              context.loadBalancingPolicyWrapper(),
              context.speculativeExecutionPolicy(),
              context.addressTranslator(),
              context.configLoader())) {
        try {
          closeable.close();
        } catch (Throwable t) {
          Loggers.warnWithException(LOG, "[{}] Error while closing {}", logPrefix, closeable, t);
        }
      }
    }

    private List<AsyncAutoCloseable> internalComponentsToClose() {
      return ImmutableList.<AsyncAutoCloseable>builder()
          .addAll(sessions)
          .add(
              nodeStateManager,
              metadataManager,
              context.topologyMonitor(),
              context.controlConnection())
          .build();
    }
  }
}
