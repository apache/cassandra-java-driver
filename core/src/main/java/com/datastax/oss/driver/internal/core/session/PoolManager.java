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
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.driver.shaded.guava.common.collect.MapMaker;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the connection pools of a session.
 *
 * <p>Logically this belongs to {@link DefaultSession}, but it's extracted here in order to be
 * accessible from the context (notably for metrics).
 */
@ThreadSafe
public class PoolManager implements AsyncAutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(PoolManager.class);

  // This is read concurrently, but only updated from adminExecutor
  private volatile CqlIdentifier keyspace;

  private final ConcurrentMap<Node, ChannelPool> pools =
      new ConcurrentHashMap<>(
          16,
          0.75f,
          // the map will only be updated from adminExecutor
          1);

  // The raw data to reprepare requests on the fly, if we hit a node that doesn't have them in
  // its cache.
  // This is raw protocol-level data, as opposed to the actual instances returned to the client
  // (e.g. DefaultPreparedStatement) which are handled at the protocol level (e.g.
  // CqlPrepareAsyncProcessor). We keep the two separate to avoid introducing a dependency from the
  // session to a particular processor implementation.
  private final ConcurrentMap<ByteBuffer, RepreparePayload> repreparePayloads;

  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final DriverExecutionProfile config;
  private final SingleThreaded singleThreaded;

  public PoolManager(InternalDriverContext context) {
    this.logPrefix = context.getSessionName();
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.config = context.getConfig().getDefaultProfile();
    this.singleThreaded = new SingleThreaded(context);

    if (config.getBoolean(DefaultDriverOption.PREPARED_CACHE_WEAK_VALUES, true)) {
      LOG.debug("[{}] Prepared statements cache configured to use weak values", logPrefix);
      this.repreparePayloads = new MapMaker().weakValues().makeMap();
    } else {
      LOG.debug("[{}] Prepared statements cache configured to use strong values", logPrefix);
      this.repreparePayloads = new MapMaker().makeMap();
    }
  }

  public CompletionStage<Void> init(CqlIdentifier keyspace) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.init(keyspace));
    return singleThreaded.initFuture;
  }

  public CqlIdentifier getKeyspace() {
    return keyspace;
  }

  public CompletionStage<Void> setKeyspace(CqlIdentifier newKeyspace) {
    CqlIdentifier oldKeyspace = this.keyspace;
    if (Objects.equals(oldKeyspace, newKeyspace)) {
      return CompletableFuture.completedFuture(null);
    }
    if (config.getBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE)) {
      LOG.warn(
          "[{}] Detected a keyspace change at runtime ({} => {}). "
              + "This is an anti-pattern that should be avoided in production "
              + "(see '{}' in the configuration).",
          logPrefix,
          (oldKeyspace == null) ? "<none>" : oldKeyspace.asInternal(),
          newKeyspace.asInternal(),
          DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE.getPath());
    }
    this.keyspace = newKeyspace;
    CompletableFuture<Void> result = new CompletableFuture<>();
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.setKeyspace(newKeyspace, result));
    return result;
  }

  public Map<Node, ChannelPool> getPools() {
    return pools;
  }

  public ConcurrentMap<ByteBuffer, RepreparePayload> getRepreparePayloads() {
    return repreparePayloads;
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
    private final ChannelPoolFactory channelPoolFactory;
    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private boolean forceCloseWasCalled;
    private final Object distanceListenerKey;
    private final ReplayingEventFilter<DistanceEvent> distanceEventFilter =
        new ReplayingEventFilter<>(this::processDistanceEvent);
    private final Object stateListenerKey;
    private final ReplayingEventFilter<NodeStateEvent> stateEventFilter =
        new ReplayingEventFilter<>(this::processStateEvent);
    private final Object topologyListenerKey;
    // The pools that we have opened but have not finished initializing yet
    private final Map<Node, CompletionStage<ChannelPool>> pending = new HashMap<>();
    // If we receive events while a pool is initializing, the last one is stored here
    private final Map<Node, DistanceEvent> pendingDistanceEvents = new WeakHashMap<>();
    private final Map<Node, NodeStateEvent> pendingStateEvents = new WeakHashMap<>();

    private SingleThreaded(InternalDriverContext context) {
      this.context = context;
      this.channelPoolFactory = context.getChannelPoolFactory();
      this.distanceListenerKey =
          context
              .getEventBus()
              .register(
                  DistanceEvent.class, RunOrSchedule.on(adminExecutor, this::onDistanceEvent));
      this.stateListenerKey =
          context
              .getEventBus()
              .register(NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onStateEvent));
      this.topologyListenerKey =
          context
              .getEventBus()
              .register(
                  TopologyEvent.class, RunOrSchedule.on(adminExecutor, this::onTopologyEvent));
    }

    private void init(CqlIdentifier keyspace) {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;

      LOG.debug("[{}] Starting initialization", logPrefix);

      PoolManager.this.keyspace = keyspace;

      // Make sure we don't miss any event while the pools are initializing
      distanceEventFilter.start();
      stateEventFilter.start();

      Collection<Node> nodes = context.getMetadataManager().getMetadata().getNodes().values();
      List<CompletionStage<ChannelPool>> poolStages = new ArrayList<>(nodes.size());
      for (Node node : nodes) {
        NodeDistance distance = node.getDistance();
        if (distance == NodeDistance.IGNORED) {
          LOG.debug("[{}] Skipping {} because it is IGNORED", logPrefix, node);
        } else if (node.getState() == NodeState.FORCED_DOWN) {
          LOG.debug("[{}] Skipping {} because it is FORCED_DOWN", logPrefix, node);
        } else {
          LOG.debug("[{}] Creating a pool for {}", logPrefix, node);
          poolStages.add(channelPoolFactory.init(node, keyspace, distance, context, logPrefix));
        }
      }
      CompletableFutures.whenAllDone(poolStages, () -> this.onPoolsInit(poolStages), adminExecutor);
    }

    private void onPoolsInit(List<CompletionStage<ChannelPool>> poolStages) {
      assert adminExecutor.inEventLoop();
      LOG.debug("[{}] All pools have finished initializing", logPrefix);
      // We will only propagate an invalid keyspace error if all pools get it
      boolean allInvalidKeyspaces = poolStages.size() > 0;
      for (CompletionStage<ChannelPool> poolStage : poolStages) {
        // Note: pool init always succeeds
        ChannelPool pool = CompletableFutures.getCompleted(poolStage.toCompletableFuture());
        boolean invalidKeyspace = pool.isInvalidKeyspace();
        if (invalidKeyspace) {
          LOG.debug("[{}] Pool to {} reports an invalid keyspace", logPrefix, pool.getNode());
        }
        allInvalidKeyspaces &= invalidKeyspace;
        pools.put(pool.getNode(), pool);
      }
      if (allInvalidKeyspaces) {
        initFuture.completeExceptionally(
            new InvalidKeyspaceException("Invalid keyspace " + keyspace.asCql(true)));
        forceClose();
      } else {
        LOG.debug("[{}] Initialization complete, ready", logPrefix);
        initFuture.complete(null);
        distanceEventFilter.markReady();
        stateEventFilter.markReady();
      }
    }

    private void onDistanceEvent(DistanceEvent event) {
      assert adminExecutor.inEventLoop();
      distanceEventFilter.accept(event);
    }

    private void onStateEvent(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      stateEventFilter.accept(event);
    }

    private void processDistanceEvent(DistanceEvent event) {
      assert adminExecutor.inEventLoop();
      // no need to check closeWasCalled, because we stop listening for events one closed
      DefaultNode node = event.node;
      NodeDistance newDistance = event.distance;
      if (pending.containsKey(node)) {
        pendingDistanceEvents.put(node, event);
      } else if (newDistance == NodeDistance.IGNORED) {
        ChannelPool pool = pools.remove(node);
        if (pool != null) {
          LOG.debug("[{}] {} became IGNORED, destroying pool", logPrefix, node);
          pool.closeAsync()
              .exceptionally(
                  error -> {
                    Loggers.warnWithException(LOG, "[{}] Error closing pool", logPrefix, error);
                    return null;
                  });
        }
      } else {
        NodeState state = node.getState();
        if (state == NodeState.FORCED_DOWN) {
          LOG.warn(
              "[{}] {} became {} but it is FORCED_DOWN, ignoring", logPrefix, node, newDistance);
          return;
        }
        ChannelPool pool = pools.get(node);
        if (pool == null) {
          LOG.debug(
              "[{}] {} became {} and no pool found, initializing it", logPrefix, node, newDistance);
          CompletionStage<ChannelPool> poolFuture =
              channelPoolFactory.init(node, keyspace, newDistance, context, logPrefix);
          pending.put(node, poolFuture);
          poolFuture
              .thenAcceptAsync(this::onPoolInitialized, adminExecutor)
              .exceptionally(UncaughtExceptions::log);
        } else {
          LOG.debug("[{}] {} became {}, resizing it", logPrefix, node, newDistance);
          pool.resize(newDistance);
        }
      }
    }

    private void processStateEvent(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      // no need to check closeWasCalled, because we stop listening for events once closed
      DefaultNode node = event.node;
      NodeState oldState = event.oldState;
      NodeState newState = event.newState;
      if (pending.containsKey(node)) {
        pendingStateEvents.put(node, event);
      } else if (newState == null || newState == NodeState.FORCED_DOWN) {
        ChannelPool pool = pools.remove(node);
        if (pool != null) {
          LOG.debug(
              "[{}] {} was {}, destroying pool",
              logPrefix,
              node,
              newState == null ? "removed" : newState.name());
          pool.closeAsync()
              .exceptionally(
                  error -> {
                    Loggers.warnWithException(LOG, "[{}] Error closing pool", logPrefix, error);
                    return null;
                  });
        }
      } else if (oldState == NodeState.FORCED_DOWN
          && newState == NodeState.UP
          && node.getDistance() != NodeDistance.IGNORED) {
        LOG.debug("[{}] {} was forced back UP, initializing pool", logPrefix, node);
        createOrReconnectPool(node);
      }
    }

    private void onTopologyEvent(TopologyEvent event) {
      assert adminExecutor.inEventLoop();
      if (event.type == TopologyEvent.Type.SUGGEST_UP) {
        context
            .getMetadataManager()
            .getMetadata()
            .findNode(event.broadcastRpcAddress)
            .ifPresent(
                node -> {
                  if (node.getDistance() != NodeDistance.IGNORED) {
                    LOG.debug(
                        "[{}] Received a SUGGEST_UP event for {}, reconnecting pool now",
                        logPrefix,
                        node);
                    ChannelPool pool = pools.get(node);
                    if (pool != null) {
                      pool.reconnectNow();
                    }
                  }
                });
      }
    }

    private void createOrReconnectPool(Node node) {
      ChannelPool pool = pools.get(node);
      if (pool == null) {
        CompletionStage<ChannelPool> poolFuture =
            channelPoolFactory.init(node, keyspace, node.getDistance(), context, logPrefix);
        pending.put(node, poolFuture);
        poolFuture
            .thenAcceptAsync(this::onPoolInitialized, adminExecutor)
            .exceptionally(UncaughtExceptions::log);
      } else {
        pool.reconnectNow();
      }
    }

    private void onPoolInitialized(ChannelPool pool) {
      assert adminExecutor.inEventLoop();
      Node node = pool.getNode();
      if (closeWasCalled) {
        LOG.debug(
            "[{}] Session closed while a pool to {} was initializing, closing it", logPrefix, node);
        pool.forceCloseAsync();
      } else {
        LOG.debug("[{}] New pool to {} initialized", logPrefix, node);
        if (Objects.equals(keyspace, pool.getInitialKeyspaceName())) {
          reprepareStatements(pool);
        } else {
          // The keyspace changed while the pool was being initialized, switch it now.
          pool.setKeyspace(keyspace)
              .handleAsync(
                  (result, error) -> {
                    if (error != null) {
                      Loggers.warnWithException(
                          LOG, "Error while switching keyspace to " + keyspace, error);
                    }
                    reprepareStatements(pool);
                    return null;
                  },
                  adminExecutor);
        }
      }
    }

    private void reprepareStatements(ChannelPool pool) {
      assert adminExecutor.inEventLoop();
      if (config.getBoolean(DefaultDriverOption.REPREPARE_ENABLED)) {
        new ReprepareOnUp(
                logPrefix + "|" + pool.getNode().getEndPoint(),
                pool,
                adminExecutor,
                repreparePayloads,
                context,
                () -> RunOrSchedule.on(adminExecutor, () -> onPoolReady(pool)))
            .start();
      } else {
        LOG.debug("[{}] Reprepare on up is disabled, skipping", logPrefix);
        onPoolReady(pool);
      }
    }

    private void onPoolReady(ChannelPool pool) {
      assert adminExecutor.inEventLoop();
      Node node = pool.getNode();
      pending.remove(node);
      pools.put(node, pool);
      DistanceEvent distanceEvent = pendingDistanceEvents.remove(node);
      NodeStateEvent stateEvent = pendingStateEvents.remove(node);
      if (stateEvent != null && stateEvent.newState == NodeState.FORCED_DOWN) {
        LOG.debug(
            "[{}] Received {} while the pool was initializing, processing it now",
            logPrefix,
            stateEvent);
        processStateEvent(stateEvent);
      } else if (distanceEvent != null) {
        LOG.debug(
            "[{}] Received {} while the pool was initializing, processing it now",
            logPrefix,
            distanceEvent);
        processDistanceEvent(distanceEvent);
      }
    }

    private void setKeyspace(CqlIdentifier newKeyspace, CompletableFuture<Void> doneFuture) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        doneFuture.complete(null);
        return;
      }
      LOG.debug("[{}] Switching to keyspace {}", logPrefix, newKeyspace);
      List<CompletionStage<Void>> poolReadyFutures = Lists.newArrayListWithCapacity(pools.size());
      for (ChannelPool pool : pools.values()) {
        poolReadyFutures.add(pool.setKeyspace(newKeyspace));
      }
      CompletableFutures.completeFrom(CompletableFutures.allDone(poolReadyFutures), doneFuture);
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Starting shutdown", logPrefix);

      // Stop listening for events
      context.getEventBus().unregister(distanceListenerKey, DistanceEvent.class);
      context.getEventBus().unregister(stateListenerKey, NodeStateEvent.class);
      context.getEventBus().unregister(topologyListenerKey, TopologyEvent.class);

      List<CompletionStage<Void>> closePoolStages = new ArrayList<>(pools.size());
      for (ChannelPool pool : pools.values()) {
        closePoolStages.add(pool.closeAsync());
      }
      CompletableFutures.whenAllDone(
          closePoolStages, () -> onAllPoolsClosed(closePoolStages), adminExecutor);
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
        for (ChannelPool pool : pools.values()) {
          pool.forceCloseAsync();
        }
      } else {
        List<CompletionStage<Void>> closePoolStages = new ArrayList<>(pools.size());
        for (ChannelPool pool : pools.values()) {
          closePoolStages.add(pool.forceCloseAsync());
        }
        CompletableFutures.whenAllDone(
            closePoolStages, () -> onAllPoolsClosed(closePoolStages), adminExecutor);
      }
    }

    private void onAllPoolsClosed(List<CompletionStage<Void>> closePoolStages) {
      assert adminExecutor.inEventLoop();
      Throwable firstError = null;
      for (CompletionStage<Void> closePoolStage : closePoolStages) {
        CompletableFuture<Void> closePoolFuture = closePoolStage.toCompletableFuture();
        assert closePoolFuture.isDone();
        if (closePoolFuture.isCompletedExceptionally()) {
          Throwable error = CompletableFutures.getFailed(closePoolFuture);
          if (firstError == null) {
            firstError = error;
          } else {
            firstError.addSuppressed(error);
          }
        }
      }
      if (firstError != null) {
        closeFuture.completeExceptionally(firstError);
      } else {
        LOG.debug("[{}] Shutdown complete", logPrefix);
        closeFuture.complete(null);
      }
    }
  }
}
