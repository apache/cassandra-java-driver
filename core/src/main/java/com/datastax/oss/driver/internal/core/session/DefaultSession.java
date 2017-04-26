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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.InvalidKeyspaceException;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.ReplayingEventFilter;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.google.common.annotations.VisibleForTesting;
import io.netty.util.concurrent.EventExecutor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
public class DefaultSession implements CqlSession {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSession.class);

  public static CompletionStage<CqlSession> init(
      InternalDriverContext context, CqlIdentifier keyspace) {
    return new DefaultSession(context, keyspace).init();
  }

  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;

  @VisibleForTesting
  final ConcurrentMap<Node, ChannelPool> pools =
      new ConcurrentHashMap<>(
          16,
          0.75f,
          // the map will only be updated from adminExecutor
          1);

  private DefaultSession(InternalDriverContext context, CqlIdentifier keyspace) {
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context, keyspace);
  }

  private CompletionStage<CqlSession> init() {
    RunOrSchedule.on(adminExecutor, singleThreaded::init);
    return singleThreaded.initFuture;
  }

  @Override
  public <SyncResultT, AsyncResultT> SyncResultT execute(
      Request<SyncResultT, AsyncResultT> request) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public <SyncResultT, AsyncResultT> AsyncResultT executeAsync(
      Request<SyncResultT, AsyncResultT> request) {
    throw new UnsupportedOperationException("TODO");
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
    private final ChannelPoolFactory channelPoolFactory;
    private final CompletableFuture<CqlSession> initFuture = new CompletableFuture<>();
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
    // The pools that we have opened but have not finished initializing yet
    private final Map<Node, CompletionStage<ChannelPool>> pending = new HashMap<>();
    // If we receive events while a pool is initializing, the last one is stored here
    private final Map<Node, DistanceEvent> pendingDistanceEvents = new HashMap<>();
    private final Map<Node, NodeStateEvent> pendingStateEvents = new HashMap<>();

    private CqlIdentifier keyspace;

    private SingleThreaded(InternalDriverContext context, CqlIdentifier keyspace) {
      this.context = context;
      this.channelPoolFactory = context.channelPoolFactory();
      this.keyspace = keyspace;
      this.distanceListenerKey =
          context
              .eventBus()
              .register(
                  DistanceEvent.class, RunOrSchedule.on(adminExecutor, this::onDistanceEvent));
      this.stateListenerKey =
          context
              .eventBus()
              .register(NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onStateEvent));
    }

    private void init() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;

      LOG.debug("Initializing {}", DefaultSession.this);

      // Make sure we don't miss any event while the pools are initializing
      distanceEventFilter.start();
      stateEventFilter.start();

      Collection<Node> nodes = context.metadataManager().getMetadata().getNodes().values();
      List<CompletionStage<ChannelPool>> poolStages = new ArrayList<>(nodes.size());
      for (Node node : nodes) {
        NodeDistance distance = node.getDistance();
        if (distance == NodeDistance.IGNORED) {
          LOG.debug("Skipping {} because it is IGNORED", node);
        } else if (node.getState() == NodeState.FORCED_DOWN) {
          LOG.debug("Skipping {} because it is FORCED_DOWN", node);
        } else {
          poolStages.add(channelPoolFactory.init(node, keyspace, distance, context));
        }
      }
      CompletableFutures.whenAllDone(poolStages, () -> this.onPoolsInit(poolStages), adminExecutor);
    }

    private void onPoolsInit(List<CompletionStage<ChannelPool>> poolStages) {
      assert adminExecutor.inEventLoop();
      LOG.debug("{}: all pools have finished initializing", DefaultSession.this);
      // We will only propagate an invalid keyspace error if all pools get it
      boolean allInvalidKeyspaces = true;
      for (CompletionStage<ChannelPool> poolStage : poolStages) {
        // Note: pool init always succeeds
        ChannelPool pool = CompletableFutures.getCompleted(poolStage.toCompletableFuture());
        boolean invalidKeyspace = pool.isInvalidKeyspace();
        LOG.debug("Pool to {} -- invalid keyspace = {}", pool.getNode(), invalidKeyspace);
        allInvalidKeyspaces &= invalidKeyspace;
        pools.put(pool.getNode(), pool);
      }
      if (allInvalidKeyspaces) {
        initFuture.completeExceptionally(
            new InvalidKeyspaceException("Invalid keyspace " + keyspace.asPrettyCql()));
        forceClose();
      } else {
        initFuture.complete(DefaultSession.this);
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
      } else if (newDistance == NodeDistance.IGNORED && pools.containsKey(node)) {
        ChannelPool pool = pools.remove(node);
        if (pool != null) {
          LOG.debug("{} became IGNORED, destroying pool", node);
          pool.closeAsync()
              .exceptionally(
                  error -> {
                    LOG.warn("Error closing pool", error);
                    return null;
                  });
        }
      } else {
        NodeState state = node.getState();
        if (state == NodeState.FORCED_DOWN) {
          LOG.warn("{} became {} but it is FORCED_DOWN, ignoring", node, newDistance);
          return;
        }
        ChannelPool pool = pools.get(node);
        if (pool == null) {
          LOG.debug("{} became {} and no pool found, initializing it", node, newDistance);
          CompletionStage<ChannelPool> poolFuture =
              channelPoolFactory.init(node, keyspace, newDistance, context);
          pending.put(node, poolFuture);
          poolFuture
              .thenAcceptAsync(this::onPoolAdded, adminExecutor)
              .exceptionally(UncaughtExceptions::log);
        } else {
          LOG.debug("{} became {}, resizing it", node, newDistance);
          pool.resize(newDistance);
        }
      }
    }

    private void processStateEvent(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      // no need to check closeWasCalled, because we stop listening for events one closed
      DefaultNode node = event.node;
      NodeState newState = event.newState;
      if (pending.containsKey(node)) {
        pendingStateEvents.put(node, event);
      } else if (newState == NodeState.FORCED_DOWN) {
        ChannelPool pool = pools.remove(node);
        if (pool != null) {
          LOG.debug("{} became FORCED_DOWN, destroying pool", node);
          pool.closeAsync()
              .exceptionally(
                  error -> {
                    LOG.warn("Error closing pool", error);
                    return null;
                  });
        }
      } else if (newState == NodeState.UP) {
        ChannelPool pool = pools.get(node);
        if (pool == null) {
          LOG.debug("{} came back UP and no pool found, initializing it");
          CompletionStage<ChannelPool> poolFuture =
              channelPoolFactory.init(node, keyspace, node.getDistance(), context);
          pending.put(node, poolFuture);
          poolFuture
              .thenAcceptAsync(this::onPoolAdded, adminExecutor)
              .exceptionally(UncaughtExceptions::log);
        } else {
          LOG.debug("{} came back UP, triggering pool reconnection", node);
          pool.reconnectNow();
        }
      }
    }

    private void onPoolAdded(ChannelPool pool) {
      assert adminExecutor.inEventLoop();
      Node node = pool.getNode();
      if (closeWasCalled) {
        LOG.debug("Session was closed while a pool to {} was initializing, closing it", node);
        pool.forceCloseAsync();
      } else {
        LOG.debug("New pool to {} initialized", node);
        pending.remove(node);
        pools.put(node, pool);
        DistanceEvent distanceEvent = pendingDistanceEvents.remove(node);
        NodeStateEvent stateEvent = pendingStateEvents.remove(node);
        if (stateEvent != null && stateEvent.newState == NodeState.FORCED_DOWN) {
          LOG.debug("Received {} while the pool was initializing, processing it now", stateEvent);
          processStateEvent(stateEvent);
        } else if (distanceEvent != null) {
          LOG.debug(
              "Received {} while the pool was initializing, processing it now", distanceEvent);
          processDistanceEvent(distanceEvent);
        }
      }
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;

      // Stop listening for events
      context.eventBus().unregister(distanceListenerKey, DistanceEvent.class);
      context.eventBus().unregister(stateListenerKey, NodeStateEvent.class);

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
            LOG.error(
                "Error closing multiple pools in the same session, logging because only "
                    + "the first one is included in the session's failed future",
                error);
          }
        }
      }
      if (firstError != null) {
        closeFuture.completeExceptionally(new DriverException("Error closing pool(s)", firstError));
      } else {
        closeFuture.complete(null);
      }
    }
  }
}
