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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.Debouncer;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains the state of the Cassandra nodes, based on the events received from other components of
 * the driver.
 *
 * <p>See {@link NodeState} and {@link TopologyEvent} for a description of the state change rules.
 */
@ThreadSafe
public class NodeStateManager implements AsyncAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(NodeStateManager.class);

  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;
  private final String logPrefix;

  public NodeStateManager(InternalDriverContext context) {
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context);
    this.logPrefix = context.getSessionName();
  }

  /**
   * Indicates when the driver initialization is complete (that is, we have performed the first node
   * list refresh and are about to initialize the load balancing policy).
   */
  public void markInitialized() {
    RunOrSchedule.on(adminExecutor, singleThreaded::markInitialized);
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
    return closeAsync();
  }

  private class SingleThreaded {

    private final MetadataManager metadataManager;
    private final EventBus eventBus;
    private final Debouncer<TopologyEvent, Collection<TopologyEvent>> topologyEventDebouncer;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean isInitialized = false;
    private boolean closeWasCalled;

    private SingleThreaded(InternalDriverContext context) {
      this.metadataManager = context.getMetadataManager();

      DriverExecutionProfile config = context.getConfig().getDefaultProfile();
      this.topologyEventDebouncer =
          new Debouncer<>(
              logPrefix + "|topology debouncer",
              adminExecutor,
              this::coalesceTopologyEvents,
              this::flushTopologyEvents,
              config.getDuration(DefaultDriverOption.METADATA_TOPOLOGY_WINDOW),
              config.getInt(DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS));

      this.eventBus = context.getEventBus();
      this.eventBus.register(
          ChannelEvent.class, RunOrSchedule.on(adminExecutor, this::onChannelEvent));
      this.eventBus.register(
          TopologyEvent.class, RunOrSchedule.on(adminExecutor, this::onTopologyEvent));
      // Note: this component exists for the whole life of the driver instance, so don't worry about
      // unregistering the listeners.
    }

    private void markInitialized() {
      assert adminExecutor.inEventLoop();
      isInitialized = true;
    }

    // Updates to DefaultNode's volatile fields are confined to the admin thread
    @SuppressWarnings({"NonAtomicVolatileUpdate", "NonAtomicOperationOnVolatileField"})
    private void onChannelEvent(ChannelEvent event) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      LOG.debug("[{}] Processing {}", logPrefix, event);
      DefaultNode node = (DefaultNode) event.node;
      assert node != null;
      switch (event.type) {
        case OPENED:
          node.openConnections += 1;
          if (node.state == NodeState.DOWN || node.state == NodeState.UNKNOWN) {
            setState(node, NodeState.UP, "a new connection was opened to it");
          }
          break;
        case CLOSED:
          node.openConnections -= 1;
          if (node.openConnections == 0 && node.reconnections > 0) {
            setState(node, NodeState.DOWN, "it was reconnecting and lost its last connection");
          }
          break;
        case RECONNECTION_STARTED:
          node.reconnections += 1;
          if (node.openConnections == 0) {
            setState(node, NodeState.DOWN, "it has no connections and started reconnecting");
          }
          break;
        case RECONNECTION_STOPPED:
          node.reconnections -= 1;
          break;
        case CONTROL_CONNECTION_FAILED:
          // Special case for init, where this means that a contact point is down. In other
          // situations that information is not really useful, we rely on
          // openConnections/reconnections instead.
          if (!isInitialized) {
            setState(node, NodeState.DOWN, "it was tried as a contact point but failed");
          }
          break;
      }
    }

    private void onDebouncedTopologyEvent(TopologyEvent event) {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      LOG.debug("[{}] Processing {}", logPrefix, event);
      Optional<Node> maybeNode = metadataManager.getMetadata().findNode(event.broadcastRpcAddress);
      switch (event.type) {
        case SUGGEST_UP:
          if (maybeNode.isPresent()) {
            DefaultNode node = (DefaultNode) maybeNode.get();
            if (node.state == NodeState.FORCED_DOWN) {
              LOG.debug("[{}] Not setting {} UP because it is FORCED_DOWN", logPrefix, node);
            } else if (node.distance == NodeDistance.IGNORED) {
              setState(node, NodeState.UP, "it is IGNORED and an UP topology event was received");
            }
          } else {
            LOG.debug(
                "[{}] Received UP event for unknown node {}, refreshing node list",
                logPrefix,
                event.broadcastRpcAddress);
            metadataManager.refreshNodes();
          }
          break;
        case SUGGEST_DOWN:
          if (maybeNode.isPresent()) {
            DefaultNode node = (DefaultNode) maybeNode.get();
            if (node.openConnections > 0) {
              LOG.debug(
                  "[{}] Not setting {} DOWN because it still has active connections",
                  logPrefix,
                  node);
            } else if (node.state == NodeState.FORCED_DOWN) {
              LOG.debug("[{}] Not setting {} DOWN because it is FORCED_DOWN", logPrefix, node);
            } else if (node.distance == NodeDistance.IGNORED) {
              setState(
                  node, NodeState.DOWN, "it is IGNORED and a DOWN topology event was received");
            }
          } else {
            LOG.debug(
                "[{}] Received DOWN event for unknown node {}, ignoring it",
                logPrefix,
                event.broadcastRpcAddress);
          }
          break;
        case FORCE_UP:
          if (maybeNode.isPresent()) {
            DefaultNode node = (DefaultNode) maybeNode.get();
            setState(node, NodeState.UP, "a FORCE_UP topology event was received");
          } else {
            LOG.debug(
                "[{}] Received FORCE_UP event for unknown node {}, adding it",
                logPrefix,
                event.broadcastRpcAddress);
            metadataManager.addNode(event.broadcastRpcAddress);
          }
          break;
        case FORCE_DOWN:
          if (maybeNode.isPresent()) {
            DefaultNode node = (DefaultNode) maybeNode.get();
            setState(node, NodeState.FORCED_DOWN, "a FORCE_DOWN topology event was received");
          } else {
            LOG.debug(
                "[{}] Received FORCE_DOWN event for unknown node {}, ignoring it",
                logPrefix,
                event.broadcastRpcAddress);
          }
          break;
        case SUGGEST_ADDED:
          if (maybeNode.isPresent()) {
            DefaultNode node = (DefaultNode) maybeNode.get();
            LOG.debug(
                "[{}] Received ADDED event for {} but it is already in our metadata, ignoring",
                logPrefix,
                node);
          } else {
            metadataManager.addNode(event.broadcastRpcAddress);
          }
          break;
        case SUGGEST_REMOVED:
          if (maybeNode.isPresent()) {
            metadataManager.removeNode(event.broadcastRpcAddress);
          } else {
            LOG.debug(
                "[{}] Received REMOVED event for {} but it is not in our metadata, ignoring",
                logPrefix,
                event.broadcastRpcAddress);
          }
          break;
      }
    }

    // Called by the event bus, needs debouncing
    private void onTopologyEvent(TopologyEvent event) {
      assert adminExecutor.inEventLoop();
      topologyEventDebouncer.receive(event);
    }

    // Called to process debounced events before flushing
    private Collection<TopologyEvent> coalesceTopologyEvents(List<TopologyEvent> events) {
      assert adminExecutor.inEventLoop();
      Collection<TopologyEvent> result;
      if (events.size() == 1) {
        result = events;
      } else {
        // Keep the last FORCE* event for each node, or if there is none the last normal event
        Map<InetSocketAddress, TopologyEvent> last = Maps.newHashMapWithExpectedSize(events.size());
        for (TopologyEvent event : events) {
          if (event.isForceEvent()
              || !last.containsKey(event.broadcastRpcAddress)
              || !last.get(event.broadcastRpcAddress).isForceEvent()) {
            last.put(event.broadcastRpcAddress, event);
          }
        }
        result = last.values();
      }
      LOG.debug("[{}] Coalesced topology events: {} => {}", logPrefix, events, result);
      return result;
    }

    // Called when the debouncer flushes
    private void flushTopologyEvents(Collection<TopologyEvent> events) {
      assert adminExecutor.inEventLoop();
      for (TopologyEvent event : events) {
        onDebouncedTopologyEvent(event);
      }
    }

    private void close() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      topologyEventDebouncer.stop();
      closeFuture.complete(null);
    }

    private void setState(DefaultNode node, NodeState newState, String reason) {
      NodeState oldState = node.state;
      if (oldState != newState) {
        LOG.debug(
            "[{}] Transitioning {} {}=>{} (because {})",
            logPrefix,
            node,
            oldState,
            newState,
            reason);
        node.state = newState;
        if (newState == NodeState.UP) {
          node.upSinceMillis = System.currentTimeMillis();
        } else {
          node.upSinceMillis = -1;
        }
        // Fire the state change event, either immediately, or after a refresh if the node just came
        // back up.
        // If oldState == UNKNOWN, the node was just added, we already refreshed while processing
        // the addition.
        if (oldState == NodeState.UNKNOWN || newState != NodeState.UP) {
          eventBus.fire(NodeStateEvent.changed(oldState, newState, node));
        } else {
          metadataManager
              .refreshNode(node)
              .whenComplete(
                  (success, error) -> {
                    try {
                      if (error != null) {
                        Loggers.warnWithException(
                            LOG, "[{}] Error while refreshing info for {}", logPrefix, node, error);
                      }
                      // Fire the event whether the refresh succeeded or not
                      eventBus.fire(NodeStateEvent.changed(oldState, newState, node));
                    } catch (Throwable t) {
                      Loggers.warnWithException(LOG, "[{}] Unexpected exception", logPrefix, t);
                    }
                  });
        }
      }
    }
  }
}
