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
package com.datastax.oss.driver.internal.core.control;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.EventCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.event.TopologyChangeEvent;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Maintains a dedicated connection to a Cassandra node for administrative queries: schema
 * refreshes, and cluster topology queries and events.
 *
 * <p>If the control node goes down, a reconnection is triggered. The control node is chosen
 * randomly among the contact points at startup, or according to the load balancing policy for later
 * reconnections.
 *
 * <p>If a custom {@link TopologyMonitor} is used, the control connection is used only for schema
 * refreshes; if schema metadata is also disabled, the control connection never initializes.
 */
@ThreadSafe
public class ControlConnection implements EventCallback, AsyncAutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ControlConnection.class);

  private final InternalDriverContext context;
  private final String logPrefix;
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;

  // The single channel used by this connection. This field is accessed concurrently, but only
  // mutated on adminExecutor (by SingleThreaded methods)
  private volatile DriverChannel channel;

  public ControlConnection(InternalDriverContext context) {
    this.context = context;
    this.logPrefix = context.getSessionName();
    this.adminExecutor = context.getNettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context);
  }

  /**
   * @param listenToClusterEvents whether to register for TOPOLOGY_CHANGE and STATUS_CHANGE events.
   *     If the control connection has already initialized with another value, this is ignored.
   *     SCHEMA_CHANGE events are always registered.
   * @param reconnectOnFailure whether to schedule a reconnection if the initial attempt fails (this
   *     does not affect the returned future, which always represent the outcome of the initial
   *     attempt only).
   */
  public CompletionStage<Void> init(boolean listenToClusterEvents, boolean reconnectOnFailure) {
    RunOrSchedule.on(
        adminExecutor, () -> singleThreaded.init(listenToClusterEvents, reconnectOnFailure));
    return singleThreaded.initFuture;
  }

  public CompletionStage<Void> initFuture() {
    return singleThreaded.initFuture;
  }

  public boolean isInit() {
    return singleThreaded.initFuture.isDone();
  }

  public CompletionStage<Void> firstConnectionAttemptFuture() {
    return singleThreaded.firstConnectionAttemptFuture;
  }

  /**
   * The channel currently used by this control connection. This is modified concurrently in the
   * event of a reconnection, so it may occasionally return a closed channel (clients should be
   * ready to deal with that).
   */
  public DriverChannel channel() {
    return channel;
  }

  /**
   * Forces an immediate reconnect: if we were connected to a node, that connection will be closed;
   * if we were already reconnecting, the next attempt is started immediately, without waiting for
   * the next scheduled interval; in all cases, a new query plan is fetched from the load balancing
   * policy, and each node in it will be tried in sequence.
   */
  public void reconnectNow() {
    RunOrSchedule.on(adminExecutor, singleThreaded::reconnectNow);
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return singleThreaded.closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    // Control queries are never critical, so there is no graceful close.
    return forceCloseAsync();
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  @Override
  public void onEvent(Message eventMessage) {
    if (!(eventMessage instanceof Event)) {
      LOG.warn("[{}] Unsupported event class: {}", logPrefix, eventMessage.getClass().getName());
    } else {
      LOG.debug("[{}] Processing incoming event {}", logPrefix, eventMessage);
      Event event = (Event) eventMessage;
      switch (event.type) {
        case ProtocolConstants.EventType.TOPOLOGY_CHANGE:
          processTopologyChange(event);
          break;
        case ProtocolConstants.EventType.STATUS_CHANGE:
          processStatusChange(event);
          break;
        case ProtocolConstants.EventType.SCHEMA_CHANGE:
          processSchemaChange(event);
          break;
        default:
          LOG.warn("[{}] Unsupported event type: {}", logPrefix, event.type);
      }
    }
  }

  private void processTopologyChange(Event event) {
    TopologyChangeEvent tce = (TopologyChangeEvent) event;
    InetSocketAddress address = context.getAddressTranslator().translate(tce.address);
    switch (tce.changeType) {
      case ProtocolConstants.TopologyChangeType.NEW_NODE:
        context.getEventBus().fire(TopologyEvent.suggestAdded(address));
        break;
      case ProtocolConstants.TopologyChangeType.REMOVED_NODE:
        context.getEventBus().fire(TopologyEvent.suggestRemoved(address));
        break;
      default:
        LOG.warn("[{}] Unsupported topology change type: {}", logPrefix, tce.changeType);
    }
  }

  private void processStatusChange(Event event) {
    StatusChangeEvent sce = (StatusChangeEvent) event;
    InetSocketAddress address = context.getAddressTranslator().translate(sce.address);
    switch (sce.changeType) {
      case ProtocolConstants.StatusChangeType.UP:
        context.getEventBus().fire(TopologyEvent.suggestUp(address));
        break;
      case ProtocolConstants.StatusChangeType.DOWN:
        context.getEventBus().fire(TopologyEvent.suggestDown(address));
        break;
      default:
        LOG.warn("[{}] Unsupported status change type: {}", logPrefix, sce.changeType);
    }
  }

  private void processSchemaChange(Event event) {
    SchemaChangeEvent sce = (SchemaChangeEvent) event;
    context.getMetadataManager().refreshSchema(sce.keyspace, false, false);
  }

  private class SingleThreaded {
    private final InternalDriverContext context;
    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();
    private final CompletableFuture<Void> firstConnectionAttemptFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private final Reconnection reconnection;
    private DriverChannelOptions channelOptions;
    // The last events received for each node
    private final Map<Node, DistanceEvent> lastDistanceEvents = new WeakHashMap<>();
    private final Map<Node, NodeStateEvent> lastStateEvents = new WeakHashMap<>();

    private SingleThreaded(InternalDriverContext context) {
      this.context = context;
      ReconnectionPolicy reconnectionPolicy = context.getReconnectionPolicy();
      this.reconnection =
          new Reconnection(
              logPrefix,
              adminExecutor,
              reconnectionPolicy::newControlConnectionSchedule,
              this::reconnect);
      // In "reconnect-on-init" mode, handle cancellation of the initFuture by user code
      CompletableFutures.whenCancelled(
          this.initFuture,
          () -> {
            LOG.debug("[{}] Init future was cancelled, stopping reconnection", logPrefix);
            reconnection.stop();
          });

      context
          .getEventBus()
          .register(DistanceEvent.class, RunOrSchedule.on(adminExecutor, this::onDistanceEvent));
      context
          .getEventBus()
          .register(NodeStateEvent.class, RunOrSchedule.on(adminExecutor, this::onStateEvent));
    }

    private void init(boolean listenToClusterEvents, boolean reconnectOnFailure) {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;
      try {
        ImmutableList<String> eventTypes = buildEventTypes(listenToClusterEvents);
        LOG.debug("[{}] Initializing with event types {}", logPrefix, eventTypes);
        channelOptions =
            DriverChannelOptions.builder()
                .withEvents(eventTypes, ControlConnection.this)
                .withOwnerLogPrefix(logPrefix + "|control")
                .build();

        Queue<Node> nodes = context.getLoadBalancingPolicyWrapper().newQueryPlan();

        connect(
            nodes,
            null,
            () -> {
              initFuture.complete(null);
              firstConnectionAttemptFuture.complete(null);
            },
            error -> {
              if (reconnectOnFailure && !closeWasCalled) {
                reconnection.start();
              } else {
                // Special case for the initial connection: reword to a more user-friendly error
                // message
                if (error instanceof AllNodesFailedException) {
                  error =
                      ((AllNodesFailedException) error)
                          .reword(
                              "Could not reach any contact point, "
                                  + "make sure you've provided valid addresses");
                }
                initFuture.completeExceptionally(error);
              }
              firstConnectionAttemptFuture.completeExceptionally(error);
            });
      } catch (Throwable t) {
        initFuture.completeExceptionally(t);
      }
    }

    private CompletionStage<Boolean> reconnect() {
      assert adminExecutor.inEventLoop();
      Queue<Node> nodes = context.getLoadBalancingPolicyWrapper().newQueryPlan();
      CompletableFuture<Boolean> result = new CompletableFuture<>();
      connect(
          nodes,
          null,
          () -> {
            result.complete(true);
            onSuccessfulReconnect();
          },
          error -> result.complete(false));
      return result;
    }

    private void connect(
        Queue<Node> nodes,
        Map<Node, Throwable> errors,
        Runnable onSuccess,
        Consumer<Throwable> onFailure) {
      assert adminExecutor.inEventLoop();
      Node node = nodes.poll();
      if (node == null) {
        onFailure.accept(AllNodesFailedException.fromErrors(errors));
      } else {
        LOG.debug("[{}] Trying to establish a connection to {}", logPrefix, node);
        context
            .getChannelFactory()
            .connect(node, channelOptions)
            .whenCompleteAsync(
                (channel, error) -> {
                  try {
                    DistanceEvent lastDistanceEvent = lastDistanceEvents.get(node);
                    NodeStateEvent lastStateEvent = lastStateEvents.get(node);
                    if (error != null) {
                      if (closeWasCalled || initFuture.isCancelled()) {
                        onSuccess.run(); // abort, we don't really care about the result
                      } else {
                        LOG.debug(
                            "[{}] Error connecting to {}, trying next node",
                            logPrefix,
                            node,
                            error);
                        Map<Node, Throwable> newErrors =
                            (errors == null) ? new LinkedHashMap<>() : errors;
                        newErrors.put(node, error);
                        context.getEventBus().fire(ChannelEvent.controlConnectionFailed(node));
                        connect(nodes, newErrors, onSuccess, onFailure);
                      }
                    } else if (closeWasCalled || initFuture.isCancelled()) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but the control connection was closed, closing it",
                          logPrefix,
                          channel);
                      channel.forceClose();
                      onSuccess.run();
                    } else if (lastDistanceEvent != null
                        && lastDistanceEvent.distance == NodeDistance.IGNORED) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but node became ignored, "
                              + "closing and trying next node",
                          logPrefix,
                          channel);
                      channel.forceClose();
                      connect(nodes, errors, onSuccess, onFailure);
                    } else if (lastStateEvent != null
                        && (lastStateEvent.newState == null /*(removed)*/
                            || lastStateEvent.newState == NodeState.FORCED_DOWN)) {
                      LOG.debug(
                          "[{}] New channel opened ({}) but node was removed or forced down, "
                              + "closing and trying next node",
                          logPrefix,
                          channel);
                      channel.forceClose();
                      connect(nodes, errors, onSuccess, onFailure);
                    } else {
                      LOG.debug("[{}] Connection established to {}", logPrefix, node);
                      // Make sure previous channel gets closed (it may still be open if
                      // reconnection was forced)
                      DriverChannel previousChannel = ControlConnection.this.channel;
                      if (previousChannel != null) {
                        previousChannel.forceClose();
                      }
                      ControlConnection.this.channel = channel;
                      context.getEventBus().fire(ChannelEvent.channelOpened(node));
                      channel
                          .closeFuture()
                          .addListener(
                              f ->
                                  adminExecutor
                                      .submit(() -> onChannelClosed(channel, node))
                                      .addListener(UncaughtExceptions::log));
                      onSuccess.run();
                    }
                  } catch (Exception e) {
                    Loggers.warnWithException(
                        LOG,
                        "[{}] Unexpected exception while processing channel init result",
                        logPrefix,
                        e);
                  }
                },
                adminExecutor);
      }
    }

    private void onSuccessfulReconnect() {
      // If reconnectOnFailure was true and we've never connected before, complete the future now,
      // otherwise it's already complete and this is a no-op.
      initFuture.complete(null);

      // Always perform a full refresh (we don't know how long we were disconnected)
      context
          .getMetadataManager()
          .refreshNodes()
          .whenComplete(
              (result, error) -> {
                if (error != null) {
                  LOG.debug("[{}] Error while refreshing node list", logPrefix, error);
                } else {
                  try {
                    // A failed node list refresh at startup is not fatal, so this might be the
                    // first successful refresh; make sure the LBP gets initialized (this is a no-op
                    // if it was initialized already).
                    context.getLoadBalancingPolicyWrapper().init();
                    context.getMetadataManager().refreshSchema(null, false, true);
                  } catch (Throwable t) {
                    Loggers.warnWithException(
                        LOG, "[{}] Unexpected error on control connection reconnect", logPrefix, t);
                  }
                }
              });
    }

    private void onChannelClosed(DriverChannel channel, Node node) {
      assert adminExecutor.inEventLoop();
      if (!closeWasCalled) {
        LOG.debug("[{}] Lost channel {}", logPrefix, channel);
        context.getEventBus().fire(ChannelEvent.channelClosed(node));
        reconnection.start();
      }
    }

    private void reconnectNow() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled && !closeWasCalled) {
        reconnection.reconnectNow(true);
      }
    }

    private void onDistanceEvent(DistanceEvent event) {
      assert adminExecutor.inEventLoop();
      this.lastDistanceEvents.put(event.node, event);
      if (event.distance == NodeDistance.IGNORED
          && channel != null
          && !channel.closeFuture().isDone()
          && event.node.getConnectAddress().equals(channel.remoteAddress())) {
        LOG.debug(
            "[{}] Control node {} became IGNORED, reconnecting to a different node",
            logPrefix,
            event.node);
        reconnectNow();
      }
    }

    private void onStateEvent(NodeStateEvent event) {
      assert adminExecutor.inEventLoop();
      this.lastStateEvents.put(event.node, event);
      if ((event.newState == null /*(removed)*/ || event.newState == NodeState.FORCED_DOWN)
          && channel != null
          && !channel.closeFuture().isDone()
          && event.node.getConnectAddress().equals(channel.remoteAddress())) {
        LOG.debug(
            "[{}] Control node {} was removed or forced down, reconnecting to a different node",
            logPrefix,
            event.node);
        reconnectNow();
      }
    }

    private void forceClose() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      LOG.debug("[{}] Starting shutdown", logPrefix);
      reconnection.stop();
      if (channel == null) {
        LOG.debug("[{}] Shutdown complete", logPrefix);
        closeFuture.complete(null);
      } else {
        channel
            .forceClose()
            .addListener(
                f -> {
                  if (f.isSuccess()) {
                    LOG.debug("[{}] Shutdown complete", logPrefix);
                    closeFuture.complete(null);
                  } else {
                    closeFuture.completeExceptionally(f.cause());
                  }
                });
      }
    }
  }

  private static ImmutableList<String> buildEventTypes(boolean listenClusterEvents) {
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    builder.add(ProtocolConstants.EventType.SCHEMA_CHANGE);
    if (listenClusterEvents) {
      builder
          .add(ProtocolConstants.EventType.STATUS_CHANGE)
          .add(ProtocolConstants.EventType.TOPOLOGY_CHANGE);
    }
    return builder.build();
  }
}
