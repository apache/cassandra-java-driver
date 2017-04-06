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
package com.datastax.oss.driver.internal.core.control;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.channel.DriverChannelOptions;
import com.datastax.oss.driver.internal.core.channel.EventCallback;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.SchemaElementKind;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.util.concurrent.Reconnection;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.internal.core.util.concurrent.UncaughtExceptions;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.event.SchemaChangeEvent;
import com.datastax.oss.protocol.internal.response.event.StatusChangeEvent;
import com.datastax.oss.protocol.internal.response.event.TopologyChangeEvent;
import com.google.common.collect.ImmutableList;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
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
public class ControlConnection implements EventCallback {
  private static final Logger LOG = LoggerFactory.getLogger(ControlConnection.class);

  private final InternalDriverContext context;
  private final EventExecutor adminExecutor;
  private final SingleThreaded singleThreaded;

  // The single channel used by this connection. This field is accessed currently, but only
  // mutated on adminExecutor (by SingleThreaded methods)
  private volatile DriverChannel channel;

  public ControlConnection(InternalDriverContext context) {
    this.context = context;
    this.adminExecutor = context.nettyOptions().adminEventExecutorGroup().next();
    this.singleThreaded = new SingleThreaded(context);
  }

  /**
   * @param listenToClusterEvents whether to register for TOPOLOGY_CHANGE and STATUS_CHANGE events.
   *     If the control connection has already initialized with another value, this is ignored.
   *     SCHEMA_CHANGE events are always registered.
   */
  public CompletionStage<Void> init(boolean listenToClusterEvents) {
    RunOrSchedule.on(adminExecutor, () -> singleThreaded.init(listenToClusterEvents));
    return singleThreaded.initFuture;
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

  /** Note: control queries are never critical, so there is no graceful close. */
  public CompletionStage<Void> forceClose() {
    RunOrSchedule.on(adminExecutor, singleThreaded::forceClose);
    return singleThreaded.closeFuture;
  }

  @Override
  public void onEvent(Message eventMessage) {
    if (!(eventMessage instanceof Event)) {
      LOG.warn("Unsupported event class: {}", eventMessage.getClass().getName());
    } else {
      LOG.debug("Processing incoming event {}", eventMessage);
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
          LOG.warn("Unsupported event type: {}", event.type);
      }
    }
  }

  private void processTopologyChange(Event event) {
    TopologyChangeEvent tce = (TopologyChangeEvent) event;
    InetSocketAddress address = context.addressTranslator().translate(tce.address);
    switch (tce.changeType) {
      case ProtocolConstants.TopologyChangeType.NEW_NODE:
        context.eventBus().fire(TopologyEvent.suggestAdded(address));
        break;
      case ProtocolConstants.TopologyChangeType.REMOVED_NODE:
        context.eventBus().fire(TopologyEvent.suggestRemoved(address));
        break;
      default:
        LOG.warn("Unsupported topology change type: {}", tce.changeType);
    }
  }

  private void processStatusChange(Event event) {
    StatusChangeEvent sce = (StatusChangeEvent) event;
    InetSocketAddress address = context.addressTranslator().translate(sce.address);
    switch (sce.changeType) {
      case ProtocolConstants.StatusChangeType.UP:
        context.eventBus().fire(TopologyEvent.suggestUp(address));
        break;
      case ProtocolConstants.StatusChangeType.DOWN:
        context.eventBus().fire(TopologyEvent.suggestDown(address));
        break;
      default:
        LOG.warn("Unsupported status change type: {}", sce.changeType);
    }
  }

  private void processSchemaChange(Event event) {
    SchemaChangeEvent sce = (SchemaChangeEvent) event;
    context
        .metadataManager()
        .refreshSchema(
            SchemaElementKind.fromProtocolString(sce.target),
            sce.keyspace,
            sce.object,
            sce.arguments);
  }

  private class SingleThreaded {
    private final InternalDriverContext context;
    private final CompletableFuture<Void> initFuture = new CompletableFuture<>();
    private boolean initWasCalled;
    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    private boolean closeWasCalled;
    private final Reconnection reconnection;
    private DriverChannelOptions channelOptions;

    private SingleThreaded(InternalDriverContext context) {
      this.context = context;
      this.reconnection =
          new Reconnection(adminExecutor, context.reconnectionPolicy(), this::reconnect);
    }

    private void init(boolean listenToClusterEvents) {
      assert adminExecutor.inEventLoop();
      if (initWasCalled) {
        return;
      }
      initWasCalled = true;
      ImmutableList<String> eventTypes = buildEventTypes(listenToClusterEvents);
      LOG.debug("Initializing with event types {}", eventTypes);
      channelOptions =
          DriverChannelOptions.builder().withEvents(eventTypes, ControlConnection.this).build();

      Queue<Node> nodes = context.loadBalancingPolicyWrapper().newQueryPlan();

      connect(nodes, null, () -> initFuture.complete(null), initFuture::completeExceptionally);
    }

    private CompletionStage<Boolean> reconnect() {
      assert adminExecutor.inEventLoop();
      Queue<Node> nodes = context.loadBalancingPolicyWrapper().newQueryPlan();
      CompletableFuture<Boolean> result = new CompletableFuture<>();
      connect(nodes, null, () -> result.complete(true), error -> result.complete(false));
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
        LOG.debug("Trying to establish a connection to {}", node);
        context
            .channelFactory()
            .connect(node.getConnectAddress(), channelOptions)
            .whenCompleteAsync(
                (channel, error) -> {
                  try {
                    if (error != null) {
                      if (closeWasCalled) {
                        onSuccess.run(); // abort, we don't really care about the result
                      } else {
                        LOG.debug("Error connecting to " + node + ", trying next node", error);
                        Map<Node, Throwable> newErrors =
                            (errors == null) ? new LinkedHashMap<>() : errors;
                        newErrors.put(node, error);
                        connect(nodes, newErrors, onSuccess, onFailure);
                      }
                    } else if (closeWasCalled) {
                      LOG.debug(
                          "New channel opened ({}) but the control connection was closed, closing it",
                          channel);
                      channel.forceClose();
                      onSuccess.run();
                    } else {
                      LOG.debug("Connection established to {}", node);
                      // Make sure previous channel gets closed (it may still be open if reconnection was forced)
                      DriverChannel previousChannel = ControlConnection.this.channel;
                      if (previousChannel != null) {
                        previousChannel.forceClose();
                      }
                      ControlConnection.this.channel = channel;
                      context.eventBus().fire(ChannelEvent.channelOpened(node.getConnectAddress()));
                      channel
                          .closeFuture()
                          .addListener(
                              f ->
                                  adminExecutor
                                      .submit(() -> onChannelClosed(channel))
                                      .addListener(UncaughtExceptions::log));
                      onSuccess.run();
                    }
                  } catch (Exception e) {
                    LOG.warn("Unexpected exception while processing channel init result", e);
                  }
                },
                adminExecutor);
      }
    }

    private void onChannelClosed(DriverChannel channel) {
      assert adminExecutor.inEventLoop();
      LOG.debug("Lost channel {}", channel);
      context.eventBus().fire(ChannelEvent.channelClosed(channel.address()));
      if (!closeWasCalled && !reconnection.isRunning()) {
        reconnection.start();
      }
    }

    private void reconnectNow() {
      assert adminExecutor.inEventLoop();
      if (initWasCalled && !closeWasCalled) {
        reconnection.reconnectNow(true);
      }
    }

    private void forceClose() {
      assert adminExecutor.inEventLoop();
      if (closeWasCalled) {
        return;
      }
      closeWasCalled = true;
      reconnection.stop();
      if (channel == null) {
        closeFuture.complete(null);
      } else {
        channel
            .forceClose()
            .addListener(
                f -> {
                  if (f.isSuccess()) {
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
