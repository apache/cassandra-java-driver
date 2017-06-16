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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.channel.ChannelEvent;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.util.concurrent.BlockingOperation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

public class NodeStateManagerTest {
  private static final InetSocketAddress NEW_ADDRESS = new InetSocketAddress("127.0.0.3", 9042);

  @Mock private InternalDriverContext context;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultConfigProfile;
  @Mock private NettyOptions nettyOptions;
  @Mock private MetadataManager metadataManager;
  @Mock private Metadata metadata;
  private DefaultNode node1, node2;
  private EventBus eventBus;
  private DefaultEventLoopGroup adminEventLoopGroup;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    // Disable debouncing by default, tests that need it will override
    Mockito.when(defaultConfigProfile.getDuration(CoreDriverOption.METADATA_TOPOLOGY_WINDOW))
        .thenReturn(Duration.ofSeconds(0));
    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.METADATA_TOPOLOGY_MAX_EVENTS))
        .thenReturn(1);
    Mockito.when(config.defaultProfile()).thenReturn(defaultConfigProfile);
    Mockito.when(context.config()).thenReturn(config);

    this.eventBus = Mockito.spy(new EventBus("test"));
    Mockito.when(context.eventBus()).thenReturn(eventBus);

    adminEventLoopGroup = new DefaultEventLoopGroup(1, new BlockingOperation.SafeThreadFactory());
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);

    node1 = new DefaultNode(new InetSocketAddress("127.0.0.1", 9042));
    node2 = new DefaultNode(new InetSocketAddress("127.0.0.2", 9042));
    ImmutableMap<InetSocketAddress, Node> nodes =
        ImmutableMap.<InetSocketAddress, Node>builder()
            .put(node1.getConnectAddress(), node1)
            .put(node2.getConnectAddress(), node2)
            .build();
    Mockito.when(metadata.getNodes()).thenReturn(nodes);
    Mockito.when(metadataManager.getMetadata()).thenReturn(metadata);
    Mockito.when(metadataManager.refreshNode(any(Node.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
    Mockito.when(context.metadataManager()).thenReturn(metadataManager);
  }

  @AfterMethod
  public void teardown() {
    adminEventLoopGroup.shutdownGracefully(100, 200, TimeUnit.MILLISECONDS);
  }

  @Test
  public void should_ignore_up_event_if_node_is_already_up_or_forced_down() {
    new NodeStateManager(context);

    for (NodeState oldState : ImmutableList.of(NodeState.UP, NodeState.FORCED_DOWN)) {
      // Given
      node1.state = oldState;

      // When
      eventBus.fire(TopologyEvent.suggestUp(node1.getConnectAddress()));
      waitForPendingAdminTasks();

      // Then
      assertThat(node1.state).isEqualTo(oldState);
    }
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
  }

  @Test
  public void should_apply_up_event_if_node_is_unknown_or_down() {
    new NodeStateManager(context);

    int i = 0;
    for (NodeState oldState : ImmutableList.of(NodeState.UNKNOWN, NodeState.DOWN)) {
      // Given
      node1.state = oldState;

      // When
      eventBus.fire(TopologyEvent.suggestUp(node1.getConnectAddress()));
      waitForPendingAdminTasks();

      // Then
      assertThat(node1.state).isEqualTo(NodeState.UP);
      Mockito.verify(metadataManager, times(++i)).refreshNode(node1);
      Mockito.verify(eventBus).fire(NodeStateEvent.changed(oldState, NodeState.UP, node1));
    }
  }

  @Test
  public void should_add_node_if_up_event_and_not_in_metadata() {
    // Given
    new NodeStateManager(context);

    // When
    eventBus.fire(TopologyEvent.suggestUp(NEW_ADDRESS));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
    Mockito.verify(metadataManager).addNode(NEW_ADDRESS);
  }

  @Test
  public void should_ignore_down_event_if_node_is_down_or_forced_down() {
    new NodeStateManager(context);

    for (NodeState oldState : ImmutableList.of(NodeState.DOWN, NodeState.FORCED_DOWN)) {
      // Given
      node1.state = oldState;

      // When
      eventBus.fire(TopologyEvent.suggestDown(node1.getConnectAddress()));
      waitForPendingAdminTasks();

      // Then
      assertThat(node1.state).isEqualTo(oldState);
    }
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
  }

  @Test
  public void should_ignore_down_event_if_node_has_active_connections() {
    new NodeStateManager(context);
    node1.state = NodeState.UP;
    eventBus.fire(ChannelEvent.channelOpened(node1));
    waitForPendingAdminTasks();
    assertThat(node1.openConnections).isEqualTo(1);

    // When
    eventBus.fire(TopologyEvent.suggestDown(node1.getConnectAddress()));
    waitForPendingAdminTasks();

    // Then
    assertThat(node1.state).isEqualTo(NodeState.UP);
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
  }

  @Test
  public void should_apply_down_event_if_node_has_no_active_connections() {
    new NodeStateManager(context);

    for (NodeState oldState : ImmutableList.of(NodeState.UP, NodeState.UNKNOWN)) {
      // Given
      node1.state = oldState;
      assertThat(node1.openConnections).isEqualTo(0);

      // When
      eventBus.fire(TopologyEvent.suggestDown(node1.getConnectAddress()));
      waitForPendingAdminTasks();

      // Then
      assertThat(node1.state).isEqualTo(NodeState.DOWN);
      Mockito.verify(eventBus).fire(NodeStateEvent.changed(oldState, NodeState.DOWN, node1));
    }
  }

  @Test
  public void should_ignore_down_event_if_not_in_metadata() {
    // Given
    new NodeStateManager(context);

    // When
    eventBus.fire(TopologyEvent.suggestDown(NEW_ADDRESS));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
    Mockito.verify(metadataManager, never()).addNode(NEW_ADDRESS);
  }

  @Test
  public void should_ignore_force_down_event_if_already_forced_down() {
    // Given
    new NodeStateManager(context);
    node1.state = NodeState.FORCED_DOWN;

    // When
    eventBus.fire(TopologyEvent.forceDown(node1.getConnectAddress()));
    waitForPendingAdminTasks();

    // Then
    assertThat(node1.state).isEqualTo(NodeState.FORCED_DOWN);
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
  }

  @Test
  public void should_apply_force_down_event_over_any_other_state() {
    new NodeStateManager(context);

    for (NodeState oldState : ImmutableList.of(NodeState.UNKNOWN, NodeState.DOWN, NodeState.UP)) {
      // Given
      node1.state = oldState;

      // When
      eventBus.fire(TopologyEvent.forceDown(node1.getConnectAddress()));
      waitForPendingAdminTasks();

      // Then
      assertThat(node1.state).isEqualTo(NodeState.FORCED_DOWN);
      Mockito.verify(eventBus).fire(NodeStateEvent.changed(oldState, NodeState.FORCED_DOWN, node1));
    }
  }

  @Test
  public void should_ignore_force_down_event_if_not_in_metadata() {
    // Given
    new NodeStateManager(context);

    // When
    eventBus.fire(TopologyEvent.forceDown(NEW_ADDRESS));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
    Mockito.verify(metadataManager, never()).addNode(NEW_ADDRESS);
  }

  @Test
  public void should_ignore_force_up_event_if_node_is_already_up() {
    // Given
    new NodeStateManager(context);
    node1.state = NodeState.UP;

    // When
    eventBus.fire(TopologyEvent.forceUp(node1.getConnectAddress()));
    waitForPendingAdminTasks();

    // Then
    assertThat(node1.state).isEqualTo(NodeState.UP);
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
  }

  @Test
  public void should_apply_force_up_event_if_node_is_not_up() {
    new NodeStateManager(context);

    int i = 0;
    for (NodeState oldState :
        ImmutableList.of(NodeState.UNKNOWN, NodeState.DOWN, NodeState.FORCED_DOWN)) {
      // Given
      node1.state = oldState;

      // When
      eventBus.fire(TopologyEvent.forceUp(node1.getConnectAddress()));
      waitForPendingAdminTasks();

      // Then
      assertThat(node1.state).isEqualTo(NodeState.UP);
      Mockito.verify(eventBus).fire(NodeStateEvent.changed(oldState, NodeState.UP, node1));
      Mockito.verify(metadataManager, times(++i)).refreshNode(node1);
    }
  }

  @Test
  public void should_add_node_if_force_up_and_not_in_metadata() {
    // Given
    new NodeStateManager(context);

    // When
    eventBus.fire(TopologyEvent.forceUp(NEW_ADDRESS));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
    Mockito.verify(metadataManager).addNode(NEW_ADDRESS);
  }

  @Test
  public void should_notify_metadata_of_node_addition() {
    // Given
    new NodeStateManager(context);
    InetSocketAddress newAddress = NEW_ADDRESS;

    // When
    eventBus.fire(TopologyEvent.suggestAdded(newAddress));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(metadataManager).addNode(newAddress);
  }

  @Test
  public void should_ignore_addition_of_existing_node() {
    // Given
    new NodeStateManager(context);

    // When
    eventBus.fire(TopologyEvent.suggestAdded(node1.getConnectAddress()));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(metadataManager, never()).addNode(any(InetSocketAddress.class));
  }

  @Test
  public void should_notify_metadata_of_node_removal() {
    // Given
    new NodeStateManager(context);

    // When
    eventBus.fire(TopologyEvent.suggestRemoved(node1.getConnectAddress()));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(metadataManager).removeNode(node1.getConnectAddress());
  }

  @Test
  public void should_ignore_removal_of_nonexistent_node() {
    // Given
    new NodeStateManager(context);
    InetSocketAddress newAddress = NEW_ADDRESS;

    // When
    eventBus.fire(TopologyEvent.suggestRemoved(newAddress));
    waitForPendingAdminTasks();

    // Then
    Mockito.verify(metadataManager, never()).removeNode(any(InetSocketAddress.class));
  }

  @Test
  public void should_coalesce_topology_events() {
    // Given
    Mockito.when(defaultConfigProfile.getDuration(CoreDriverOption.METADATA_TOPOLOGY_WINDOW))
        .thenReturn(Duration.ofDays(1));
    Mockito.when(defaultConfigProfile.getInt(CoreDriverOption.METADATA_TOPOLOGY_MAX_EVENTS))
        .thenReturn(5);
    new NodeStateManager(context);
    node1.state = NodeState.FORCED_DOWN;
    node2.state = NodeState.DOWN;

    // When
    eventBus.fire(TopologyEvent.suggestDown(node1.getConnectAddress()));
    eventBus.fire(TopologyEvent.forceUp(node1.getConnectAddress()));
    eventBus.fire(TopologyEvent.suggestDown(node2.getConnectAddress()));
    eventBus.fire(TopologyEvent.suggestDown(node1.getConnectAddress()));
    eventBus.fire(TopologyEvent.suggestUp(node2.getConnectAddress()));
    waitForPendingAdminTasks();

    // Then
    // down / forceUp / down => keep the last forced event => forceUp
    assertThat(node1.state).isEqualTo(NodeState.UP);
    // down / up => keep the last => up
    assertThat(node2.state).isEqualTo(NodeState.UP);
  }

  @Test
  public void should_track_open_connections() {
    new NodeStateManager(context);

    assertThat(node1.openConnections).isEqualTo(0);

    eventBus.fire(ChannelEvent.channelOpened(node1));
    eventBus.fire(ChannelEvent.channelOpened(node1));
    waitForPendingAdminTasks();
    assertThat(node1.openConnections).isEqualTo(2);

    eventBus.fire(ChannelEvent.channelClosed(node1));
    waitForPendingAdminTasks();
    assertThat(node1.openConnections).isEqualTo(1);
  }

  @Test
  public void should_mark_node_up_if_down_or_unknown_and_connection_opened() {
    new NodeStateManager(context);

    for (NodeState oldState : ImmutableList.of(NodeState.DOWN, NodeState.UNKNOWN)) {
      // Given
      node1.state = oldState;

      // When
      eventBus.fire(ChannelEvent.channelOpened(node1));
      waitForPendingAdminTasks();

      // Then
      assertThat(node1.state).isEqualTo(NodeState.UP);
      Mockito.verify(eventBus).fire(NodeStateEvent.changed(oldState, NodeState.UP, node1));
    }
  }

  @Test
  public void should_not_mark_node_up_if_forced_down_and_connection_opened() {
    // Given
    new NodeStateManager(context);
    node1.state = NodeState.FORCED_DOWN;

    // When
    eventBus.fire(ChannelEvent.channelOpened(node1));
    waitForPendingAdminTasks();

    // Then
    assertThat(node1.state).isEqualTo(NodeState.FORCED_DOWN);
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
  }

  @Test
  public void should_track_reconnections() {
    new NodeStateManager(context);

    assertThat(node1.reconnections).isEqualTo(0);

    eventBus.fire(ChannelEvent.reconnectionStarted(node1));
    eventBus.fire(ChannelEvent.reconnectionStarted(node1));
    waitForPendingAdminTasks();
    assertThat(node1.reconnections).isEqualTo(2);

    eventBus.fire(ChannelEvent.reconnectionStopped(node1));
    waitForPendingAdminTasks();
    assertThat(node1.reconnections).isEqualTo(1);
  }

  @Test
  public void should_mark_node_down_if_reconnection_starts_with_no_connections() {
    new NodeStateManager(context);

    node1.state = NodeState.UP;
    node1.openConnections = 1;

    eventBus.fire(ChannelEvent.channelClosed(node1));
    eventBus.fire(ChannelEvent.reconnectionStarted(node1));
    waitForPendingAdminTasks();

    assertThat(node1.state).isEqualTo(NodeState.DOWN);
    Mockito.verify(eventBus).fire(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, node1));
  }

  @Test
  public void should_keep_node_up_if_reconnection_starts_with_some_connections() {
    new NodeStateManager(context);

    node1.state = NodeState.UP;
    node1.openConnections = 2;

    eventBus.fire(ChannelEvent.channelClosed(node1));
    eventBus.fire(ChannelEvent.reconnectionStarted(node1));
    waitForPendingAdminTasks();

    assertThat(node1.state).isEqualTo(NodeState.UP);
    Mockito.verify(eventBus, never()).fire(any(NodeStateEvent.class));
  }

  @Test
  public void should_ignore_events_when_closed() throws Exception {
    NodeStateManager manager = new NodeStateManager(context);
    assertThat(node1.reconnections).isEqualTo(0);

    manager.close();

    eventBus.fire(ChannelEvent.reconnectionStarted(node1));
    waitForPendingAdminTasks();

    assertThat(node1.reconnections).isEqualTo(0);
  }

  // Wait for all the tasks on the pool's admin executor to complete.
  private void waitForPendingAdminTasks() {
    // This works because the event loop group is single-threaded
    Future<?> f = adminEventLoopGroup.schedule(() -> null, 5, TimeUnit.NANOSECONDS);
    try {
      Uninterruptibles.getUninterruptibly(f, 100, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      fail("unexpected error", e.getCause());
    } catch (TimeoutException e) {
      fail("timed out while waiting for admin tasks to complete", e);
    }
  }
}
