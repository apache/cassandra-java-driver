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
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Uninterruptibles;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.Future;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.datastax.oss.driver.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Mockito.timeout;

public class DefaultSessionTest {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");

  @Mock private InternalDriverContext context;
  @Mock private NettyOptions nettyOptions;
  @Mock private ChannelPoolFactory channelPoolFactory;
  @Mock private MetadataManager metadataManager;
  @Mock private Metadata metadata;
  @Mock private DriverConfig config;
  @Mock private DriverConfigProfile defaultConfigProfile;

  private DefaultNode node1;
  private DefaultNode node2;
  private DefaultNode node3;
  private DefaultEventLoopGroup adminEventLoopGroup;
  private EventBus eventBus;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.initMocks(this);

    adminEventLoopGroup = new DefaultEventLoopGroup(1);
    Mockito.when(context.nettyOptions()).thenReturn(nettyOptions);
    Mockito.when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);

    Mockito.when(context.channelPoolFactory()).thenReturn(channelPoolFactory);

    eventBus = Mockito.spy(new EventBus("test"));
    Mockito.when(context.eventBus()).thenReturn(eventBus);

    node1 = mockLocalNode(1);
    node2 = mockLocalNode(2);
    node3 = mockLocalNode(3);
    ImmutableMap<InetSocketAddress, Node> nodes =
        ImmutableMap.of(
            node1.getConnectAddress(), node1,
            node2.getConnectAddress(), node2,
            node3.getConnectAddress(), node3);
    Mockito.when(metadata.getNodes()).thenReturn(nodes);
    Mockito.when(metadataManager.getMetadata()).thenReturn(metadata);
    Mockito.when(context.metadataManager()).thenReturn(metadataManager);

    Mockito.when(defaultConfigProfile.getBoolean(CoreDriverOption.REQUEST_WARN_IF_SET_KEYSPACE))
        .thenReturn(true);
    Mockito.when(config.defaultProfile()).thenReturn(defaultConfigProfile);
    Mockito.when(context.config()).thenReturn(config);
  }

  @Test
  public void should_initialize_pools_with_distances() {
    Mockito.when(node3.getDistance()).thenReturn(NodeDistance.REMOTE);

    CompletableFuture<ChannelPool> pool1Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool3Future = new CompletableFuture<>();
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .pending(node1, KEYSPACE, NodeDistance.LOCAL, pool1Future)
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .pending(node3, KEYSPACE, NodeDistance.REMOTE, pool3Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.REMOTE);
    waitForPendingAdminTasks();

    assertThat(initFuture).isNotDone();

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);
    waitForPendingAdminTasks();

    assertThat(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools())
                    .containsValues(pool1, pool2, pool3));
  }

  @Test
  public void should_not_connect_to_ignored_nodes() {
    Mockito.when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_not_connect_to_forced_down_nodes() {
    Mockito.when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_adjust_distance_if_changed_while_init() {
    CompletableFuture<ChannelPool> pool1Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool3Future = new CompletableFuture<>();
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .pending(node1, KEYSPACE, NodeDistance.LOCAL, pool1Future)
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .pending(node3, KEYSPACE, NodeDistance.LOCAL, pool3Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();

    assertThat(initFuture).isNotDone();

    // Distance changes while init still pending
    eventBus.fire(new DistanceEvent(NodeDistance.REMOTE, node2));

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);
    waitForPendingAdminTasks();

    Mockito.verify(pool2).resize(NodeDistance.REMOTE);

    assertThat(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools())
                    .containsValues(pool1, pool2, pool3));
  }

  @Test
  public void should_remove_pool_if_ignored_while_init() {
    CompletableFuture<ChannelPool> pool1Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool3Future = new CompletableFuture<>();
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .pending(node1, KEYSPACE, NodeDistance.LOCAL, pool1Future)
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .pending(node3, KEYSPACE, NodeDistance.LOCAL, pool3Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();

    assertThat(initFuture).isNotDone();

    // Distance changes while init still pending
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);
    waitForPendingAdminTasks();

    Mockito.verify(pool2).closeAsync();

    assertThat(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_remove_pool_if_forced_down_while_init() {
    CompletableFuture<ChannelPool> pool1Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    CompletableFuture<ChannelPool> pool3Future = new CompletableFuture<>();
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .pending(node1, KEYSPACE, NodeDistance.LOCAL, pool1Future)
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .pending(node3, KEYSPACE, NodeDistance.LOCAL, pool3Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();

    assertThat(initFuture).isNotDone();

    // Forced down while init still pending
    eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);
    waitForPendingAdminTasks();

    Mockito.verify(pool2).closeAsync();

    assertThat(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_resize_pool_if_distance_changes() {
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();

    eventBus.fire(new DistanceEvent(NodeDistance.REMOTE, node2));
    Mockito.verify(pool2, timeout(100)).resize(NodeDistance.REMOTE);
  }

  @Test
  public void should_remove_pool_if_node_becomes_ignored() {
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();

    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));
    Mockito.verify(pool2, timeout(100)).closeAsync();

    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);
  }

  @Test
  public void should_recreate_pool_if_node_becomes_not_ignored() {
    Mockito.when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            // When node2 becomes not ignored
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool2, pool3);
  }

  @Test
  public void should_remove_pool_if_node_is_forced_down() {
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();

    eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));
    Mockito.verify(pool2, timeout(100)).closeAsync();

    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);
  }

  @Test
  public void should_recreate_pool_if_node_is_forced_back_up() {
    Mockito.when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // init
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            // when node2 comes back up
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool2, pool3);
  }

  @Test
  public void should_adjust_distance_if_changed_while_recreating() {
    Mockito.when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            // When node2 becomes not ignored
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Distance changes again while pool init is in progress
    eventBus.fire(new DistanceEvent(NodeDistance.REMOTE, node2));

    // Now pool init succeeds
    pool2Future.complete(pool2);
    waitForPendingAdminTasks();

    // Pool should have been adjusted
    Mockito.verify(pool2).resize(NodeDistance.REMOTE);

    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool2, pool3);
  }

  @Test
  public void should_remove_pool_if_ignored_while_recreating() {
    Mockito.when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            // When node2 becomes not ignored
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Distance changes to ignored while pool init is in progress
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));

    // Now pool init succeeds
    pool2Future.complete(pool2);
    waitForPendingAdminTasks();

    // Pool should have been closed
    Mockito.verify(pool2).closeAsync();

    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);
  }

  @Test
  public void should_remove_pool_if_forced_down_while_recreating() {
    Mockito.when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            // When node2 becomes not ignored
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Forced down while pool init is in progress
    eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));

    // Now pool init succeeds
    pool2Future.complete(pool2);
    waitForPendingAdminTasks();

    // Pool should have been closed
    Mockito.verify(pool2).closeAsync();

    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);
  }

  @Test
  public void should_close_all_pools_when_closing() {
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());

    CompletionStage<Void> closeFuture = session.closeAsync();
    waitForPendingAdminTasks();
    assertThat(closeFuture).isSuccess();

    Mockito.verify(pool1).closeAsync();
    Mockito.verify(pool2).closeAsync();
    Mockito.verify(pool3).closeAsync();
  }

  @Test
  public void should_force_close_all_pools_when_force_closing() {
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());

    CompletionStage<Void> closeFuture = session.forceCloseAsync();
    waitForPendingAdminTasks();
    assertThat(closeFuture).isSuccess();

    Mockito.verify(pool1).forceCloseAsync();
    Mockito.verify(pool2).forceCloseAsync();
    Mockito.verify(pool3).forceCloseAsync();
  }

  @Test
  public void should_close_pool_if_recreated_while_closing() {
    Mockito.when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // init
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            // when node2 comes back up
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    // node2 comes back up, start initializing a pool for it
    eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // but the session gets closed before pool init completes
    CompletionStage<Void> closeFuture = session.closeAsync();
    waitForPendingAdminTasks();
    assertThat(closeFuture).isSuccess();

    // now pool init completes
    pool2Future.complete(pool2);
    waitForPendingAdminTasks();

    // Pool should have been closed
    Mockito.verify(pool2).forceCloseAsync();
  }

  @Test
  public void should_set_keyspace_on_all_pools() {
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());

    CqlIdentifier newKeyspace = CqlIdentifier.fromInternal("newKeyspace");
    ((DefaultSession) session).setKeyspace(newKeyspace);
    waitForPendingAdminTasks();

    Mockito.verify(pool1).setKeyspace(newKeyspace);
    Mockito.verify(pool2).setKeyspace(newKeyspace);
    Mockito.verify(pool3).setKeyspace(newKeyspace);
  }

  @Test
  public void should_set_keyspace_on_pool_if_recreated_while_switching_keyspace() {
    Mockito.when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    CompletableFuture<ChannelPool> pool2Future = new CompletableFuture<>();
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // init
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            // when node2 comes back up
            .pending(node2, KEYSPACE, NodeDistance.LOCAL, pool2Future)
            .build();

    CompletionStage<Session> initFuture = DefaultSession.init(context, KEYSPACE, "test");

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    waitForPendingAdminTasks();
    assertThat(initFuture).isSuccess();
    DefaultSession session =
        (DefaultSession) CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(session.getPools()).containsValues(pool1, pool3);

    // node2 comes back up, start initializing a pool for it
    eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Keyspace gets changed on the session in the meantime, node2's pool will miss it
    CqlIdentifier newKeyspace = CqlIdentifier.fromInternal("newKeyspace");
    session.setKeyspace(newKeyspace);
    waitForPendingAdminTasks();
    Mockito.verify(pool1).setKeyspace(newKeyspace);
    Mockito.verify(pool3).setKeyspace(newKeyspace);

    // now pool init completes
    pool2Future.complete(pool2);
    waitForPendingAdminTasks();

    // Pool should have been closed
    Mockito.verify(pool2).setKeyspace(newKeyspace);
  }

  private ChannelPool mockPool(Node node) {
    ChannelPool pool = Mockito.mock(ChannelPool.class);
    Mockito.when(pool.getNode()).thenReturn(node);
    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    Mockito.when(pool.closeFuture()).thenReturn(closeFuture);
    Mockito.when(pool.closeAsync())
        .then(
            i -> {
              closeFuture.complete(null);
              return closeFuture;
            });
    Mockito.when(pool.forceCloseAsync())
        .then(
            i -> {
              closeFuture.complete(null);
              return closeFuture;
            });
    return pool;
  }

  private static DefaultNode mockLocalNode(int i) {
    DefaultNode node = Mockito.mock(DefaultNode.class);
    Mockito.when(node.getConnectAddress()).thenReturn(new InetSocketAddress("127.0.0." + i, 9042));
    Mockito.when(node.getDistance()).thenReturn(NodeDistance.LOCAL);
    Mockito.when(node.toString()).thenReturn("node" + i);
    return node;
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
