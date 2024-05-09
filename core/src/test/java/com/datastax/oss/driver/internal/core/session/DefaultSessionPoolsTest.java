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
package com.datastax.oss.driver.internal.core.session;

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.Assertions.assertThatStage;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.connection.ReconnectionPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListener;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.specex.SpeculativeExecutionPolicy;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.context.NettyOptions;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.DistanceEvent;
import com.datastax.oss.driver.internal.core.metadata.LoadBalancingPolicyWrapper;
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TestNodeFactory;
import com.datastax.oss.driver.internal.core.metadata.TopologyMonitor;
import com.datastax.oss.driver.internal.core.metrics.MetricsFactory;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.pool.ChannelPoolFactory;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.util.concurrent.DefaultPromise;
import io.netty.util.concurrent.GlobalEventExecutor;
import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.verification.VerificationWithTimeout;

public class DefaultSessionPoolsTest {

  private static final CqlIdentifier KEYSPACE = CqlIdentifier.fromInternal("ks");
  /** How long we wait when verifying mocks for async invocations */
  protected static final VerificationWithTimeout VERIFY_TIMEOUT = timeout(500);

  @Mock private InternalDriverContext context;
  @Mock private NettyOptions nettyOptions;
  @Mock private ChannelPoolFactory channelPoolFactory;
  @Mock private MetadataManager metadataManager;
  @Mock private TopologyMonitor topologyMonitor;
  @Mock private LoadBalancingPolicyWrapper loadBalancingPolicyWrapper;
  @Mock private DriverConfigLoader configLoader;
  @Mock private Metadata metadata;
  @Mock private DriverConfig config;
  @Mock private DriverExecutionProfile defaultProfile;
  @Mock private ReconnectionPolicy reconnectionPolicy;
  @Mock private RetryPolicy retryPolicy;
  @Mock private SpeculativeExecutionPolicy speculativeExecutionPolicy;
  @Mock private AddressTranslator addressTranslator;
  @Mock private ControlConnection controlConnection;
  @Mock private MetricsFactory metricsFactory;
  @Mock private NodeStateListener nodeStateListener;
  @Mock private SchemaChangeListener schemaChangeListener;
  @Mock private RequestTracker requestTracker;

  private DefaultNode node1;
  private DefaultNode node2;
  private DefaultNode node3;
  private EventBus eventBus;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    DefaultEventLoopGroup adminEventLoopGroup = new DefaultEventLoopGroup(1);
    when(nettyOptions.adminEventExecutorGroup()).thenReturn(adminEventLoopGroup);
    when(context.getNettyOptions()).thenReturn(nettyOptions);

    // Config:
    when(defaultProfile.getBoolean(DefaultDriverOption.REQUEST_WARN_IF_SET_KEYSPACE))
        .thenReturn(true);
    when(defaultProfile.getBoolean(DefaultDriverOption.REPREPARE_ENABLED)).thenReturn(false);
    when(defaultProfile.isDefined(DefaultDriverOption.PROTOCOL_VERSION)).thenReturn(true);
    when(defaultProfile.getDuration(DefaultDriverOption.METADATA_TOPOLOGY_WINDOW))
        .thenReturn(Duration.ZERO);
    when(defaultProfile.getInt(DefaultDriverOption.METADATA_TOPOLOGY_MAX_EVENTS)).thenReturn(1);
    when(config.getDefaultProfile()).thenReturn(defaultProfile);
    when(context.getConfig()).thenReturn(config);

    // Init sequence:
    when(metadataManager.refreshNodes()).thenReturn(CompletableFuture.completedFuture(null));
    when(metadataManager.refreshSchema(null, false, true))
        .thenReturn(CompletableFuture.completedFuture(null));
    when(context.getMetadataManager()).thenReturn(metadataManager);

    when(topologyMonitor.init()).thenReturn(CompletableFuture.completedFuture(null));
    when(context.getTopologyMonitor()).thenReturn(topologyMonitor);

    when(context.getLoadBalancingPolicyWrapper()).thenReturn(loadBalancingPolicyWrapper);

    when(context.getConfigLoader()).thenReturn(configLoader);

    when(context.getMetricsFactory()).thenReturn(metricsFactory);

    // Runtime behavior:
    when(context.getSessionName()).thenReturn("test");

    when(context.getChannelPoolFactory()).thenReturn(channelPoolFactory);

    eventBus = spy(new EventBus("test"));
    when(context.getEventBus()).thenReturn(eventBus);

    node1 = mockLocalNode(1);
    node2 = mockLocalNode(2);
    node3 = mockLocalNode(3);
    @SuppressWarnings("ConstantConditions")
    ImmutableMap<UUID, Node> nodes =
        ImmutableMap.of(
            node1.getHostId(), node1,
            node2.getHostId(), node2,
            node3.getHostId(), node3);
    when(metadata.getNodes()).thenReturn(nodes);
    when(metadataManager.getMetadata()).thenReturn(metadata);

    PoolManager poolManager = new PoolManager(context);
    when(context.getPoolManager()).thenReturn(poolManager);

    // Shutdown sequence:
    when(context.getReconnectionPolicy()).thenReturn(reconnectionPolicy);
    when(context.getRetryPolicy(DriverExecutionProfile.DEFAULT_NAME)).thenReturn(retryPolicy);
    when(context.getSpeculativeExecutionPolicies())
        .thenReturn(
            ImmutableMap.of(DriverExecutionProfile.DEFAULT_NAME, speculativeExecutionPolicy));
    when(context.getAddressTranslator()).thenReturn(addressTranslator);
    when(context.getNodeStateListener()).thenReturn(nodeStateListener);
    when(context.getSchemaChangeListener()).thenReturn(schemaChangeListener);
    when(context.getRequestTracker()).thenReturn(requestTracker);

    when(metadataManager.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
    when(metadataManager.forceCloseAsync()).thenReturn(CompletableFuture.completedFuture(null));

    when(topologyMonitor.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
    when(topologyMonitor.forceCloseAsync()).thenReturn(CompletableFuture.completedFuture(null));

    when(context.getControlConnection()).thenReturn(controlConnection);
    when(controlConnection.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));
    when(controlConnection.forceCloseAsync()).thenReturn(CompletableFuture.completedFuture(null));

    DefaultPromise<Void> nettyCloseFuture = new DefaultPromise<>(GlobalEventExecutor.INSTANCE);
    nettyCloseFuture.setSuccess(null);
    when(nettyOptions.onClose()).thenAnswer(invocation -> nettyCloseFuture);
  }

  @Test
  public void should_initialize_pools_with_distances() {
    when(node3.getDistance()).thenReturn(NodeDistance.REMOTE);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.REMOTE);

    assertThatStage(initFuture).isNotDone();

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);

    assertThatStage(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools())
                    .containsValues(pool1, pool2, pool3));
  }

  @Test
  public void should_not_connect_to_ignored_nodes() {
    when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture)
        .isSuccess(
            session ->
                assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_not_connect_to_forced_down_nodes() {
    when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // Initial connection
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture)
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);

    assertThatStage(initFuture).isNotDone();

    // Distance changes while init still pending
    eventBus.fire(new DistanceEvent(NodeDistance.REMOTE, node2));

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);

    verify(pool2, VERIFY_TIMEOUT).resize(NodeDistance.REMOTE);

    assertThatStage(initFuture)
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);

    assertThatStage(initFuture).isNotDone();

    // Distance changes while init still pending
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);

    verify(pool2, VERIFY_TIMEOUT).closeAsync();

    assertThatStage(initFuture)
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);

    assertThatStage(initFuture).isNotDone();

    // Forced down while init still pending
    eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));

    pool1Future.complete(pool1);
    pool2Future.complete(pool2);
    pool3Future.complete(pool3);

    verify(pool2, VERIFY_TIMEOUT).closeAsync();

    assertThatStage(initFuture)
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(new DistanceEvent(NodeDistance.REMOTE, node2));
    verify(pool2, timeout(500)).resize(NodeDistance.REMOTE);
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));
    verify(pool2, timeout(500)).closeAsync();

    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    await()
        .untilAsserted(
            () -> assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_do_nothing_if_node_becomes_ignored_but_was_already_ignored()
      throws InterruptedException {
    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool2 = mockPool(node2);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node2, KEYSPACE, NodeDistance.LOCAL, pool2)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));
    verify(pool2, timeout(100)).closeAsync();

    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    // Fire the same event again, nothing should happen
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));
    TimeUnit.MILLISECONDS.sleep(200);
    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_recreate_pool_if_node_becomes_not_ignored() {
    when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    await()
        .untilAsserted(
            () ->
                assertThat(((DefaultSession) session).getPools())
                    .containsValues(pool1, pool2, pool3));
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();

    eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));
    verify(pool2, timeout(500)).closeAsync();

    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    await()
        .untilAsserted(
            () -> assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_recreate_pool_if_node_is_forced_back_up() {
    when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    await()
        .untilAsserted(
            () ->
                assertThat(((DefaultSession) session).getPools())
                    .containsValues(pool1, pool2, pool3));
  }

  @Test
  public void should_not_recreate_pool_if_node_is_forced_back_up_but_ignored() {
    when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);
    when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

    ChannelPool pool1 = mockPool(node1);
    ChannelPool pool3 = mockPool(node3);
    MockChannelPoolFactoryHelper factoryHelper =
        MockChannelPoolFactoryHelper.builder(channelPoolFactory)
            // init
            .success(node1, KEYSPACE, NodeDistance.LOCAL, pool1)
            .success(node3, KEYSPACE, NodeDistance.LOCAL, pool3)
            .build();

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
    await()
        .untilAsserted(
            () -> assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
    factoryHelper.verifyNoMoreCalls();
  }

  @Test
  public void should_adjust_distance_if_changed_while_recreating() {
    when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Distance changes again while pool init is in progress
    eventBus.fire(new DistanceEvent(NodeDistance.REMOTE, node2));

    // Now pool init succeeds
    pool2Future.complete(pool2);

    // Pool should have been adjusted
    verify(pool2, VERIFY_TIMEOUT).resize(NodeDistance.REMOTE);
    await()
        .untilAsserted(
            () ->
                assertThat(((DefaultSession) session).getPools())
                    .containsValues(pool1, pool2, pool3));
  }

  @Test
  public void should_remove_pool_if_ignored_while_recreating() {
    when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Distance changes to ignored while pool init is in progress
    eventBus.fire(new DistanceEvent(NodeDistance.IGNORED, node2));

    // Now pool init succeeds
    pool2Future.complete(pool2);

    // Pool should have been closed
    verify(pool2, VERIFY_TIMEOUT).closeAsync();

    await()
        .untilAsserted(
            () -> assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
  }

  @Test
  public void should_remove_pool_if_forced_down_while_recreating() {
    when(node2.getDistance()).thenReturn(NodeDistance.IGNORED);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    eventBus.fire(new DistanceEvent(NodeDistance.LOCAL, node2));

    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Forced down while pool init is in progress
    eventBus.fire(NodeStateEvent.changed(NodeState.UP, NodeState.FORCED_DOWN, node2));

    // Now pool init succeeds
    pool2Future.complete(pool2);

    // Pool should have been closed
    verify(pool2, VERIFY_TIMEOUT).closeAsync();
    await()
        .untilAsserted(
            () -> assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3));
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());

    CompletionStage<Void> closeFuture = session.closeAsync();
    assertThatStage(closeFuture).isSuccess();

    verify(pool1, VERIFY_TIMEOUT).closeAsync();
    verify(pool2, VERIFY_TIMEOUT).closeAsync();
    verify(pool3, VERIFY_TIMEOUT).closeAsync();
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());

    CompletionStage<Void> closeFuture = session.forceCloseAsync();
    assertThatStage(closeFuture).isSuccess();

    verify(pool1, VERIFY_TIMEOUT).forceCloseAsync();
    verify(pool2, VERIFY_TIMEOUT).forceCloseAsync();
    verify(pool3, VERIFY_TIMEOUT).forceCloseAsync();
  }

  @Test
  public void should_close_pool_if_recreated_while_closing() {
    when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(((DefaultSession) session).getPools()).containsValues(pool1, pool3);

    // node2 comes back up, start initializing a pool for it
    eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // but the session gets closed before pool init completes
    CompletionStage<Void> closeFuture = session.closeAsync();
    assertThatStage(closeFuture).isSuccess();

    // now pool init completes
    pool2Future.complete(pool2);

    // Pool should have been closed
    verify(pool2, VERIFY_TIMEOUT).forceCloseAsync();
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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    Session session = CompletableFutures.getCompleted(initFuture.toCompletableFuture());

    CqlIdentifier newKeyspace = CqlIdentifier.fromInternal("newKeyspace");
    ((DefaultSession) session).setKeyspace(newKeyspace);

    verify(pool1, VERIFY_TIMEOUT).setKeyspace(newKeyspace);
    verify(pool2, VERIFY_TIMEOUT).setKeyspace(newKeyspace);
    verify(pool3, VERIFY_TIMEOUT).setKeyspace(newKeyspace);
  }

  @Test
  public void should_set_keyspace_on_pool_if_recreated_while_switching_keyspace() {
    when(node2.getState()).thenReturn(NodeState.FORCED_DOWN);

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

    CompletionStage<CqlSession> initFuture = newSession();

    factoryHelper.waitForCall(node1, KEYSPACE, NodeDistance.LOCAL);
    factoryHelper.waitForCall(node3, KEYSPACE, NodeDistance.LOCAL);
    assertThatStage(initFuture).isSuccess();
    DefaultSession session =
        (DefaultSession) CompletableFutures.getCompleted(initFuture.toCompletableFuture());
    assertThat(session.getPools()).containsValues(pool1, pool3);

    // node2 comes back up, start initializing a pool for it
    eventBus.fire(NodeStateEvent.changed(NodeState.FORCED_DOWN, NodeState.UP, node2));
    factoryHelper.waitForCall(node2, KEYSPACE, NodeDistance.LOCAL);

    // Keyspace gets changed on the session in the meantime, node2's pool will miss it
    CqlIdentifier newKeyspace = CqlIdentifier.fromInternal("newKeyspace");
    session.setKeyspace(newKeyspace);
    verify(pool1, VERIFY_TIMEOUT).setKeyspace(newKeyspace);
    verify(pool3, VERIFY_TIMEOUT).setKeyspace(newKeyspace);

    // now pool init completes
    pool2Future.complete(pool2);

    // Pool should have been closed
    verify(pool2, VERIFY_TIMEOUT).setKeyspace(newKeyspace);
  }

  private ChannelPool mockPool(Node node) {
    ChannelPool pool = mock(ChannelPool.class);
    when(pool.getNode()).thenReturn(node);
    when(pool.getInitialKeyspaceName()).thenReturn(KEYSPACE);
    when(pool.setKeyspace(any(CqlIdentifier.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
    CompletableFuture<Void> closeFuture = new CompletableFuture<>();
    when(pool.closeFuture()).thenReturn(closeFuture);
    when(pool.closeAsync())
        .then(
            i -> {
              closeFuture.complete(null);
              return closeFuture;
            });
    when(pool.forceCloseAsync())
        .then(
            i -> {
              closeFuture.complete(null);
              return closeFuture;
            });
    return pool;
  }

  private CompletionStage<CqlSession> newSession() {
    return DefaultSession.init(context, Collections.emptySet(), KEYSPACE);
  }

  private static DefaultNode mockLocalNode(int i) {
    DefaultNode node = mock(DefaultNode.class);
    when(node.getHostId()).thenReturn(UUID.randomUUID());
    DefaultEndPoint endPoint = TestNodeFactory.newEndPoint(i);
    when(node.getEndPoint()).thenReturn(endPoint);
    when(node.getBroadcastRpcAddress()).thenReturn(Optional.of(endPoint.resolve()));
    when(node.getDistance()).thenReturn(NodeDistance.LOCAL);
    when(node.toString()).thenReturn("node" + i);
    return node;
  }
}
