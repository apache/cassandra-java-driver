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
package com.datastax.oss.driver.core.metadata;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static com.datastax.oss.driver.assertions.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.NodeStateListener;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeConnectionReport;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.RejectScope;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.junit.MockitoJUnitRunner;

@Category(ParallelizableTests.class)
@RunWith(MockitoJUnitRunner.class)
public class NodeStateIT {

  private SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(2));

  private NodeStateListener nodeStateListener = mock(NodeStateListener.class);
  private InOrder inOrder;

  private SessionRule<CqlSession> sessionRule =
      SessionRule.builder(simulacron)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 2)
                  .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(1))
                  .withClass(
                      DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                      NodeStateIT.ConfigurableIgnoresPolicy.class)
                  .build())
          .withNodeStateListener(nodeStateListener)
          .build();

  @Rule public TestRule chain = RuleChain.outerRule(simulacron).around(sessionRule);

  private @Captor ArgumentCaptor<DefaultNode> nodeCaptor;

  private InternalDriverContext driverContext;
  private ConfigurableIgnoresPolicy defaultLoadBalancingPolicy;
  private final BlockingQueue<NodeStateEvent> stateEvents = new LinkedBlockingDeque<>();

  private BoundNode simulacronControlNode;
  private BoundNode simulacronRegularNode;
  private DefaultNode metadataControlNode;
  private DefaultNode metadataRegularNode;

  @Before
  public void setup() {
    inOrder = inOrder(nodeStateListener);

    AtomicBoolean nonInitialEvent = new AtomicBoolean(false);
    driverContext = (InternalDriverContext) sessionRule.session().getContext();
    driverContext
        .getEventBus()
        .register(
            NodeStateEvent.class,
            (e) -> {
              // Skip transition from unknown to up if we haven't received any other events,
              // these may just be the initial events that have typically fired by now, but
              // may not have depending on timing.
              if (!nonInitialEvent.get()
                  && e.oldState == NodeState.UNKNOWN
                  && e.newState == NodeState.UP) {
                return;
              }
              nonInitialEvent.set(true);
              stateEvents.add(e);
            });

    defaultLoadBalancingPolicy =
        (ConfigurableIgnoresPolicy)
            driverContext.getLoadBalancingPolicy(DriverExecutionProfile.DEFAULT_NAME);

    // Sanity check: the driver should have connected to simulacron
    await()
        .alias("Connections established")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .until(
            () ->
                // 1 control connection + 2 pooled connections per node
                simulacron.cluster().getActiveConnections() == 5);

    // Find out which node is the control node, and identify the corresponding Simulacron and driver
    // metadata objects.
    simulacronControlNode = simulacronRegularNode = null;
    for (BoundNode boundNode : simulacron.cluster().getNodes()) {
      if (boundNode.getActiveConnections() == 3) {
        simulacronControlNode = boundNode;
      } else {
        simulacronRegularNode = boundNode;
      }
    }
    assertThat(simulacronControlNode).isNotNull();
    assertThat(simulacronRegularNode).isNotNull();

    Metadata metadata = sessionRule.session().getMetadata();
    metadataControlNode =
        (DefaultNode)
            metadata
                .findNode(simulacronControlNode.inetSocketAddress())
                .orElseThrow(AssertionError::new);
    metadataRegularNode =
        (DefaultNode)
            metadata
                .findNode(simulacronRegularNode.inetSocketAddress())
                .orElseThrow(AssertionError::new);

    // SessionRule uses all nodes as contact points, so we only get onUp notifications for them (no
    // onAdd)
    inOrder.verify(nodeStateListener, timeout(500)).onUp(metadataControlNode);
    inOrder.verify(nodeStateListener, timeout(500)).onUp(metadataRegularNode);
  }

  @After
  public void teardown() {
    reset(nodeStateListener);
  }

  @Test
  public void should_report_connections_for_healthy_nodes() {
    await()
        .alias("Node metadata up-to-date")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> {
              assertThat(metadataControlNode).isUp().hasOpenConnections(3).isNotReconnecting();
              assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting();
            });
  }

  @Test
  public void should_keep_regular_node_up_when_still_one_connection() {
    simulacronRegularNode.rejectConnections(0, RejectScope.UNBIND);
    NodeConnectionReport report = simulacronRegularNode.getConnections();
    simulacron.cluster().closeConnection(report.getConnections().get(0), CloseType.DISCONNECT);

    await()
        .alias("Reconnection started")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> assertThat(metadataRegularNode).isUp().hasOpenConnections(1).isReconnecting());
    inOrder.verify(nodeStateListener, never()).onDown(metadataRegularNode);
  }

  @Test
  public void should_mark_regular_node_down_when_no_more_connections() {
    simulacronRegularNode.stop();

    await()
        .alias("Node going down")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> assertThat(metadataRegularNode).isDown().hasOpenConnections(0).isReconnecting());

    expect(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, metadataRegularNode));
    inOrder.verify(nodeStateListener, timeout(500)).onDown(metadataRegularNode);
  }

  @Test
  public void should_mark_control_node_down_when_control_connection_is_last_connection_and_dies() {
    simulacronControlNode.rejectConnections(0, RejectScope.UNBIND);

    // Identify the control connection and close the two other ones
    SocketAddress controlAddress = driverContext.getControlConnection().channel().localAddress();
    NodeConnectionReport report = simulacronControlNode.getConnections();
    for (SocketAddress address : report.getConnections()) {
      if (!address.equals(controlAddress)) {
        simulacron.cluster().closeConnection(address, CloseType.DISCONNECT);
      }
    }
    await()
        .alias("Control node lost its non-control connections")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> assertThat(metadataControlNode).isUp().hasOpenConnections(1).isReconnecting());
    inOrder.verify(nodeStateListener, never()).onDown(metadataRegularNode);

    simulacron.cluster().closeConnection(controlAddress, CloseType.DISCONNECT);
    await()
        .alias("Control node going down")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> assertThat(metadataControlNode).isDown().hasOpenConnections(0).isReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onDown(metadataControlNode);

    expect(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, metadataControlNode));
  }

  @Test
  public void should_bring_node_back_up_when_reconnection_succeeds() {
    simulacronRegularNode.stop();

    await()
        .alias("Node going down")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> assertThat(metadataRegularNode).isDown().hasOpenConnections(0).isReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onDown(metadataRegularNode);

    simulacronRegularNode.acceptConnections();

    await()
        .alias("Connections re-established")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onUp(metadataRegularNode);

    expect(
        NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, metadataRegularNode),
        NodeStateEvent.changed(NodeState.DOWN, NodeState.UP, metadataRegularNode));
  }

  @Test
  public void should_apply_up_and_down_topology_events_when_ignored() {
    defaultLoadBalancingPolicy.ignore(metadataRegularNode);

    await()
        .alias("Driver closed all connections to ignored node")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                assertThat(metadataRegularNode)
                    .isUp()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting());

    driverContext
        .getEventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getBroadcastRpcAddress().get()));
    await()
        .alias("SUGGEST_DOWN event applied")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                assertThat(metadataRegularNode)
                    .isDown()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onDown(metadataRegularNode);

    driverContext
        .getEventBus()
        .fire(TopologyEvent.suggestUp(metadataRegularNode.getBroadcastRpcAddress().get()));
    await()
        .alias("SUGGEST_UP event applied")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                assertThat(metadataRegularNode)
                    .isUp()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onUp(metadataRegularNode);

    defaultLoadBalancingPolicy.stopIgnoring(metadataRegularNode);
  }

  @Test
  public void should_ignore_down_topology_event_when_still_connected() throws InterruptedException {
    driverContext
        .getEventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getBroadcastRpcAddress().get()));
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting();
  }

  @Test
  public void should_force_immediate_reconnection_when_up_topology_event()
      throws InterruptedException {
    // This test requires a longer reconnection interval, so create a separate driver instance
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofHours(1))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofHours(1))
            .build();
    NodeStateListener localNodeStateListener = mock(NodeStateListener.class);
    try (CqlSession session =
        SessionUtils.newSession(simulacron, null, localNodeStateListener, null, null, loader)) {

      BoundNode localSimulacronNode = simulacron.cluster().getNodes().iterator().next();
      assertThat(localSimulacronNode).isNotNull();

      DefaultNode localMetadataNode =
          (DefaultNode)
              session
                  .getMetadata()
                  .findNode(localSimulacronNode.inetSocketAddress())
                  .orElseThrow(AssertionError::new);
      // UP fired a first time as part of the init process
      verify(localNodeStateListener, timeout(500)).onUp(localMetadataNode);

      localSimulacronNode.stop();

      await()
          .alias("Node going down")
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(
              () -> assertThat(localMetadataNode).isDown().hasOpenConnections(0).isReconnecting());
      verify(localNodeStateListener, timeout(500)).onDown(localMetadataNode);

      expect(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, localMetadataNode));

      localSimulacronNode.acceptConnections();
      ((InternalDriverContext) session.getContext())
          .getEventBus()
          .fire(TopologyEvent.suggestUp(localMetadataNode.getBroadcastRpcAddress().get()));

      await()
          .alias("Node coming back up")
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .untilAsserted(() -> assertThat(localMetadataNode).isUp().isNotReconnecting());
      verify(localNodeStateListener, timeout(500).times(2)).onUp(localMetadataNode);

      expect(NodeStateEvent.changed(NodeState.DOWN, NodeState.UP, localMetadataNode));
    }
  }

  @Test
  public void should_force_down_when_not_ignored() throws InterruptedException {
    driverContext
        .getEventBus()
        .fire(TopologyEvent.forceDown(metadataRegularNode.getBroadcastRpcAddress().get()));
    await()
        .alias("Node forced down")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                assertThat(metadataRegularNode)
                    .isForcedDown()
                    .hasOpenConnections(0)
                    .isNotReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onDown(metadataRegularNode);

    // Should ignore up/down topology events while forced down
    driverContext
        .getEventBus()
        .fire(TopologyEvent.suggestUp(metadataRegularNode.getBroadcastRpcAddress().get()));
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(metadataRegularNode).isForcedDown();

    driverContext
        .getEventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getBroadcastRpcAddress().get()));
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(metadataRegularNode).isForcedDown();

    // Should only come back up on a FORCE_UP event
    driverContext
        .getEventBus()
        .fire(TopologyEvent.forceUp(metadataRegularNode.getBroadcastRpcAddress().get()));
    await()
        .alias("Node forced back up")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () -> assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onUp(metadataRegularNode);
  }

  @Test
  public void should_force_down_when_ignored() throws InterruptedException {
    defaultLoadBalancingPolicy.ignore(metadataRegularNode);

    driverContext
        .getEventBus()
        .fire(TopologyEvent.forceDown(metadataRegularNode.getBroadcastRpcAddress().get()));
    await()
        .alias("Node forced down")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                assertThat(metadataRegularNode)
                    .isForcedDown()
                    .hasOpenConnections(0)
                    .isNotReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onDown(metadataRegularNode);

    // Should ignore up/down topology events while forced down
    driverContext
        .getEventBus()
        .fire(TopologyEvent.suggestUp(metadataRegularNode.getBroadcastRpcAddress().get()));
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(metadataRegularNode).isForcedDown();

    driverContext
        .getEventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getBroadcastRpcAddress().get()));
    TimeUnit.MILLISECONDS.sleep(500);
    assertThat(metadataRegularNode).isForcedDown();

    // Should only come back up on a FORCE_UP event, will not reopen connections since it is still
    // ignored
    driverContext
        .getEventBus()
        .fire(TopologyEvent.forceUp(metadataRegularNode.getBroadcastRpcAddress().get()));
    await()
        .alias("Node forced back up")
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(
            () ->
                assertThat(metadataRegularNode)
                    .isUp()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting());
    inOrder.verify(nodeStateListener, timeout(500)).onUp(metadataRegularNode);

    defaultLoadBalancingPolicy.stopIgnoring(metadataRegularNode);
  }

  @Test
  public void should_signal_non_contact_points_as_added() {
    // Since we need to observe the behavior of non-contact points, build a dedicated session with
    // just one contact point.
    Iterator<EndPoint> contactPoints = simulacron.getContactPoints().iterator();
    EndPoint endPoint1 = contactPoints.next();
    EndPoint endPoint2 = contactPoints.next();
    NodeStateListener localNodeStateListener = mock(NodeStateListener.class);
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofHours(1))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofHours(1))
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 0)
            .build();
    try (CqlSession localSession =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoint(endPoint1)
                .withNodeStateListener(localNodeStateListener)
                .withConfigLoader(loader)
                .build()) {

      Metadata metadata = localSession.getMetadata();
      Node localMetadataNode1 = metadata.findNode(endPoint1).orElseThrow(AssertionError::new);
      Node localMetadataNode2 = metadata.findNode(endPoint2).orElseThrow(AssertionError::new);

      // Successful contact point goes to up directly
      verify(localNodeStateListener, timeout(500)).onUp(localMetadataNode1);
      // Non-contact point only added since we don't have a connection or events for it yet
      verify(localNodeStateListener, timeout(500)).onAdd(localMetadataNode2);
    }
  }

  @Test
  public void should_remove_invalid_contact_point() {

    Iterator<EndPoint> contactPoints = simulacron.getContactPoints().iterator();
    EndPoint endPoint1 = contactPoints.next();
    EndPoint endPoint2 = contactPoints.next();
    NodeStateListener localNodeStateListener = mock(NodeStateListener.class);

    // Initialize the driver with 1 wrong address and 1 valid address
    EndPoint wrongContactPoint = withUnusedPort(endPoint1);
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofHours(1))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofHours(1))
            .build();
    try (CqlSession localSession =
        (CqlSession)
            SessionUtils.baseBuilder()
                .addContactEndPoint(endPoint1)
                .addContactEndPoint(wrongContactPoint)
                .withNodeStateListener(localNodeStateListener)
                .withConfigLoader(loader)
                .build()) {

      Metadata metadata = localSession.getMetadata();
      assertThat(metadata.findNode(wrongContactPoint)).isEmpty();
      Node localMetadataNode1 = metadata.findNode(endPoint1).orElseThrow(AssertionError::new);
      Node localMetadataNode2 = metadata.findNode(endPoint2).orElseThrow(AssertionError::new);

      // The order of the calls is not deterministic because contact points are shuffled, but it
      // does not matter here since Mockito.verify does not enforce order.
      verify(localNodeStateListener, timeout(500)).onRemove(nodeCaptor.capture());
      assertThat(nodeCaptor.getValue().getEndPoint()).isEqualTo(wrongContactPoint);
      verify(localNodeStateListener, timeout(500)).onUp(localMetadataNode1);
      verify(localNodeStateListener, timeout(500)).onAdd(localMetadataNode2);

      // Note: there might be an additional onDown for wrongContactPoint if it was hit first at
      // init. This is hard to test since the node was removed later, so we simply don't call
      // verifyNoMoreInteractions.
    }
  }

  @Test
  public void should_mark_unreachable_contact_point_down() {
    // This time we connect with two valid contact points, but is unresponsive, it should be marked
    // down
    Iterator<BoundNode> simulacronNodes = simulacron.cluster().getNodes().iterator();
    BoundNode localSimulacronNode1 = simulacronNodes.next();
    BoundNode localSimulacronNode2 = simulacronNodes.next();

    InetSocketAddress address1 = localSimulacronNode1.inetSocketAddress();
    InetSocketAddress address2 = localSimulacronNode2.inetSocketAddress();

    NodeStateListener localNodeStateListener = mock(NodeStateListener.class);

    localSimulacronNode2.stop();
    try {
      // Since contact points are shuffled, we have a 50% chance that our bad contact point will be
      // hit first. So we retry the scenario a few times if needed.
      DriverConfigLoader loader =
          SessionUtils.configLoaderBuilder()
              .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofHours(1))
              .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofHours(1))
              .build();
      for (int i = 0; i < 10; i++) {
        try (CqlSession localSession =
            (CqlSession)
                SessionUtils.baseBuilder()
                    .addContactPoint(address1)
                    .addContactPoint(address2)
                    .withNodeStateListener(localNodeStateListener)
                    .withConfigLoader(loader)
                    .build()) {

          Metadata metadata = localSession.getMetadata();
          Node localMetadataNode1 = metadata.findNode(address1).orElseThrow(AssertionError::new);
          Node localMetadataNode2 = metadata.findNode(address2).orElseThrow(AssertionError::new);
          if (localMetadataNode2.getState() == NodeState.DOWN) {
            // Stopped node was tried first and marked down, that's our target scenario
            verify(localNodeStateListener, timeout(500)).onDown(localMetadataNode2);
            verify(localNodeStateListener, timeout(500)).onUp(localMetadataNode1);
            verify(localNodeStateListener, timeout(500)).onSessionReady(localSession);
            verifyNoMoreInteractions(localNodeStateListener);
            return;
          } else {
            // Stopped node was not tried
            assertThat(localMetadataNode2).isUnknown();
            verify(localNodeStateListener, timeout(500)).onUp(localMetadataNode1);
            verifyNoMoreInteractions(localNodeStateListener);
          }
        }
        reset(localNodeStateListener);
      }
      fail("Couldn't get the driver to try stopped node first (tried 5 times)");
    } finally {
      localSimulacronNode2.acceptConnections();
    }
  }

  private void expect(NodeStateEvent... expectedEvents) {
    for (NodeStateEvent expected : expectedEvents) {
      try {
        NodeStateEvent actual = stateEvents.poll(10, TimeUnit.SECONDS);
        assertThat(actual).isNotNull();

        // Don't compare events directly: some tests call this method with nodes obtained from
        // another session instance, and nodes are compared by reference.
        assertThat(actual.oldState).isEqualTo(expected.oldState);
        assertThat(actual.newState).isEqualTo(expected.newState);
        assertThat(actual.node.getHostId()).isEqualTo(expected.node.getHostId());
      } catch (InterruptedException e) {
        fail("Interrupted while waiting for event");
      }
    }
  }

  // Generates an endpoint that is not the connect address of one of the nodes in the cluster
  private EndPoint withUnusedPort(EndPoint endPoint) {
    InetSocketAddress address = (InetSocketAddress) endPoint.resolve();
    return new DefaultEndPoint(new InetSocketAddress(address.getAddress(), findAvailablePort()));
  }

  /**
   * Finds an available port in the ephemeral range. This is loosely inspired by Apache MINA's
   * AvailablePortFinder.
   */
  private static synchronized int findAvailablePort() throws RuntimeException {
    // let the system pick an ephemeral port
    try (ServerSocket ss = new ServerSocket(0)) {
      ss.setReuseAddress(true);
      return ss.getLocalPort();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * A load balancing policy that can be told to ignore a node temporarily (the rest of the
   * implementation uses a simple round-robin, non DC-aware shuffle).
   */
  public static class ConfigurableIgnoresPolicy implements LoadBalancingPolicy {

    private final CopyOnWriteArraySet<Node> liveNodes = new CopyOnWriteArraySet<>();
    private final AtomicInteger offset = new AtomicInteger();
    private final Set<Node> ignoredNodes = new CopyOnWriteArraySet<>();

    private volatile DistanceReporter distanceReporter;

    public ConfigurableIgnoresPolicy(
        @SuppressWarnings("unused") DriverContext context,
        @SuppressWarnings("unused") String profileName) {
      // nothing to do
    }

    @Override
    public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
      this.distanceReporter = distanceReporter;
      for (Node node : nodes.values()) {
        liveNodes.add(node);
        distanceReporter.setDistance(node, NodeDistance.LOCAL);
      }
    }

    public void ignore(Node node) {
      if (ignoredNodes.add(node)) {
        liveNodes.remove(node);
        distanceReporter.setDistance(node, NodeDistance.IGNORED);
      }
    }

    public void stopIgnoring(Node node) {
      if (ignoredNodes.remove(node)) {
        distanceReporter.setDistance(node, NodeDistance.LOCAL);
        // There might be a short delay until the node's pool becomes usable, but clients know how
        // to deal with that.
        liveNodes.add(node);
      }
    }

    @NonNull
    @Override
    public Queue<Node> newQueryPlan(@NonNull Request request, @NonNull Session session) {
      Object[] snapshot = liveNodes.toArray();
      Queue<Node> queryPlan = new ConcurrentLinkedQueue<>();
      int start = offset.getAndIncrement(); // Note: offset overflow won't be an issue in tests
      for (int i = 0; i < snapshot.length; i++) {
        queryPlan.add((Node) snapshot[(start + i) % snapshot.length]);
      }
      return queryPlan;
    }

    @Override
    public void onAdd(@NonNull Node node) {
      if (ignoredNodes.contains(node)) {
        distanceReporter.setDistance(node, NodeDistance.IGNORED);
      } else {
        // Setting to a non-ignored distance triggers the session to open a pool, which will in turn
        // set the node UP when the first channel gets opened.
        distanceReporter.setDistance(node, NodeDistance.LOCAL);
      }
    }

    @Override
    public void onUp(@NonNull Node node) {
      if (!ignoredNodes.contains(node)) {
        liveNodes.add(node);
      }
    }

    @Override
    public void onDown(@NonNull Node node) {
      liveNodes.remove(node);
    }

    @Override
    public void onRemove(@NonNull Node node) {
      liveNodes.remove(node);
    }

    @Override
    public void close() {
      // nothing to do
    }
  }
}
