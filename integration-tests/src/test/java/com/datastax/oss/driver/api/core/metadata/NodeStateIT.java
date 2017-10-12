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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.session.CqlSession;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.driver.internal.testinfra.cluster.TestConfigLoader;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeConnectionReport;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.RejectScope;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static com.datastax.oss.driver.assertions.Assertions.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;

public class NodeStateIT {

  public @Rule SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(2));

  private NodeStateListener nodeStateListener = Mockito.mock(NodeStateListener.class);
  private InOrder inOrder;

  public @Rule ClusterRule cluster =
      ClusterRule.builder(simulacron)
          .withOptions(
              "connection.pool.local.size = 2",
              "connection.reconnection-policy.max-delay = 1 second")
          .withNodeStateListeners(nodeStateListener)
          .build();

  private InternalDriverContext driverContext;
  private final BlockingQueue<NodeStateEvent> stateEvents = new LinkedBlockingDeque<>();

  private BoundNode simulacronControlNode;
  private BoundNode simulacronRegularNode;
  private DefaultNode metadataControlNode;
  private DefaultNode metadataRegularNode;

  @Before
  public void setup() {
    inOrder = Mockito.inOrder(nodeStateListener);

    driverContext = (InternalDriverContext) cluster.cluster().getContext();
    driverContext.eventBus().register(NodeStateEvent.class, stateEvents::add);

    // Sanity check: the driver should have connected to simulacron
    ConditionChecker.checkThat(
            () ->
                // 1 control connection + 2 pooled connections per node
                simulacron.cluster().getActiveConnections() == 5)
        .as("Connections established")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();

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

    Map<InetSocketAddress, Node> nodesMetadata = cluster.cluster().getMetadata().getNodes();
    metadataControlNode =
        (DefaultNode) nodesMetadata.get(simulacronControlNode.inetSocketAddress());
    metadataRegularNode =
        (DefaultNode) nodesMetadata.get(simulacronRegularNode.inetSocketAddress());

    // ClusterRule uses all nodes as contact points, so we only get onUp notifications for them (no
    // onAdd)
    inOrder.verify(nodeStateListener, timeout(100)).onUp(metadataControlNode);
    inOrder.verify(nodeStateListener, timeout(100)).onUp(metadataRegularNode);
  }

  @After
  public void teardown() {
    Mockito.reset(nodeStateListener);
  }

  @Test
  public void should_report_connections_for_healthy_nodes() {
    ConditionChecker.checkThat(
            () -> {
              assertThat(metadataControlNode).isUp().hasOpenConnections(3).isNotReconnecting();
              assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting();
            })
        .as("Node metadata up-to-date")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
  }

  @Test
  public void should_keep_regular_node_up_when_still_one_connection() {
    simulacronRegularNode.rejectConnections(0, RejectScope.UNBIND);
    NodeConnectionReport report = simulacronRegularNode.getConnections();
    simulacron.cluster().closeConnection(report.getConnections().get(0), CloseType.DISCONNECT);

    ConditionChecker.checkThat(
            () -> assertThat(metadataRegularNode).isUp().hasOpenConnections(1).isReconnecting())
        .as("Reconnection started")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, never()).onDown(metadataRegularNode);
  }

  @Test
  public void should_mark_regular_node_down_when_no_more_connections() {
    simulacronRegularNode.stop();

    ConditionChecker.checkThat(
            () -> assertThat(metadataRegularNode).isDown().hasOpenConnections(0).isReconnecting())
        .as("Node going down")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();

    expect(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, metadataRegularNode));
    inOrder.verify(nodeStateListener, timeout(100)).onDown(metadataRegularNode);
  }

  @Test
  public void should_mark_control_node_down_when_control_connection_is_last_connection_and_dies() {
    simulacronControlNode.rejectConnections(0, RejectScope.UNBIND);

    // Identify the control connection and close the two other ones
    SocketAddress controlAddress = driverContext.controlConnection().channel().localAddress();
    NodeConnectionReport report = simulacronControlNode.getConnections();
    for (SocketAddress address : report.getConnections()) {
      if (!address.equals(controlAddress)) {
        simulacron.cluster().closeConnection(address, CloseType.DISCONNECT);
      }
    }
    ConditionChecker.checkThat(
            () -> assertThat(metadataControlNode).isUp().hasOpenConnections(1).isReconnecting())
        .as("Control node lost its non-control connections")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, never()).onDown(metadataRegularNode);

    simulacron.cluster().closeConnection(controlAddress, CloseType.DISCONNECT);
    ConditionChecker.checkThat(
            () -> assertThat(metadataControlNode).isDown().hasOpenConnections(0).isReconnecting())
        .as("Control node going down")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onDown(metadataControlNode);

    expect(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, metadataControlNode));
  }

  @Test
  public void should_bring_node_back_up_when_reconnection_succeeds() {
    simulacronRegularNode.stop();

    ConditionChecker.checkThat(
            () -> assertThat(metadataRegularNode).isDown().hasOpenConnections(0).isReconnecting())
        .as("Node going down")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onDown(metadataRegularNode);

    simulacronRegularNode.acceptConnections();

    ConditionChecker.checkThat(
            () -> assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting())
        .as("Connections re-established")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onUp(metadataRegularNode);

    expect(
        NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, metadataRegularNode),
        NodeStateEvent.changed(NodeState.DOWN, NodeState.UP, metadataRegularNode));
  }

  @Test
  public void should_apply_up_and_down_topology_events_when_ignored() {
    driverContext
        .loadBalancingPolicyWrapper()
        .setDistance(metadataRegularNode, NodeDistance.IGNORED);
    ConditionChecker.checkThat(
            () ->
                assertThat(metadataRegularNode)
                    .isUp()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting())
        .as("Driver closed all connections to ignored node")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();

    driverContext
        .eventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getConnectAddress()));
    ConditionChecker.checkThat(
            () ->
                assertThat(metadataRegularNode)
                    .isDown()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting())
        .as("SUGGEST_DOWN event applied")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onDown(metadataRegularNode);

    driverContext.eventBus().fire(TopologyEvent.suggestUp(metadataRegularNode.getConnectAddress()));
    ConditionChecker.checkThat(
            () ->
                assertThat(metadataRegularNode)
                    .isUp()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting())
        .as("SUGGEST_UP event applied")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onUp(metadataRegularNode);
  }

  @Test
  public void should_ignore_down_topology_event_when_still_connected() throws InterruptedException {
    driverContext
        .eventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getConnectAddress()));
    TimeUnit.MILLISECONDS.sleep(200);
    assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting();
  }

  @Test
  public void should_force_immediate_reconnection_when_up_topology_event()
      throws InterruptedException {
    // This test requires a longer reconnection interval, so create a separate driver instance
    try (Cluster<CqlSession> localCluster =
        ClusterUtils.newCluster(
            simulacron,
            "connection.reconnection-policy.base-delay = 1 hour",
            "connection.reconnection-policy.max-delay = 1 hour")) {
      localCluster.connect();

      NodeStateListener localNodeStateListener = Mockito.mock(NodeStateListener.class);
      localCluster.register(localNodeStateListener);

      BoundNode localSimulacronNode = simulacron.cluster().getNodes().iterator().next();
      assertThat(localSimulacronNode).isNotNull();

      DefaultNode localMetadataNode =
          (DefaultNode)
              localCluster.getMetadata().getNodes().get(localSimulacronNode.inetSocketAddress());

      localSimulacronNode.stop();

      ConditionChecker.checkThat(
              () -> assertThat(localMetadataNode).isDown().hasOpenConnections(0).isReconnecting())
          .as("Node going down")
          .before(10, TimeUnit.SECONDS)
          .becomesTrue();
      Mockito.verify(localNodeStateListener, timeout(100)).onDown(localMetadataNode);

      expect(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, localMetadataNode));

      localSimulacronNode.acceptConnections();
      ((InternalDriverContext) localCluster.getContext())
          .eventBus()
          .fire(TopologyEvent.suggestUp(localMetadataNode.getConnectAddress()));

      ConditionChecker.checkThat(() -> assertThat(localMetadataNode).isUp().isNotReconnecting())
          .as("Node coming back up")
          .before(10, TimeUnit.SECONDS)
          .becomesTrue();
      Mockito.verify(localNodeStateListener, timeout(100)).onUp(localMetadataNode);

      expect(NodeStateEvent.changed(NodeState.DOWN, NodeState.UP, localMetadataNode));
    }
  }

  @Test
  public void should_force_down_when_not_ignored() throws InterruptedException {
    driverContext.eventBus().fire(TopologyEvent.forceDown(metadataRegularNode.getConnectAddress()));
    ConditionChecker.checkThat(
            () ->
                assertThat(metadataRegularNode)
                    .isForcedDown()
                    .hasOpenConnections(0)
                    .isNotReconnecting())
        .as("Node forced down")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onDown(metadataRegularNode);

    // Should ignore up/down topology events while forced down
    driverContext.eventBus().fire(TopologyEvent.suggestUp(metadataRegularNode.getConnectAddress()));
    TimeUnit.MILLISECONDS.sleep(200);
    assertThat(metadataRegularNode).isForcedDown();

    driverContext
        .eventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getConnectAddress()));
    TimeUnit.MILLISECONDS.sleep(200);
    assertThat(metadataRegularNode).isForcedDown();

    // Should only come back up on a FORCE_UP event
    driverContext.eventBus().fire(TopologyEvent.forceUp(metadataRegularNode.getConnectAddress()));
    ConditionChecker.checkThat(
            () -> assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting())
        .as("Node forced back up")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onUp(metadataRegularNode);
  }

  @Test
  public void should_force_down_when_ignored() throws InterruptedException {
    driverContext
        .loadBalancingPolicyWrapper()
        .setDistance(metadataRegularNode, NodeDistance.IGNORED);
    driverContext.eventBus().fire(TopologyEvent.forceDown(metadataRegularNode.getConnectAddress()));
    ConditionChecker.checkThat(
            () ->
                assertThat(metadataRegularNode)
                    .isForcedDown()
                    .hasOpenConnections(0)
                    .isNotReconnecting())
        .as("Node forced down")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onDown(metadataRegularNode);

    // Should ignore up/down topology events while forced down
    driverContext.eventBus().fire(TopologyEvent.suggestUp(metadataRegularNode.getConnectAddress()));
    TimeUnit.MILLISECONDS.sleep(200);
    assertThat(metadataRegularNode).isForcedDown();

    driverContext
        .eventBus()
        .fire(TopologyEvent.suggestDown(metadataRegularNode.getConnectAddress()));
    TimeUnit.MILLISECONDS.sleep(200);
    assertThat(metadataRegularNode).isForcedDown();

    // Should only come back up on a FORCE_UP event, will not reopen connections since it is still
    // ignored
    driverContext.eventBus().fire(TopologyEvent.forceUp(metadataRegularNode.getConnectAddress()));
    ConditionChecker.checkThat(
            () ->
                assertThat(metadataRegularNode)
                    .isUp()
                    .isIgnored()
                    .hasOpenConnections(0)
                    .isNotReconnecting())
        .as("Node forced back up")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();
    inOrder.verify(nodeStateListener, timeout(100)).onUp(metadataRegularNode);

    driverContext.loadBalancingPolicyWrapper().setDistance(metadataRegularNode, NodeDistance.LOCAL);
  }

  @Test
  public void should_signal_non_contact_points_as_added() {
    // Since we need to observe the behavior of non-contact points, build a dedicated cluster with
    // just one contact point.
    Iterator<InetSocketAddress> contactPoints = simulacron.getContactPoints().iterator();
    InetSocketAddress address1 = contactPoints.next();
    InetSocketAddress address2 = contactPoints.next();
    NodeStateListener localNodeStateListener = Mockito.mock(NodeStateListener.class);
    try (Cluster localCluster =
        Cluster.builder()
            .addContactPoint(address1)
            .addNodeStateListeners(localNodeStateListener)
            .withConfigLoader(
                new TestConfigLoader(
                    "connection.reconnection-policy.base-delay = 1 hour",
                    "connection.reconnection-policy.max-delay = 1 hour"))
            .build()) {

      Map<InetSocketAddress, Node> nodes = localCluster.getMetadata().getNodes();
      Node localMetadataNode1 = nodes.get(address1);
      Node localMetadataNode2 = nodes.get(address2);

      // Successful contact point goes to up directly
      Mockito.verify(localNodeStateListener, timeout(100)).onUp(localMetadataNode1);
      // Non-contact point only added since we don't have a connection or events for it yet
      Mockito.verify(localNodeStateListener, timeout(100)).onAdd(localMetadataNode2);

      localCluster.connect();

      // Non-contact point now has a connection opened => up
      Mockito.verify(localNodeStateListener, timeout(100)).onUp(localMetadataNode2);
    }
  }

  @Test
  public void should_remove_invalid_contact_point() {
    // Initialize the driver with 1 wrong address and 1 valid address
    InetSocketAddress wrongContactPoint = unusedAddress();

    Iterator<InetSocketAddress> contactPoints = simulacron.getContactPoints().iterator();
    InetSocketAddress address1 = contactPoints.next();
    InetSocketAddress address2 = contactPoints.next();
    NodeStateListener localNodeStateListener = Mockito.mock(NodeStateListener.class);

    try (Cluster<CqlSession> localCluster =
        Cluster.builder()
            .addContactPoint(address1)
            .addContactPoint(wrongContactPoint)
            .addNodeStateListeners(localNodeStateListener)
            .withConfigLoader(
                new TestConfigLoader(
                    "connection.reconnection-policy.base-delay = 1 hour",
                    "connection.reconnection-policy.max-delay = 1 hour"))
            .build()) {

      Map<InetSocketAddress, Node> nodes = localCluster.getMetadata().getNodes();
      assertThat(nodes).doesNotContainKey(wrongContactPoint);
      Node localMetadataNode1 = nodes.get(address1);
      Node localMetadataNode2 = nodes.get(address2);

      // The order of the calls is not deterministic because contact points are shuffled, but it
      // does not matter here since Mockito.verify does not enforce order.
      Mockito.verify(localNodeStateListener, timeout(100))
          .onRemove(new DefaultNode(wrongContactPoint));
      Mockito.verify(localNodeStateListener, timeout(100)).onUp(localMetadataNode1);
      Mockito.verify(localNodeStateListener, timeout(100)).onAdd(localMetadataNode2);

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

    NodeStateListener localNodeStateListener = Mockito.mock(NodeStateListener.class);

    localSimulacronNode2.stop();
    try {
      // Since contact points are shuffled, we have a 50% chance that our bad contact point will be
      // hit first. So we retry the scenario a few times if needed.
      for (int i = 0; i < 10; i++) {
        try (Cluster localCluster =
            Cluster.builder()
                .addContactPoint(address1)
                .addContactPoint(address2)
                .addNodeStateListeners(localNodeStateListener)
                .withConfigLoader(
                    new TestConfigLoader(
                        "connection.reconnection-policy.base-delay = 1 hour",
                        "connection.reconnection-policy.max-delay = 1 hour"))
                .build()) {

          Mockito.verify(localNodeStateListener).onRegister(localCluster);

          Map<InetSocketAddress, Node> nodes = localCluster.getMetadata().getNodes();
          Node localMetadataNode1 = nodes.get(address1);
          Node localMetadataNode2 = nodes.get(address2);
          if (localMetadataNode2.getState() == NodeState.DOWN) {
            // Stopped node was tried first and marked down, that's our target scenario
            Mockito.verify(localNodeStateListener, timeout(100)).onDown(localMetadataNode2);
            Mockito.verify(localNodeStateListener, timeout(100)).onUp(localMetadataNode1);
            Mockito.verifyNoMoreInteractions(localNodeStateListener);
            return;
          } else {
            // Stopped node was not tried
            assertThat(localMetadataNode2).isUnknown();
            Mockito.verify(localNodeStateListener, timeout(100)).onUp(localMetadataNode1);
            Mockito.verifyNoMoreInteractions(localNodeStateListener);
          }
        }
        Mockito.reset(localNodeStateListener);
      }
      fail("Couldn't get the driver to try stopped node first (tried 5 times)");
    } finally {
      localSimulacronNode2.acceptConnections();
    }
  }

  @Test
  public void should_call_onRegister_and_onUnregister_implicitly() {
    NodeStateListener localNodeStateListener = Mockito.mock(NodeStateListener.class);
    Cluster localClusterRef;
    // onRegister should be called implicitly when added as a listener on builder.
    try (Cluster localCluster =
        Cluster.builder()
            .addContactPoints(simulacron.getContactPoints())
            .addNodeStateListeners(localNodeStateListener)
            .build()) {
      Mockito.verify(localNodeStateListener).onRegister(localCluster);
      localClusterRef = localCluster;
    }
    // onUnregister should be called implicitly when cluster is closed.
    Mockito.verify(localNodeStateListener).onUnregister(localClusterRef);
  }

  @Test
  public void should_call_onRegister_and_onUnregister_when_used() {
    NodeStateListener localNodeStateListener = Mockito.mock(NodeStateListener.class);
    cluster.cluster().register(localNodeStateListener);
    Mockito.verify(localNodeStateListener, timeout(1000)).onRegister(cluster.cluster());
    cluster.cluster().unregister(localNodeStateListener);
    Mockito.verify(localNodeStateListener, timeout(1000)).onUnregister(cluster.cluster());
  }

  private void expect(NodeStateEvent... expectedEvents) {
    for (NodeStateEvent expected : expectedEvents) {
      try {
        NodeStateEvent actual = stateEvents.poll(10, TimeUnit.SECONDS);
        assertThat(actual).isEqualTo(expected);
      } catch (InterruptedException e) {
        fail("Interrupted while waiting for event");
      }
    }
  }

  // Generates a socket address that is not the connect address of one of the nodes in the cluster
  private InetSocketAddress unusedAddress() {
    try {
      byte[] bytes = new byte[] {127, 0, 1, 2};
      for (int i = 0; i < 100; i++) {
        bytes[3] += 1;
        InetSocketAddress address = new InetSocketAddress(InetAddress.getByAddress(bytes), 9043);
        if (!simulacron.getContactPoints().contains(address)) {
          return address;
        }
      }
    } catch (UnknownHostException e) {
      fail("unexpected error", e);
    }
    fail("Could not find unused address");
    return null; // never reached
  }
}
