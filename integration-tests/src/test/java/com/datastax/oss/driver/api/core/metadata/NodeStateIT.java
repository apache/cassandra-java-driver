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
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.metadata.NodeStateEvent;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeConnectionReport;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.RejectScope;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class NodeStateIT {

  public @Rule SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(2));

  public @Rule ClusterRule cluster =
      ClusterRule.builder(simulacron)
          .withOptions(
              "connection.pool.local.size = 2",
              "connection.reconnection-policy.max-delay = 1 second")
          .build();

  private InternalDriverContext driverContext;
  private final BlockingQueue<NodeStateEvent> stateEvents = new LinkedBlockingDeque<>();

  private BoundNode simulacronControlNode;
  private BoundNode simulacronRegularNode;
  private DefaultNode metadataControlNode;
  private DefaultNode metadataRegularNode;

  @Before
  public void setup() {
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

    simulacron.cluster().closeConnection(controlAddress, CloseType.DISCONNECT);
    ConditionChecker.checkThat(
            () -> assertThat(metadataControlNode).isDown().hasOpenConnections(0).isReconnecting())
        .as("Control node going down")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();

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

    simulacronRegularNode.acceptConnections();

    ConditionChecker.checkThat(
            () -> assertThat(metadataRegularNode).isUp().hasOpenConnections(2).isNotReconnecting())
        .as("Connections re-established")
        .before(10, TimeUnit.SECONDS)
        .becomesTrue();

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
    try (Cluster localCluster =
        ClusterUtils.newCluster(
            simulacron,
            "connection.reconnection-policy.base-delay = 1 hour",
            "connection.reconnection-policy.max-delay = 1 hour")) {
      localCluster.connect();

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

      expect(NodeStateEvent.changed(NodeState.UP, NodeState.DOWN, localMetadataNode));

      localSimulacronNode.acceptConnections();
      ((InternalDriverContext) localCluster.getContext())
          .eventBus()
          .fire(TopologyEvent.suggestUp(localMetadataNode.getConnectAddress()));

      ConditionChecker.checkThat(() -> assertThat(localMetadataNode).isUp().isNotReconnecting())
          .as("Node coming back up")
          .before(10, TimeUnit.SECONDS)
          .becomesTrue();

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

    driverContext.loadBalancingPolicyWrapper().setDistance(metadataRegularNode, NodeDistance.LOCAL);
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
}
