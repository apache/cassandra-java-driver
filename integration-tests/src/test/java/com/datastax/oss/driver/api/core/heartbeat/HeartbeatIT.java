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
package com.datastax.oss.driver.api.core.heartbeat;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.LongTests;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.request.Options;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.RejectScope;
import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static com.datastax.oss.driver.api.testinfra.utils.ConditionChecker.checkThat;
import static com.datastax.oss.driver.api.testinfra.utils.NodeUtils.waitForDown;
import static com.datastax.oss.driver.api.testinfra.utils.NodeUtils.waitForUp;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.closeConnection;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noResult;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class HeartbeatIT {

  @ClassRule
  public static SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(2));

  @Rule
  public ClusterRule cluster =
      ClusterRule.builder(simulacron)
          .withDefaultSession(false)
          .withOptions(
              "connection.heartbeat.interval = 1 second",
              "connection.heartbeat.timeout = 500 milliseconds",
              "connection.init-query-timeout = 2 seconds",
              "connection.reconnection-policy.max-delay = 1 second")
          .build();

  private static final String queryStr = "select * from foo";

  private BoundNode nonControlNode;
  private BoundNode controlNode;
  private SocketAddress controlConnection;

  @Before
  public void setUp() {
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);
    Optional<BoundNode> nonControlNodeOption =
        simulacron
            .cluster()
            .getNodes()
            .stream()
            .filter(n -> n.getActiveConnections() == 0)
            .findFirst();

    if (nonControlNodeOption.isPresent()) {
      nonControlNode = nonControlNodeOption.get();
    } else {
      fail("Non-control node not found");
    }

    Optional<BoundNode> controlNodeOption =
        simulacron
            .cluster()
            .getNodes()
            .stream()
            .filter(n -> n.getActiveConnections() == 1)
            .findFirst();

    if (controlNodeOption.isPresent()) {
      controlNode = controlNodeOption.get();
      controlConnection = controlNode.getConnections().getConnections().get(0);
    } else {
      fail("Control node not found");
    }
  }

  @Test
  public void node_should_go_down_gracefully_when_connection_closed_during_heartbeat()
      throws InterruptedException {
    // Create session to initialize pools.
    cluster.cluster().connect();

    // Node should be up.
    Node node = cluster.cluster().getMetadata().getNodes().get(nonControlNode.inetSocketAddress());
    assertThat(node.getState()).isEqualTo(NodeState.UP);

    // Stop listening for new connections (so it can't reconnect)
    nonControlNode.rejectConnections(0, RejectScope.UNBIND);

    int heartbeatCount = getHeartbeatsForNode(nonControlNode).size();
    // When node receives a heartbeat, close the connection.
    nonControlNode.prime(
        when(Options.INSTANCE)
            .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

    // Wait for heartbeat and for node to subsequently close it's connection.
    waitForDown(node);

    // Should have been a heartbeat received since that's what caused the disconnect.
    assertThat(getHeartbeatsForNode(nonControlNode).size()).isGreaterThan(heartbeatCount);
  }

  private static final Predicate<QueryLog> optionsRequest = (l) -> l.getQuery().equals("OPTIONS");

  @Test
  public void should_not_send_heartbeat_during_protocol_initialization()
      throws InterruptedException {
    // Configure node to reject startup.
    nonControlNode.rejectConnections(0, RejectScope.REJECT_STARTUP);
    Node node = cluster.cluster().getMetadata().getNodes().get(nonControlNode.inetSocketAddress());

    // Create session to initialize pools.
    cluster.cluster().connect();

    // wait for node to go down as result of startup failing.
    waitForDown(node);

    // no heartbeats should have been sent while protocol was initializing.
    assertThat(getHeartbeatsForNode(nonControlNode)).isEmpty();

    // node should be down since there were no successful connections.
    assertThat(node.getState()).isEqualTo(NodeState.DOWN);

    // start accepting connections again.
    nonControlNode.acceptConnections();

    // listen for heartbeats on node.
    AtomicInteger heartbeats = new AtomicInteger();
    nonControlNode.registerQueryListener(
        (n, l) -> heartbeats.incrementAndGet(), true, optionsRequest);

    // wait a heartbeat to be sent (indicating node is up and sending heartbeats)
    checkThat(() -> heartbeats.get() > 0).every(100).becomesTrue();
    assertThat(getHeartbeatsForNode(nonControlNode)).isNotEmpty();
    assertThat(node.getState()).isEqualTo(NodeState.UP);
  }

  @Test
  public void should_send_heartbeat_on_interval() throws InterruptedException {
    // Prime a simple query so we get at least some results
    simulacron
        .cluster()
        .prime(when(queryStr).then(PrimeDsl.rows().row("column1", "1", "column2", "2")));

    // listen for heartbeats on node.
    AtomicInteger controlNodeHeartbeats = new AtomicInteger();
    controlNode.registerQueryListener(
        (n, l) -> controlNodeHeartbeats.incrementAndGet(), true, optionsRequest);

    // Ensure heartbeats are received on control node, even when no sessions are present.
    checkThat(() -> controlNodeHeartbeats.get() > 0).becomesTrue();

    // Create session to initialize pools.
    Session session = cluster.cluster().connect();

    // Ensure we get a heartbeat after a second.
    AtomicInteger heartbeats = new AtomicInteger();
    nonControlNode.registerQueryListener(
        (n, l) -> heartbeats.incrementAndGet(), true, optionsRequest);

    checkThat(() -> heartbeats.get() > 0).becomesTrue();

    // count all options received not from control connection.
    AtomicInteger nonControlHeartbeats = null;

    // Make a bunch of queries over two seconds.  This should preempt any heartbeats.
    for (int i = 0; i < 20; i++) {
      assertThat(session.execute(queryStr)).hasSize(1);
      assertThat(session.execute(queryStr)).hasSize(1);

      // after first write, start counting number of heartbeats.
      if (i == 0) {
        nonControlHeartbeats = registerHeartbeatListener();
      }
      MILLISECONDS.sleep(100);
    }

    // No heartbeats should be sent, except those on the control connection.
    assertThat(nonControlHeartbeats.get()).isZero();

    // Wait for 2 more heartbeats to be sent (one on each node).
    AtomicInteger fNonControlHeartbeats = nonControlHeartbeats;
    checkThat(() -> fNonControlHeartbeats.get() >= 2).becomesTrue();
  }

  @Test
  public void should_send_heartbeat_when_requests_being_written_but_nothing_received()
      throws InterruptedException {
    // Prime a query that will never return a response.
    String noResponseQueryStr = "delay";
    simulacron.cluster().prime(when(noResponseQueryStr).then(noResult()));

    AtomicInteger heartbeats = registerHeartbeatListener();

    // Send requests over 2.5 seconds.
    Session session = cluster.cluster().connect();
    for (int i = 0; i < 25; i++) {
      session.executeAsync(noResponseQueryStr);
      session.executeAsync(noResponseQueryStr);
      MILLISECONDS.sleep(100);
    }

    // We should expect at least 4 heartbeats (2 from each node's connection)
    assertThat(heartbeats.get()).isGreaterThanOrEqualTo(4);
  }

  @Test
  public void should_close_connection_when_heartbeat_times_out() {
    cluster.cluster().connect();

    Node node = cluster.cluster().getMetadata().getNodes().get(nonControlNode.inetSocketAddress());

    // Node should be up.
    assertThat(node.getState()).isEqualTo(NodeState.UP);

    // Ensure we get some heartbeats and the node remains up.
    AtomicInteger heartbeats = new AtomicInteger();
    nonControlNode.registerQueryListener(
        (n, l) -> heartbeats.incrementAndGet(), true, optionsRequest);

    checkThat(() -> heartbeats.get() >= 2).becomesTrue();

    // configure node to not respond to options request, which should cause a timeout.
    nonControlNode.prime(when(Options.INSTANCE).then(noResult()));
    heartbeats.set(0);

    // wait for heartbeat to be sent.
    checkThat(() -> heartbeats.get() == 1).becomesTrue();
    heartbeats.set(0);

    // node should go down because heartbeat was not set.
    waitForDown(node);

    // clear prime so now responds to options request again.
    nonControlNode.clearPrimes();

    // wait for node to come up again and ensure heartbeats are successful and node remains up.
    waitForUp(node);

    checkThat(() -> heartbeats.get() >= 2).becomesTrue();
    assertThat(node.getState()).isEqualTo(NodeState.UP);
  }

  @Test
  @Category(LongTests.class)
  public void should_not_send_heartbeat_when_disabled() throws InterruptedException {
    // Disable heartbeats entirely, wait longer than the default timeout and make sure we didn't receive any
    try (Cluster cluster =
        ClusterUtils.newCluster(simulacron, "connection.heartbeat.interval = 0 second")) {
      cluster.connect();
      AtomicInteger heartbeats = registerHeartbeatListener();
      SECONDS.sleep(35);

      assertThat(heartbeats.get()).isZero();
    }
  }

  /**
   * Registers a query listener that increments the returned {@link AtomicInteger} whenever a
   * heartbeat is received on the non-control connection.
   *
   * @return integer that represents current count of heartbeats.
   */
  private AtomicInteger registerHeartbeatListener() {
    AtomicInteger nonControlHeartbeats = new AtomicInteger();
    simulacron
        .cluster()
        .registerQueryListener(
            (n, l) -> nonControlHeartbeats.incrementAndGet(),
            false,
            (l) -> optionsRequest.test(l) && !l.getConnection().equals(controlConnection));
    return nonControlHeartbeats;
  }

  private List<QueryLog> getHeartbeatsForNode(BoundNode node) {
    return node.getLogs()
        .getQueryLogs()
        .stream()
        .filter(l -> l.getQuery().equals("OPTIONS"))
        .collect(Collectors.toList());
  }
}
