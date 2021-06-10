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
package com.datastax.oss.driver.core.heartbeat;

import static com.datastax.oss.driver.api.testinfra.utils.NodeUtils.waitForDown;
import static com.datastax.oss.driver.api.testinfra.utils.NodeUtils.waitForUp;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.closeConnection;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.noResult;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.request.Options;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.RejectScope;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class HeartbeatIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private static final String QUERY = "select * from foo";
  private BoundNode simulacronNode;

  @Before
  public void setUp() {
    SIMULACRON_RULE.cluster().acceptConnections();
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
    simulacronNode = SIMULACRON_RULE.cluster().getNodes().iterator().next();
  }

  @Test
  public void node_should_go_down_gracefully_when_connection_closed_during_heartbeat() {
    try (CqlSession session = newSession()) {

      Node node = session.getMetadata().getNodes().values().iterator().next();
      assertThat(node.getState()).isEqualTo(NodeState.UP);

      // Stop listening for new connections (so it can't reconnect)
      simulacronNode.rejectConnections(0, RejectScope.UNBIND);

      int heartbeatCount = getHeartbeatsForNode().size();
      // When node receives a heartbeat, close the connection.
      simulacronNode.prime(
          when(Options.INSTANCE)
              .then(closeConnection(DisconnectAction.Scope.CONNECTION, CloseType.DISCONNECT)));

      // Wait for heartbeat and for node to subsequently close its connection.
      waitForDown(node);

      // Should have been a heartbeat received since that's what caused the disconnect.
      assertThat(getHeartbeatsForNode().size()).isGreaterThan(heartbeatCount);
    }
  }

  @Test
  public void should_not_send_heartbeat_during_protocol_initialization() {
    // Configure node to reject startup.
    simulacronNode.rejectConnections(0, RejectScope.REJECT_STARTUP);

    // Try to create a session. Note that the init query timeout is twice the heartbeat interval, so
    // we're sure that at least one heartbeat would be sent if it was not properly disabled during
    // init.
    try (CqlSession ignored = newSession()) {
      fail("Expected session creation to fail");
    } catch (Exception expected) {
      // no heartbeats should have been sent while protocol was initializing, but one OPTIONS
      // message is expected to be sent as part of the initialization process.
      assertThat(getHeartbeatsForNode()).hasSize(1);
    }
  }

  @Test
  public void should_send_heartbeat_on_control_connection() {
    // Ensure we only have the control connection)
    ProgrammaticDriverConfigLoaderBuilder loader =
        SessionUtils.configLoaderBuilder()
            .withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, 0);
    try (CqlSession ignored = newSession(loader)) {
      AtomicInteger heartbeats = countHeartbeatsOnControlConnection();
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> heartbeats.get() > 0);
    }
  }

  @Test
  public void should_send_heartbeat_on_regular_connection() throws InterruptedException {
    // Prime a simple query so we get at least some results
    simulacronNode.prime(when(QUERY).then(PrimeDsl.rows().row("column1", "1", "column2", "2")));

    try (CqlSession session = newSession()) {
      // Make a bunch of queries over two seconds.  This should preempt any heartbeats.
      assertThat(session.execute(QUERY)).hasSize(1);
      final AtomicInteger nonControlHeartbeats = countHeartbeatsOnRegularConnection();
      for (int i = 0; i < 20; i++) {
        assertThat(session.execute(QUERY)).hasSize(1);
        MILLISECONDS.sleep(100);
      }

      // No heartbeats should be sent, except those on the control connection.
      assertThat(nonControlHeartbeats.get()).isZero();

      // Stop querying, heartbeats should be sent again
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> nonControlHeartbeats.get() >= 1);
    }
  }

  @Test
  public void should_send_heartbeat_when_requests_being_written_but_nothing_received()
      throws InterruptedException {
    // Prime a query that will never return a response.
    String noResponseQueryStr = "delay";
    SIMULACRON_RULE.cluster().prime(when(noResponseQueryStr).then(noResult()));

    try (CqlSession session = newSession()) {
      AtomicInteger heartbeats = countHeartbeatsOnRegularConnection();

      for (int i = 0; i < 25; i++) {
        session.executeAsync(noResponseQueryStr);
        session.executeAsync(noResponseQueryStr);
        MILLISECONDS.sleep(100);
      }

      // We should expect at least 2 heartbeats
      assertThat(heartbeats.get()).isGreaterThanOrEqualTo(2);
    }
  }

  @Test
  public void should_close_connection_when_heartbeat_times_out() {
    try (CqlSession session = newSession()) {
      Node node = session.getMetadata().getNodes().values().iterator().next();
      assertThat(node.getState()).isEqualTo(NodeState.UP);

      // Ensure we get some heartbeats and the node remains up.
      AtomicInteger heartbeats = new AtomicInteger();
      simulacronNode.registerQueryListener(
          (n, l) -> heartbeats.incrementAndGet(), true, this::isOptionRequest);

      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> heartbeats.get() >= 2);
      assertThat(node.getState()).isEqualTo(NodeState.UP);

      // configure node to not respond to options request, which should cause a timeout.
      simulacronNode.prime(when(Options.INSTANCE).then(noResult()));
      heartbeats.set(0);

      // wait for heartbeat to be sent.
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> heartbeats.get() >= 1);
      heartbeats.set(0);

      // node should go down because heartbeat was unanswered.
      waitForDown(node);

      // clear prime so now responds to options request again.
      simulacronNode.clearPrimes();

      // wait for node to come up again and ensure heartbeats are successful and node remains up.
      waitForUp(node);

      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .until(() -> heartbeats.get() >= 2);
      assertThat(node.getState()).isEqualTo(NodeState.UP);
    }
  }

  private CqlSession newSession() {
    return newSession(null);
  }

  private CqlSession newSession(ProgrammaticDriverConfigLoaderBuilder loaderBuilder) {
    if (loaderBuilder == null) {
      loaderBuilder = SessionUtils.configLoaderBuilder();
    }
    DriverConfigLoader loader =
        loaderBuilder
            .withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL, Duration.ofSeconds(1))
            .withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT, Duration.ofMillis(500))
            .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(2))
            .withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY, Duration.ofSeconds(1))
            .build();
    return SessionUtils.newSession(SIMULACRON_RULE, loader);
  }

  private AtomicInteger countHeartbeatsOnRegularConnection() {
    return countHeartbeats(true);
  }

  private AtomicInteger countHeartbeatsOnControlConnection() {
    return countHeartbeats(false);
  }

  private AtomicInteger countHeartbeats(boolean regularConnection) {
    SocketAddress controlConnectionAddress = findControlConnectionAddress();
    AtomicInteger count = new AtomicInteger();
    SIMULACRON_RULE
        .cluster()
        .registerQueryListener(
            (n, l) -> count.incrementAndGet(),
            false,
            (l) ->
                isOptionRequest(l)
                    && (regularConnection ^ l.getConnection().equals(controlConnectionAddress)));
    return count;
  }

  private SocketAddress findControlConnectionAddress() {
    List<QueryLog> logs = simulacronNode.getLogs().getQueryLogs();
    for (QueryLog log : logs) {
      if (log.getFrame().message instanceof Register) {
        return log.getConnection();
      }
    }
    throw new AssertionError("Could not find address of control connection");
  }

  private List<QueryLog> getHeartbeatsForNode() {
    return simulacronNode.getLogs().getQueryLogs().stream()
        .filter(l -> l.getQuery().equals("OPTIONS"))
        .collect(Collectors.toList());
  }

  private boolean isOptionRequest(QueryLog l) {
    return l.getQuery().equals("OPTIONS");
  }
}
