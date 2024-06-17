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
package com.datastax.oss.driver.core;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.connection.ConstantReconnectionPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.RejectScope;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(ParallelizableTests.class)
public class ConnectIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(2));

  @Before
  public void setup() {
    SIMULACRON_RULE.cluster().acceptConnections();
    SIMULACRON_RULE
        .cluster()
        .prime(
            // Absolute minimum for a working schema metadata (we just want to check that it gets
            // loaded at startup).
            when("SELECT * FROM system_schema.keyspaces")
                .then(rows().row("keyspace_name", "system").row("keyspace_name", "test")));
  }

  @Test
  public void should_fail_fast_if_contact_points_unreachable_and_reconnection_disabled() {
    // Given
    SIMULACRON_RULE.cluster().rejectConnections(0, RejectScope.STOP);

    // When
    Throwable t = catchThrowable(() -> SessionUtils.newSession(SIMULACRON_RULE));

    // Then
    assertThat(t)
        .isInstanceOf(AllNodesFailedException.class)
        .hasMessageContaining(
            "Could not reach any contact point, make sure you've provided valid addresses");
  }

  @Test
  public void should_wait_for_contact_points_if_reconnection_enabled() throws Exception {
    // Given
    SIMULACRON_RULE.cluster().rejectConnections(0, RejectScope.STOP);

    // When
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.RECONNECT_ON_INIT, true)
            .withClass(
                DefaultDriverOption.RECONNECTION_POLICY_CLASS, InitOnlyReconnectionPolicy.class)
            // Use a short delay so we don't have to wait too long:
            .withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY, Duration.ofMillis(500))
            .build();
    CompletableFuture<? extends Session> sessionFuture =
        newSessionAsync(loader).toCompletableFuture();
    // wait a bit to ensure we have a couple of reconnections, otherwise we might race and allow
    // reconnections before the initial attempt
    TimeUnit.SECONDS.sleep(2);

    // Then
    assertThat(sessionFuture).isNotCompleted();

    // When
    SIMULACRON_RULE.cluster().acceptConnections();

    // Then this doesn't throw
    try (Session session = sessionFuture.get(30, TimeUnit.SECONDS)) {
      assertThat(session.getMetadata().getKeyspaces()).containsKey(CqlIdentifier.fromCql("test"));
    }
  }

  /**
   * Test for JAVA-1948. This ensures that when the LBP initialization fails that any connections
   * are cleaned up appropriately.
   */
  @Test
  public void should_cleanup_on_lbp_init_failure() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .without(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER)
            .build();
    assertThatThrownBy(
            () ->
                CqlSession.builder()
                    .addContactEndPoints(SIMULACRON_RULE.getContactPoints())
                    .withConfigLoader(loader)
                    .build())
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "Since you provided explicit contact points, the local DC must be explicitly set");
    // One second should be plenty of time for connections to close server side
    await()
        .atMost(1, TimeUnit.SECONDS)
        .until(() -> SIMULACRON_RULE.cluster().getConnections().getConnections().isEmpty());
  }

  /**
   * Test for JAVA-2177. This ensures that even if the first attempted contact point is unreachable,
   * its distance is set to LOCAL and reconnections are scheduled.
   */
  @Test
  public void should_mark_unreachable_contact_points_as_local_and_schedule_reconnections() {
    // Reject connections only on one node
    BoundCluster boundCluster = SIMULACRON_RULE.cluster();
    boundCluster.node(0).rejectConnections(0, RejectScope.STOP);

    try (CqlSession session = SessionUtils.newSession(SIMULACRON_RULE)) {
      Map<UUID, Node> nodes = session.getMetadata().getNodes();
      // Node states are updated asynchronously, so guard against race conditions
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                // Before JAVA-2177, this would fail every other time because if the node was tried
                // first for the initial connection, it was marked down and not passed to
                // LBP.init(), and therefore stayed at distance IGNORED.
                Node node0 = nodes.get(boundCluster.node(0).getHostId());
                assertThat(node0.getState()).isEqualTo(NodeState.DOWN);
                assertThat(node0.getDistance()).isEqualTo(NodeDistance.LOCAL);
                assertThat(node0.getOpenConnections()).isEqualTo(0);
                assertThat(node0.isReconnecting()).isTrue();

                Node node1 = nodes.get(boundCluster.node(1).getHostId());
                assertThat(node1.getState()).isEqualTo(NodeState.UP);
                assertThat(node1.getDistance()).isEqualTo(NodeDistance.LOCAL);
                assertThat(node1.getOpenConnections()).isEqualTo(2); // control + regular
                assertThat(node1.isReconnecting()).isFalse();
              });
    }
  }

  @SuppressWarnings("unchecked")
  private CompletionStage<? extends Session> newSessionAsync(DriverConfigLoader loader) {
    return SessionUtils.baseBuilder()
        .addContactEndPoints(ConnectIT.SIMULACRON_RULE.getContactPoints())
        .withConfigLoader(loader)
        .buildAsync();
  }

  /**
   * Test policy that fails if a "runtime" control connection schedule is requested.
   *
   * <p>This is just to check that {@link #newControlConnectionSchedule(boolean)} is called with the
   * correct boolean parameter.
   */
  public static class InitOnlyReconnectionPolicy extends ConstantReconnectionPolicy {

    public InitOnlyReconnectionPolicy(DriverContext context) {
      super(context);
    }

    @NonNull
    @Override
    public ReconnectionSchedule newControlConnectionSchedule(boolean isInitialConnection) {
      if (isInitialConnection) {
        return super.newControlConnectionSchedule(true);
      } else {
        throw new UnsupportedOperationException(
            "should not be called with isInitialConnection==false");
      }
    }
  }
}
