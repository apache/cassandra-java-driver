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
package com.datastax.oss.driver.api.core.heartbeat;

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.BoundNode;
import java.net.SocketAddress;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

/** This test is separate from {@link HeartbeatIT} because it can't be parallelized. */
public class HeartbeatDisabledIT {

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

  private SocketAddress controlConnection;

  @Before
  public void setUp() {
    simulacron.cluster().clearLogs();
    simulacron.cluster().clearPrimes(true);

    Optional<BoundNode> controlNodeOption =
        simulacron
            .cluster()
            .getNodes()
            .stream()
            .filter(n -> n.getActiveConnections() == 1)
            .findFirst();

    if (controlNodeOption.isPresent()) {
      BoundNode controlNode = controlNodeOption.get();
      controlConnection = controlNode.getConnections().getConnections().get(0);
    } else {
      fail("Control node not found");
    }
  }

  @Test
  public void should_not_send_heartbeat_when_disabled() throws InterruptedException {
    // Disable heartbeats entirely, wait longer than the default timeout and make sure we didn't receive any
    try (Cluster<CqlSession> cluster =
        ClusterUtils.newCluster(simulacron, "connection.heartbeat.interval = 0 second")) {
      cluster.connect();
      AtomicInteger heartbeats = registerHeartbeatListener();
      SECONDS.sleep(35);

      assertThat(heartbeats.get()).isZero();
    }
  }

  private AtomicInteger registerHeartbeatListener() {
    AtomicInteger nonControlHeartbeats = new AtomicInteger();
    simulacron
        .cluster()
        .registerQueryListener(
            (n, l) -> nonControlHeartbeats.incrementAndGet(),
            false,
            (l) -> l.getQuery().equals("OPTIONS") && !l.getConnection().equals(controlConnection));
    return nonControlHeartbeats;
  }
}
