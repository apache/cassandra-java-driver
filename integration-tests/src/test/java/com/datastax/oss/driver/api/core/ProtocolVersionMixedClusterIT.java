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
package com.datastax.oss.driver.api.core;

import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.testinfra.cluster.TestConfigLoader;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.BoundTopic;
import java.net.InetSocketAddress;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/**
 * Covers protocol re-negotiation with a mixed cluster: if, after the initial connection and the
 * first node list refresh, we find out that some nodes only support a lower version, reconnect the
 * control connection immediately.
 */
public class ProtocolVersionMixedClusterIT {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void should_downgrade_if_peer_does_not_support_negotiated_version() {
    try (BoundCluster simulacron = mixedVersions("3.0.0", "2.2.0", "2.1.0");
        BoundNode contactPoint = simulacron.node(0);
        Cluster<CqlSession> cluster =
            Cluster.builder()
                .addContactPoint(contactPoint.inetSocketAddress())
                .withConfigLoader(new TestConfigLoader("metadata.schema.enabled = false"))
                .build()) {

      InternalDriverContext context = (InternalDriverContext) cluster.getContext();
      assertThat(context.protocolVersion()).isEqualTo(CoreProtocolVersion.V3);

      // Find out which node became the control node after the reconnection (not necessarily node 0)
      InetSocketAddress controlAddress =
          (InetSocketAddress) context.controlConnection().channel().remoteAddress();
      BoundNode currentControlNode = null;
      for (BoundNode node : simulacron.getNodes()) {
        if (node.inetSocketAddress().equals(controlAddress)) {
          currentControlNode = node;
        }
      }
      assertThat(currentControlNode).isNotNull();
      assertThat(queries(simulacron)).hasSize(6);

      assertThat(protocolQueries(contactPoint, 4))
          .containsExactly(
              // Initial connection with protocol v4
              "SELECT cluster_name FROM system.local",
              "SELECT * FROM system.local",
              "SELECT * FROM system.peers");
      assertThat(protocolQueries(currentControlNode, 3))
          .containsExactly(
              // Reconnection with protocol v3
              "SELECT cluster_name FROM system.local",
              "SELECT * FROM system.local",
              "SELECT * FROM system.peers");
    }
  }

  @Test
  public void should_keep_current_if_supported_by_all_peers() {
    try (BoundCluster simulacron = mixedVersions("3.0.0", "2.2.0", "3.11");
        BoundNode contactPoint = simulacron.node(0);
        Cluster<CqlSession> cluster =
            Cluster.builder()
                .addContactPoint(contactPoint.inetSocketAddress())
                .withConfigLoader(new TestConfigLoader("metadata.schema.enabled = false"))
                .build()) {

      InternalDriverContext context = (InternalDriverContext) cluster.getContext();
      assertThat(context.protocolVersion()).isEqualTo(CoreProtocolVersion.V4);
      assertThat(queries(simulacron)).hasSize(3);
      assertThat(protocolQueries(contactPoint, 4))
          .containsExactly(
              // Initial connection with protocol v4
              "SELECT cluster_name FROM system.local",
              "SELECT * FROM system.local",
              "SELECT * FROM system.peers");
    }
  }

  @Test
  public void should_fail_if_peer_does_not_support_v3() {
    thrown.expect(UnsupportedProtocolVersionException.class);
    thrown.expectMessage(
        "reports Cassandra version 2.0.9, but the driver only supports 2.1.0 and above");

    try (BoundCluster simulacron = mixedVersions("3.0.0", "2.0.9", "3.11");
        BoundNode contactPoint = simulacron.node(0);
        Cluster<CqlSession> ignored =
            Cluster.builder()
                .addContactPoint(contactPoint.inetSocketAddress())
                .withConfigLoader(new TestConfigLoader())
                .build()) {
      fail("Cluster init should have failed");
    }
  }

  @Test
  public void should_not_downgrade_and_force_down_old_nodes_if_version_forced() {
    try (BoundCluster simulacron = mixedVersions("3.0.0", "2.2.0", "2.0.0");
        BoundNode contactPoint = simulacron.node(0);
        Cluster<CqlSession> cluster =
            Cluster.builder()
                .addContactPoint(contactPoint.inetSocketAddress())
                .withConfigLoader(
                    new TestConfigLoader(
                        "protocol.version = V4", "metadata.schema.enabled = false"))
                .build()) {
      assertThat(cluster.getContext().protocolVersion()).isEqualTo(CoreProtocolVersion.V4);

      assertThat(queries(simulacron)).hasSize(3);
      assertThat(protocolQueries(contactPoint, 4))
          .containsExactly(
              // Initial connection with protocol v4
              "SELECT cluster_name FROM system.local",
              "SELECT * FROM system.local",
              "SELECT * FROM system.peers");

      // Note: the 2.0.0 would be forced down if we try to open a connection to it. We can't check
      // that here because Simulacron can't prime STARTUP requests.
    }
  }

  private BoundCluster mixedVersions(String... versions) {
    ClusterSpec clusterSpec = ClusterSpec.builder().withCassandraVersion(versions[0]).build();
    DataCenterSpec dc0 = clusterSpec.addDataCenter().build();
    // inherits versions[0]
    dc0.addNode().build();
    for (int i = 1; i < versions.length; i++) {
      dc0.addNode().withCassandraVersion(versions[i]).build();
    }
    return SimulacronRule.server.register(clusterSpec);
  }

  private Stream<QueryLog> queries(BoundTopic topic) {
    return topic
        .getLogs()
        .getQueryLogs()
        .stream()
        .filter(q -> q.getFrame().message instanceof Query);
  }

  private Stream<String> protocolQueries(BoundTopic topic, int protocolVersion) {
    return queries(topic)
        .filter(q -> q.getFrame().protocolVersion == protocolVersion)
        .map(QueryLog::getQuery);
  }
}
