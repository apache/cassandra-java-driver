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

import static com.datastax.oss.driver.assertions.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultProtocolVersion;
import com.datastax.oss.driver.api.core.UnsupportedProtocolVersionException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.BoundTopic;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Covers protocol re-negotiation with a mixed cluster: if, after the initial connection and the
 * first node list refresh, we find out that some nodes only support a lower version, reconnect the
 * control connection immediately.
 */
@Category(ParallelizableTests.class)
public class ProtocolVersionMixedClusterIT {

  @Test
  public void should_downgrade_if_peer_does_not_support_negotiated_version() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
            .build();
    try (BoundCluster simulacron = mixedVersions("3.0.0", "2.2.0", "2.1.0");
        BoundNode contactPoint = simulacron.node(0);
        CqlSession session =
            (CqlSession)
                SessionUtils.baseBuilder()
                    .addContactPoint(contactPoint.inetSocketAddress())
                    .withConfigLoader(loader)
                    .build()) {

      InternalDriverContext context = (InternalDriverContext) session.getContext();
      // General version should have been downgraded to V3
      assertThat(context.getProtocolVersion()).isEqualTo(DefaultProtocolVersion.V3);
      // But control connection should still be using protocol V4 since node0 supports V4
      assertThat(context.getControlConnection().channel().protocolVersion())
          .isEqualTo(DefaultProtocolVersion.V4);

      assertThat(queries(simulacron)).hasSize(4);

      assertThat(protocolQueries(contactPoint, 4))
          .containsExactly(
              // Initial connection with protocol v4
              "SELECT cluster_name FROM system.local",
              "SELECT * FROM system.local",
              "SELECT * FROM system.peers_v2",
              "SELECT * FROM system.peers");
    }
  }

  @Test
  public void should_keep_current_if_supported_by_all_peers() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
            .build();
    try (BoundCluster simulacron = mixedVersions("3.0.0", "2.2.0", "3.11");
        BoundNode contactPoint = simulacron.node(0);
        CqlSession session =
            (CqlSession)
                SessionUtils.baseBuilder()
                    .addContactPoint(contactPoint.inetSocketAddress())
                    .withConfigLoader(loader)
                    .build()) {

      InternalDriverContext context = (InternalDriverContext) session.getContext();
      assertThat(context.getProtocolVersion()).isEqualTo(DefaultProtocolVersion.V4);
      assertThat(queries(simulacron)).hasSize(4);
      assertThat(protocolQueries(contactPoint, 4))
          .containsExactly(
              // Initial connection with protocol v4
              "SELECT cluster_name FROM system.local",
              "SELECT * FROM system.local",
              "SELECT * FROM system.peers_v2",
              "SELECT * FROM system.peers");
    }
  }

  @Test
  public void should_fail_if_peer_does_not_support_v3() {

    Throwable t =
        catchThrowable(
            () -> {
              try (BoundCluster simulacron = mixedVersions("3.0.0", "2.0.9", "3.11");
                  BoundNode contactPoint = simulacron.node(0);
                  CqlSession ignored =
                      (CqlSession)
                          SessionUtils.baseBuilder()
                              .addContactPoint(contactPoint.inetSocketAddress())
                              .build()) {
                fail("Cluster init should have failed");
              }
            });

    assertThat(t)
        .isInstanceOf(UnsupportedProtocolVersionException.class)
        .hasMessageContaining(
            "reports Cassandra version 2.0.9, but the driver only supports 2.1.0 and above");
  }

  @Test
  public void should_not_downgrade_and_force_down_old_nodes_if_version_forced() {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withString(DefaultDriverOption.PROTOCOL_VERSION, "V4")
            .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, false)
            .build();
    try (BoundCluster simulacron = mixedVersions("3.0.0", "2.2.0", "2.0.0");
        BoundNode contactPoint = simulacron.node(0);
        CqlSession session =
            (CqlSession)
                SessionUtils.baseBuilder()
                    .addContactPoint(contactPoint.inetSocketAddress())
                    .withConfigLoader(loader)
                    .build()) {
      assertThat(session.getContext().getProtocolVersion()).isEqualTo(DefaultProtocolVersion.V4);

      assertThat(queries(simulacron)).hasSize(4);
      assertThat(protocolQueries(contactPoint, 4))
          .containsExactly(
              // Initial connection with protocol v4
              "SELECT cluster_name FROM system.local",
              "SELECT * FROM system.local",
              "SELECT * FROM system.peers_v2",
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

  private Stream<QueryLog> queries(BoundTopic<?, ?> topic) {
    return topic.getLogs().getQueryLogs().stream()
        .filter(q -> q.getFrame().message instanceof Query);
  }

  private Stream<String> protocolQueries(BoundTopic<?, ?> topic, int protocolVersion) {
    return queries(topic)
        .filter(q -> q.getFrame().protocolVersion == protocolVersion)
        .map(QueryLog::getQuery);
  }
}
