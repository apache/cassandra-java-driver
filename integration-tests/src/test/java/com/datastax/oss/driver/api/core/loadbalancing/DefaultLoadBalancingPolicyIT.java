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
package com.datastax.oss.driver.api.core.loadbalancing;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.cluster.ClusterRule;
import com.datastax.oss.driver.api.testinfra.utils.ConditionChecker;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

public class DefaultLoadBalancingPolicyIT {

  private static final String LOCAL_DC = "dc1";

  @ClassRule public static CustomCcmRule ccmRule = CustomCcmRule.builder().withNodes(5, 5).build();

  @ClassRule
  public static ClusterRule clusterRule =
      ClusterRule.builder(ccmRule)
          .withKeyspace(false)
          .withDefaultSession(true)
          .withOptions("request.timeout = 30 seconds")
          .build();

  @BeforeClass
  public static void setup() {
    CqlSession session = clusterRule.session();
    session.execute(
        "CREATE KEYSPACE test "
            + "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}");
    session.execute("CREATE TABLE test.foo (k int PRIMARY KEY)");
  }

  @Test
  public void should_ignore_remote_dcs() {
    for (Node node : clusterRule.cluster().getMetadata().getNodes().values()) {
      if (LOCAL_DC.equals(node.getDatacenter())) {
        assertThat(node.getDistance()).isEqualTo(NodeDistance.LOCAL);
        assertThat(node.getState()).isEqualTo(NodeState.UP);
        // 1 regular connection, maybe 1 control connection
        assertThat(node.getOpenConnections()).isBetween(1, 2);
        assertThat(node.isReconnecting()).isFalse();
      } else {
        assertThat(node.getDistance()).isEqualTo(NodeDistance.IGNORED);
        assertThat(node.getOpenConnections()).isEqualTo(0);
        assertThat(node.isReconnecting()).isFalse();
      }
    }
  }

  @Test
  public void should_use_round_robin_on_local_dc_when_not_enough_routing_information() {
    ByteBuffer routingKey = TypeCodecs.INT.encodePrimitive(1, ProtocolVersion.DEFAULT);
    TokenMap tokenMap = clusterRule.cluster().getMetadata().getTokenMap().get();
    // TODO add statements with setKeyspace when that is supported
    List<Statement> statements =
        ImmutableList.of(
            // No information at all
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1"),
            // Keyspace present, missing routing key
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1")
                .setRoutingKeyspace(CqlIdentifier.fromCql("test")),
            // Routing key present, missing keyspace
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1")
                .setRoutingKey(routingKey),
            // Routing token present, missing keyspace
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1")
                .setRoutingToken(tokenMap.newToken(routingKey)));

    for (Statement statement : statements) {
      List<Node> coordinators = new ArrayList<>();
      for (int i = 0; i < 15; i++) {
        ResultSet rs = clusterRule.session().execute(statement);
        Node coordinator = rs.getExecutionInfo().getCoordinator();
        assertThat(coordinator.getDatacenter()).isEqualTo(LOCAL_DC);
        coordinators.add(coordinator);
      }
      for (int i = 0; i < 5; i++) {
        assertThat(coordinators.get(i))
            .isEqualTo(coordinators.get(5 + i))
            .isEqualTo(coordinators.get(10 + i));
      }
    }
  }

  @Test
  public void should_prioritize_replicas_when_routing_information_present() {
    CqlIdentifier keyspace = CqlIdentifier.fromCql("test");
    ByteBuffer routingKey = TypeCodecs.INT.encodePrimitive(1, ProtocolVersion.DEFAULT);
    TokenMap tokenMap = clusterRule.cluster().getMetadata().getTokenMap().get();
    Set<Node> localReplicas = new HashSet<>();
    for (Node replica : tokenMap.getReplicas(keyspace, routingKey)) {
      if (replica.getDatacenter().equals(LOCAL_DC)) {
        localReplicas.add(replica);
      }
    }
    assertThat(localReplicas).hasSize(3);

    // TODO add statements with setKeyspace when that is supported
    List<Statement> statements =
        ImmutableList.of(
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1")
                .setRoutingKeyspace(keyspace)
                .setRoutingKey(routingKey),
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1")
                .setRoutingKeyspace(keyspace)
                .setRoutingToken(tokenMap.newToken(routingKey)));

    for (Statement statement : statements) {
      // Since the exact order is randomized, just run a bunch of queries and check that we get a
      // reasonable distribution:
      Map<Node, Integer> hits = new HashMap<>();
      for (int i = 0; i < 3000; i++) {
        ResultSet rs = clusterRule.session().execute(statement);
        Node coordinator = rs.getExecutionInfo().getCoordinator();
        assertThat(localReplicas).contains(coordinator);
        assertThat(coordinator.getDatacenter()).isEqualTo(LOCAL_DC);
        hits.merge(coordinator, 1, (a, b) -> a + b);
      }

      for (Integer count : hits.values()) {
        assertThat(count).isCloseTo(1000, withinPercentage(10));
      }
    }
  }

  @Test
  public void should_hit_non_replicas_when_routing_information_present_but_all_replicas_down() {
    CqlIdentifier keyspace = CqlIdentifier.fromCql("test");
    ByteBuffer routingKey = TypeCodecs.INT.encodePrimitive(1, ProtocolVersion.DEFAULT);
    TokenMap tokenMap = clusterRule.cluster().getMetadata().getTokenMap().get();

    InternalDriverContext context = (InternalDriverContext) clusterRule.cluster().getContext();

    Set<Node> localReplicas = new HashSet<>();
    for (Node replica : tokenMap.getReplicas(keyspace, routingKey)) {
      if (replica.getDatacenter().equals(LOCAL_DC)) {
        localReplicas.add(replica);
        context.eventBus().fire(TopologyEvent.forceDown(replica.getConnectAddress()));
        ConditionChecker.checkThat(() -> assertThat(replica.getOpenConnections()).isZero())
            .becomesTrue();
      }
    }
    assertThat(localReplicas).hasSize(3);

    // TODO add statements with setKeyspace when that is supported
    List<Statement> statements =
        ImmutableList.of(
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1")
                .setRoutingKeyspace(keyspace)
                .setRoutingKey(routingKey),
            SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1")
                .setRoutingKeyspace(keyspace)
                .setRoutingToken(tokenMap.newToken(routingKey)));

    for (Statement statement : statements) {
      List<Node> coordinators = new ArrayList<>();
      for (int i = 0; i < 6; i++) {
        ResultSet rs = clusterRule.session().execute(statement);
        Node coordinator = rs.getExecutionInfo().getCoordinator();
        coordinators.add(coordinator);
        assertThat(coordinator.getDatacenter()).isEqualTo(LOCAL_DC);
        assertThat(localReplicas).doesNotContain(coordinator);
      }
      // Should round-robin on the two non-replicas
      for (int i = 0; i < 2; i++) {
        assertThat(coordinators.get(i))
            .isEqualTo(coordinators.get(2 + i))
            .isEqualTo(coordinators.get(4 + i));
      }
    }

    for (Node replica : localReplicas) {
      context.eventBus().fire(TopologyEvent.forceUp(replica.getConnectAddress()));
      ConditionChecker.checkThat(() -> assertThat(replica.getOpenConnections()).isPositive())
          .becomesTrue();
    }
  }
}
