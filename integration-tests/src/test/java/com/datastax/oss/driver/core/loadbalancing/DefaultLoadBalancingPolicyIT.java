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
package com.datastax.oss.driver.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.withinPercentage;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.testinfra.ccm.CustomCcmRule;
import com.datastax.oss.driver.api.testinfra.session.SessionRule;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import com.google.common.collect.ImmutableList;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class DefaultLoadBalancingPolicyIT {

  private static final String LOCAL_DC = "dc1";

  private static final CustomCcmRule CCM_RULE = CustomCcmRule.builder().withNodes(4, 1).build();

  private static final SessionRule<CqlSession> SESSION_RULE =
      SessionRule.builder(CCM_RULE)
          .withKeyspace(false)
          .withConfigLoader(
              SessionUtils.configLoaderBuilder()
                  .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(30))
                  .build())
          .build();

  @ClassRule
  public static final TestRule CHAIN = RuleChain.outerRule(CCM_RULE).around(SESSION_RULE);

  @BeforeClass
  public static void setup() {
    CqlSession session = SESSION_RULE.session();
    session.execute(
        "CREATE KEYSPACE test "
            + "WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': 2, 'dc2': 1}");
    session.execute("CREATE TABLE test.foo (k int PRIMARY KEY)");
  }

  @Test
  public void should_ignore_remote_dcs() {
    for (Node node : SESSION_RULE.session().getMetadata().getNodes().values()) {
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
    TokenMap tokenMap = SESSION_RULE.session().getMetadata().getTokenMap().get();
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
      for (int i = 0; i < 12; i++) {
        ResultSet rs = SESSION_RULE.session().execute(statement);
        Node coordinator = rs.getExecutionInfo().getCoordinator();
        assertThat(coordinator.getDatacenter()).isEqualTo(LOCAL_DC);
        coordinators.add(coordinator);
      }
      for (int i = 0; i < 4; i++) {
        assertThat(coordinators.get(i))
            .isEqualTo(coordinators.get(4 + i))
            .isEqualTo(coordinators.get(8 + i));
      }
    }
  }

  @Test
  public void should_prioritize_replicas_when_routing_information_present() {
    CqlIdentifier keyspace = CqlIdentifier.fromCql("test");
    ByteBuffer routingKey = TypeCodecs.INT.encodePrimitive(1, ProtocolVersion.DEFAULT);
    TokenMap tokenMap = SESSION_RULE.session().getMetadata().getTokenMap().get();
    Set<Node> localReplicas = new HashSet<>();
    for (Node replica : tokenMap.getReplicas(keyspace, routingKey)) {
      if (replica.getDatacenter().equals(LOCAL_DC)) {
        localReplicas.add(replica);
      }
    }
    assertThat(localReplicas).hasSize(2);

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
      for (int i = 0; i < 2000; i++) {
        ResultSet rs = SESSION_RULE.session().execute(statement);
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
    TokenMap tokenMap = SESSION_RULE.session().getMetadata().getTokenMap().get();

    InternalDriverContext context = (InternalDriverContext) SESSION_RULE.session().getContext();

    Set<Node> localReplicas = new HashSet<>();
    for (Node replica : tokenMap.getReplicas(keyspace, routingKey)) {
      if (replica.getDatacenter().equals(LOCAL_DC)) {
        localReplicas.add(replica);
        context.getEventBus().fire(TopologyEvent.forceDown(replica.getBroadcastRpcAddress().get()));
        await()
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(60, TimeUnit.SECONDS)
            .untilAsserted(() -> assertThat(replica.getOpenConnections()).isZero());
      }
    }
    assertThat(localReplicas).hasSize(2);

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
        ResultSet rs = SESSION_RULE.session().execute(statement);
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
      context.getEventBus().fire(TopologyEvent.forceUp(replica.getBroadcastRpcAddress().get()));
      await()
          .pollInterval(500, TimeUnit.MILLISECONDS)
          .atMost(60, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(replica.getOpenConnections()).isPositive());
    }
  }

  @Test
  public void should_apply_node_filter() {
    Set<Node> localNodes = new HashSet<>();
    for (Node node : SESSION_RULE.session().getMetadata().getNodes().values()) {
      if (node.getDatacenter().equals(LOCAL_DC)) {
        localNodes.add(node);
      }
    }
    assertThat(localNodes.size()).isEqualTo(4);
    // Pick a random node to exclude -- just ensure that it's not the default contact point since
    // we assert 0 connections at the end of this test (the filter is not applied to contact
    // points).
    EndPoint ignoredEndPoint = firstNonDefaultContactPoint(localNodes);

    // Open a separate session with a filter
    try (CqlSession session =
        SessionUtils.newSession(
            CCM_RULE,
            SESSION_RULE.keyspace(),
            null,
            null,
            node -> !node.getEndPoint().equals(ignoredEndPoint))) {

      // No routing information => should round-robin on white-listed nodes
      SimpleStatement statement = SimpleStatement.newInstance("SELECT * FROM test.foo WHERE k = 1");
      for (int i = 0; i < 12; i++) {
        ResultSet rs = session.execute(statement);
        Node coordinator = rs.getExecutionInfo().getCoordinator();
        assertThat(coordinator.getEndPoint()).isNotEqualTo(ignoredEndPoint);
      }

      assertThat(session.getMetadata().findNode(ignoredEndPoint))
          .hasValueSatisfying(
              ignoredNode -> {
                assertThat(ignoredNode.getOpenConnections()).isEqualTo(0);
              });
    }
  }

  private EndPoint firstNonDefaultContactPoint(Iterable<Node> nodes) {
    for (Node localNode : nodes) {
      EndPoint endPoint = localNode.getEndPoint();
      InetSocketAddress connectAddress = (InetSocketAddress) endPoint.retrieve();
      if (!connectAddress.getAddress().getHostAddress().equals("127.0.0.1")) {
        return endPoint;
      }
    }
    fail("should have other nodes than the default contact point");
    return null; // never reached
  }
}
