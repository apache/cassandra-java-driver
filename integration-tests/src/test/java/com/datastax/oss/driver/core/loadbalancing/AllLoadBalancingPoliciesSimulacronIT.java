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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.NoNodeAvailableException;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.api.testinfra.simulacron.SimulacronRule;
import com.datastax.oss.driver.categories.ParallelizableTests;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl.RowBuilder;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.BoundTopic;
import com.tngtech.java.junit.dataprovider.DataProvider;
import com.tngtech.java.junit.dataprovider.DataProviderRunner;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(ParallelizableTests.class)
@RunWith(DataProviderRunner.class)
public class AllLoadBalancingPoliciesSimulacronIT {

  @ClassRule
  public static final SimulacronRule SIMULACRON_RULE =
      new SimulacronRule(ClusterSpec.builder().withNodes(5, 5, 5));

  @Before
  public void reset() {
    SIMULACRON_RULE.cluster().start();
    SIMULACRON_RULE.cluster().clearLogs();
    SIMULACRON_RULE.cluster().clearPrimes(true);
    SIMULACRON_RULE
        .cluster()
        .prime(
            PrimeDsl.when("SELECT * FROM system_schema.keyspaces")
                .then(new RowBuilder().columnTypes(KEYSPACE_COLUMNS).row(KEYSPACE_ROW).build()));
  }

  @Test
  @DataProvider({
    "BasicLoadBalancingPolicy,dc1",
    "DefaultLoadBalancingPolicy,dc1",
    "DcInferringLoadBalancingPolicy,dc1",
    "DcInferringLoadBalancingPolicy,null",
  })
  public void should_round_robin_within_local_dc_when_dc_aware_but_not_token_aware(
      String lbp, String dc) {

    // given: DC is provided or inferred, token awareness is disabled and remote DCs are allowed
    try (CqlSession session = newSession(lbp, dc, 2, true, false)) {

      // when: a query is executed 50 times.
      for (int i = 0; i < 50; i++) {
        session.execute(QUERY);
      }

      // then: each node in local DC should get an equal number of requests.
      for (int i = 0; i < 5; i++) {
        assertThat(queries(0, i).count()).isEqualTo(10);
      }

      // then: no node in the remote DC should get a request.
      assertThat(queries(1).count()).isEqualTo(0);
      assertThat(queries(2).count()).isEqualTo(0);
    }
  }

  @Test
  @DataProvider({
    "BasicLoadBalancingPolicy,dc1,ONE",
    "BasicLoadBalancingPolicy,dc1,LOCAL_ONE",
    "BasicLoadBalancingPolicy,dc1,TWO",
    "BasicLoadBalancingPolicy,dc1,QUORUM",
    "BasicLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DefaultLoadBalancingPolicy,dc1,ONE",
    "DefaultLoadBalancingPolicy,dc1,LOCAL_ONE",
    "DefaultLoadBalancingPolicy,dc1,TWO",
    "DefaultLoadBalancingPolicy,dc1,QUORUM",
    "DefaultLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DcInferringLoadBalancingPolicy,dc1,ONE",
    "DcInferringLoadBalancingPolicy,dc1,LOCAL_ONE",
    "DcInferringLoadBalancingPolicy,dc1,TWO",
    "DcInferringLoadBalancingPolicy,dc1,QUORUM",
    "DcInferringLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DcInferringLoadBalancingPolicy,null,ONE",
    "DcInferringLoadBalancingPolicy,null,LOCAL_ONE",
    "DcInferringLoadBalancingPolicy,null,TWO",
    "DcInferringLoadBalancingPolicy,null,QUORUM",
    "DcInferringLoadBalancingPolicy,null,LOCAL_QUORUM",
  })
  public void should_use_local_replicas_when_dc_aware_and_token_aware_and_enough_local_replicas_up(
      String lbp, String dc, DefaultConsistencyLevel cl) {

    // given: DC is provided or inferred, token awareness enabled, remotes allowed, CL <= 2
    try (CqlSession session = newSession(lbp, dc, 2, true)) {

      // given: one replica and 2 non-replicas down in local DC, but CL <= 2 still achievable
      List<Node> aliveReplicas = degradeLocalDc(session);

      // when: a query is executed 50 times and some nodes are down in the local DC.
      for (int i = 0; i < 50; i++) {
        session.execute(
            SimpleStatement.newInstance(QUERY)
                .setConsistencyLevel(cl)
                .setRoutingKeyspace("test")
                .setRoutingKey(ROUTING_KEY));
      }

      // then: all requests should be distributed to the remaining up replicas in local DC
      BoundNode alive1 = findNode(aliveReplicas.get(0));
      BoundNode alive2 = findNode(aliveReplicas.get(1));
      assertThat(queries(alive1).count() + queries(alive2).count()).isEqualTo(50);

      // then: no node in the remote DCs should get a request.
      assertThat(queries(1).count()).isEqualTo(0);
      assertThat(queries(2).count()).isEqualTo(0);
    }
  }

  @Test
  public void should_round_robin_within_all_dcs_when_dc_agnostic() {

    // given: DC-agnostic LBP, no local DC, remotes not allowed, token awareness enabled
    try (CqlSession session = newSession("BasicLoadBalancingPolicy", null, 0, false)) {

      // when: a query is executed 150 times.
      for (int i = 0; i < 150; i++) {
        session.execute(
            SimpleStatement.newInstance(QUERY)
                // local CL should be ignored since there is no local DC
                .setConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM));
      }

      // then: each node should get 10 requests, even remote ones since the LBP is DC-agnostic.
      for (int dc = 0; dc < 3; dc++) {
        for (int n = 0; n < 5; n++) {
          assertThat(queries(dc, n).count()).isEqualTo(10);
        }
      }
    }
  }

  @Test
  @DataProvider({
    "BasicLoadBalancingPolicy,dc1,ONE",
    "BasicLoadBalancingPolicy,dc1,TWO",
    "BasicLoadBalancingPolicy,dc1,THREE",
    "BasicLoadBalancingPolicy,dc1,QUORUM",
    "BasicLoadBalancingPolicy,dc1,ANY",
    "DefaultLoadBalancingPolicy,dc1,ONE",
    "DefaultLoadBalancingPolicy,dc1,TWO",
    "DefaultLoadBalancingPolicy,dc1,THREE",
    "DefaultLoadBalancingPolicy,dc1,QUORUM",
    "DefaultLoadBalancingPolicy,dc1,ANY",
    "DcInferringLoadBalancingPolicy,dc1,ONE",
    "DcInferringLoadBalancingPolicy,dc1,TWO",
    "DcInferringLoadBalancingPolicy,dc1,THREE",
    "DcInferringLoadBalancingPolicy,dc1,QUORUM",
    "DcInferringLoadBalancingPolicy,dc1,ANY",
    "DcInferringLoadBalancingPolicy,null,ONE",
    "DcInferringLoadBalancingPolicy,null,TWO",
    "DcInferringLoadBalancingPolicy,null,THREE",
    "DcInferringLoadBalancingPolicy,null,QUORUM",
    "DcInferringLoadBalancingPolicy,null,ANY",
  })
  public void should_use_remote_nodes_when_no_up_nodes_in_local_dc_for_non_local_cl(
      String lbp, String dc, DefaultConsistencyLevel cl) {

    // given: 1 remote allowed per DC and a non-local CL, token awareness enabled
    try (CqlSession session = newSession(lbp, dc, 1, false)) {

      // given: local DC is down
      stopLocalDc(session);

      // when: a query is executed 50 times and all nodes are down in local DC.
      for (int i = 0; i < 50; i++) {
        session.execute(
            SimpleStatement.newInstance(QUERY)
                .setConsistencyLevel(cl)
                .setRoutingKeyspace("test")
                .setRoutingKey(ROUTING_KEY));
      }

      // then: only 1 node in each remote DC should get requests (we can't know which ones exactly).
      assertThat(queries(1).count() + queries(2).count()).isEqualTo(50);
    }
  }

  @Test
  @DataProvider({
    "BasicLoadBalancingPolicy,dc1,LOCAL_ONE",
    "BasicLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "BasicLoadBalancingPolicy,dc1,LOCAL_SERIAL",
    "DefaultLoadBalancingPolicy,dc1,LOCAL_ONE",
    "DefaultLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DefaultLoadBalancingPolicy,dc1,LOCAL_SERIAL",
    "DcInferringLoadBalancingPolicy,dc1,LOCAL_ONE",
    "DcInferringLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DcInferringLoadBalancingPolicy,dc1,LOCAL_SERIAL",
    "DcInferringLoadBalancingPolicy,null,LOCAL_ONE",
    "DcInferringLoadBalancingPolicy,null,LOCAL_QUORUM",
    "DcInferringLoadBalancingPolicy,null,LOCAL_SERIAL",
  })
  public void should_not_use_remote_nodes_when_using_local_cl(
      String lbp, String dc, DefaultConsistencyLevel cl) {

    // given: remotes allowed but not for local CL, token awareness enabled, local CL
    try (CqlSession session = newSession(lbp, dc, 5, false)) {

      // given: local DC is down
      stopLocalDc(session);

      // when: a query is executed 50 times and all nodes are down in local DC.
      for (int i = 0; i < 50; i++) {
        Throwable t =
            catchThrowable(
                () ->
                    session.execute(
                        SimpleStatement.newInstance(QUERY)
                            .setConsistencyLevel(cl)
                            .setRoutingKeyspace("test")
                            .setRoutingKey(ROUTING_KEY)));

        // then: expect a NNAE for a local CL since no local replicas available.
        assertThat(t).isInstanceOf(NoNodeAvailableException.class);
      }

      // then: no node in the remote DCs should get a request.
      assertThat(queries(1).count()).isEqualTo(0);
      assertThat(queries(2).count()).isEqualTo(0);
    }
  }

  @Test
  @DataProvider({
    "BasicLoadBalancingPolicy,dc1,LOCAL_ONE",
    "BasicLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DefaultLoadBalancingPolicy,dc1,LOCAL_ONE",
    "DefaultLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DcInferringLoadBalancingPolicy,dc1,LOCAL_ONE",
    "DcInferringLoadBalancingPolicy,dc1,LOCAL_QUORUM",
    "DcInferringLoadBalancingPolicy,null,LOCAL_ONE",
    "DcInferringLoadBalancingPolicy,null,LOCAL_QUORUM",
  })
  public void should_use_remote_nodes_when_using_local_cl_if_allowed(
      String lbp, String dc, DefaultConsistencyLevel cl) {

    // given: only one node allowed per remote DC and remotes allowed even for local CLs.
    try (CqlSession session = newSession(lbp, dc, 1, true)) {

      // given: local DC is down
      stopLocalDc(session);

      // when: a query is executed 50 times and all nodes are down in local DC.
      for (int i = 0; i < 50; i++) {
        session.execute(
            SimpleStatement.newInstance(QUERY)
                .setConsistencyLevel(cl)
                .setRoutingKeyspace("test")
                .setRoutingKey(ROUTING_KEY));
      }

      // then: only 1 node in each remote DC should get requests (we can't know which ones exactly).
      assertThat(queries(1).count() + queries(2).count()).isEqualTo(50);
    }
  }

  @Test
  @DataProvider({
    "BasicLoadBalancingPolicy,dc1",
    "DefaultLoadBalancingPolicy,dc1",
    "DcInferringLoadBalancingPolicy,dc1",
    "DcInferringLoadBalancingPolicy,null"
  })
  public void should_not_use_excluded_dc_using_node_filter(String lbp, String dc) {

    // given: remotes allowed even for local CLs, but node filter excluding dc2
    try (CqlSession session = newSession(lbp, dc, 5, true, true, excludeDc("dc2"))) {

      // when: A query is made and nodes for the local dc are available.
      for (int i = 0; i < 50; i++) {
        session.execute(
            SimpleStatement.newInstance(QUERY)
                .setRoutingKeyspace("test")
                .setRoutingKey(ROUTING_KEY));
      }

      // then: only nodes in the local DC should have been queried.
      assertThat(queries(0).count()).isEqualTo(50);
      assertThat(queries(1).count()).isEqualTo(0);
      assertThat(queries(2).count()).isEqualTo(0);

      // given: local DC is down
      stopLocalDc(session);

      SIMULACRON_RULE.cluster().clearLogs();

      // when: A query is made and all nodes in the local dc are down.
      for (int i = 0; i < 50; i++) {
        session.execute(
            SimpleStatement.newInstance(QUERY)
                .setRoutingKeyspace("test")
                .setRoutingKey(ROUTING_KEY));
      }

      // then: Only nodes in DC3 should have been queried, since DC2 is excluded and DC1 is down.
      assertThat(queries(0).count()).isEqualTo(0);
      assertThat(queries(1).count()).isEqualTo(0);
      assertThat(queries(2).count()).isEqualTo(50);
    }
  }

  private static final ByteBuffer ROUTING_KEY = ByteBuffer.wrap(new byte[] {1, 2, 3, 4});

  private static final String[] KEYSPACE_COLUMNS =
      new String[] {
        "keyspace_name", "varchar",
        "durable_writes", "boolean",
        "replication", "map<varchar, varchar>"
      };

  private static final Object[] KEYSPACE_ROW =
      new Object[] {
        "keyspace_name",
        "test",
        "durable_writes",
        true,
        "replication",
        ImmutableMap.of(
            "class",
            "org.apache.cassandra.locator.NetworkTopologyStrategy",
            "dc1",
            "3",
            "dc2",
            "3",
            "dc3",
            "3")
      };

  private static final String QUERY = "SELECT * FROM test.foo";

  private CqlSession newSession(String lbp, String dc, int maxRemoteNodes, boolean allowLocalCl) {
    return newSession(lbp, dc, maxRemoteNodes, allowLocalCl, true);
  }

  private CqlSession newSession(
      String lbp, String dc, int maxRemoteNodes, boolean allowLocalCl, boolean tokenAware) {
    return newSession(lbp, dc, maxRemoteNodes, allowLocalCl, tokenAware, null);
  }

  private CqlSession newSession(
      String lbp,
      String dc,
      int maxRemoteNodes,
      boolean allowLocalCl,
      boolean tokenAware,
      Predicate<Node> nodeFilter) {
    DriverConfigLoader loader =
        SessionUtils.configLoaderBuilder()
            .withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, tokenAware)
            .withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, lbp)
            .withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, dc)
            .withInt(
                DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC,
                maxRemoteNodes)
            .withBoolean(
                DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS,
                allowLocalCl)
            .build();
    return SessionUtils.newSession(SIMULACRON_RULE, null, null, null, nodeFilter, loader);
  }

  private BoundNode findNode(Node node) {
    BoundCluster simulacron = SIMULACRON_RULE.cluster();
    SocketAddress toFind = node.getEndPoint().resolve();
    for (BoundNode boundNode : simulacron.getNodes()) {
      if (boundNode.getAddress().equals(toFind)) {
        return boundNode;
      }
    }
    throw new AssertionError("Could not find node: " + toFind);
  }

  private void stopLocalDc(CqlSession session) {
    SIMULACRON_RULE.cluster().dc(0).stop();
    awaitDown(nodesInDc(session, "dc1"));
  }

  private List<Node> degradeLocalDc(CqlSession session) {
    // stop 1 replica and 2 non-replicas in dc1
    List<Node> localReplicas = replicasInDc(session, "dc1");
    assertThat(localReplicas).hasSize(3);
    BoundNode replica1 = findNode(localReplicas.get(0));

    List<Node> localOthers = nonReplicasInDc(session, "dc1");
    assertThat(localOthers).hasSize(2);
    BoundNode other1 = findNode(localOthers.get(0));
    BoundNode other2 = findNode(localOthers.get(1));

    replica1.stop();
    other1.stop();
    other2.stop();

    awaitDown(localReplicas.get(0), localOthers.get(0), localOthers.get(1));
    return localReplicas.subList(1, 3);
  }

  private Stream<QueryLog> queries(int dc, int node) {
    return queries(SIMULACRON_RULE.cluster().dc(dc).node(node));
  }

  private Stream<QueryLog> queries(int dc) {
    return queries(SIMULACRON_RULE.cluster().dc(dc));
  }

  private Stream<QueryLog> queries(BoundTopic<?, ?> topic) {
    return topic.getLogs().getQueryLogs().stream()
        .filter(q -> q.getFrame().message instanceof Query)
        .filter(q -> ((Query) q.getFrame().message).query.equals(QUERY));
  }

  private List<Node> nodesInDc(CqlSession session, String dcName) {
    return session.getMetadata().getNodes().values().stream()
        .filter(n -> Objects.equals(n.getDatacenter(), dcName))
        .collect(Collectors.toList());
  }

  private List<Node> replicasInDc(CqlSession session, String dcName) {
    assertThat(session.getMetadata().getTokenMap()).isPresent();
    TokenMap tokenMap = session.getMetadata().getTokenMap().get();
    return tokenMap.getReplicas("test", ROUTING_KEY).stream()
        .filter(n -> Objects.equals(n.getDatacenter(), dcName))
        .collect(Collectors.toList());
  }

  private List<Node> nonReplicasInDc(
      CqlSession session, @SuppressWarnings("SameParameterValue") String dcName) {
    List<Node> nodes = nodesInDc(session, dcName);
    nodes.removeAll(replicasInDc(session, dcName));
    return nodes;
  }

  private Predicate<Node> excludeDc(@SuppressWarnings("SameParameterValue") String dcName) {
    return node -> !Objects.equals(node.getDatacenter(), dcName);
  }

  private void awaitDown(Node... nodes) {
    awaitDown(Arrays.asList(nodes));
  }

  private void awaitDown(Iterable<Node> nodes) {
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(
            () -> {
              for (Node node : nodes) {
                assertThat(node.getState()).isEqualTo(NodeState.DOWN);
              }
            });
  }
}
