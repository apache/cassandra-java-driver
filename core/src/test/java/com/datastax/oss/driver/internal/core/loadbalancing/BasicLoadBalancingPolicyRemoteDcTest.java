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
package com.datastax.oss.driver.internal.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;
import org.mockito.Mock;

public class BasicLoadBalancingPolicyRemoteDcTest extends BasicLoadBalancingPolicyQueryPlanTest {

  @Mock protected DefaultNode node6;
  @Mock protected DefaultNode node7;
  @Mock protected DefaultNode node8;
  @Mock protected DefaultNode node9;
  @Mock protected DefaultNode node10;
  @Mock protected DefaultNode node11;
  @Mock protected DefaultNode node12;

  @Override
  @Test
  public void
      should_use_round_robin_when_token_map_returns_no_replicas_using_request_keyspace_and_routing_key() {
    when(request.getRoutingKeyspace()).thenReturn(KEYSPACE);
    when(request.getRoutingKey()).thenReturn(ROUTING_KEY);
    when(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY)).thenReturn(Collections.emptySet());

    assertRoundRobinQueryPlans();

    then(tokenMap).should(atLeast(1)).getReplicas(KEYSPACE, ROUTING_KEY);
  }

  @Override
  @Test
  public void should_prioritize_and_shuffle_replicas() {
    when(request.getRoutingKeyspace()).thenReturn(KEYSPACE);
    when(request.getRoutingKey()).thenReturn(ROUTING_KEY);
    when(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY)).thenReturn(ImmutableSet.of(node3, node5));

    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node1, node2, node4, node9, node10, node6, node7, node12);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node2, node4, node1, node9, node10, node6, node7, node12);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node4, node1, node2, node9, node10, node6, node7, node12);

    verify(policy, times(3)).shuffleHead(any(), eq(2));
    // No power of two choices with only two replicas
    verify(session, never()).getPools();
  }

  @Override
  @Test
  public void should_prioritize_single_replica() {
    when(request.getRoutingKeyspace()).thenReturn(KEYSPACE);
    when(request.getRoutingKey()).thenReturn(ROUTING_KEY);
    when(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY)).thenReturn(ImmutableSet.of(node3));

    // node3 always first, round-robin on the rest
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node1, node2, node4, node5, node9, node10, node6, node7, node12);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node2, node4, node5, node1, node9, node10, node6, node7, node12);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node4, node5, node1, node2, node9, node10, node6, node7, node12);
    assertThat(policy.newQueryPlan(request, session))
        .containsExactly(node3, node5, node1, node2, node4, node9, node10, node6, node7, node12);

    // Should not shuffle replicas since there is only one
    verify(policy, never()).shuffleHead(any(), anyInt());
  }

  @Override
  protected void assertRoundRobinQueryPlans() {
    for (int i = 0; i < 3; i++) {
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node1, node2, node3, node4, node5, node9, node10, node6, node7, node12);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node2, node3, node4, node5, node1, node9, node10, node6, node7, node12);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node3, node4, node5, node1, node2, node9, node10, node6, node7, node12);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node4, node5, node1, node2, node3, node9, node10, node6, node7, node12);
      assertThat(policy.newQueryPlan(request, session))
          .containsExactly(node5, node1, node2, node3, node4, node9, node10, node6, node7, node12);
    }
  }

  @Override
  protected BasicLoadBalancingPolicy createAndInitPolicy() {
    when(node4.getDatacenter()).thenReturn("dc1");
    when(node5.getDatacenter()).thenReturn("dc1");
    when(node6.getDatacenter()).thenReturn("dc2");
    when(node7.getDatacenter()).thenReturn("dc2");
    when(node8.getDatacenter()).thenReturn("dc2");
    when(node9.getDatacenter()).thenReturn("dc3");
    when(node10.getDatacenter()).thenReturn("dc3");
    when(node11.getDatacenter()).thenReturn("dc3");
    when(node12.getDatacenter()).thenReturn("dc4");

    // Accept 2 nodes per remote DC
    when(defaultProfile.getInt(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .thenReturn(2);
    when(defaultProfile.getBoolean(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS))
        .thenReturn(false);

    final List<String> preferredDcs = new ArrayList<>();
    preferredDcs.add("dc3");
    preferredDcs.add("dc2");
    when(defaultProfile.getStringList(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_PREFERRED_REMOTE_DCS, new ArrayList<>()))
        .thenReturn(preferredDcs);

    // Use a subclass to disable shuffling, we just spy to make sure that the shuffling method was
    // called (makes tests easier)
    BasicLoadBalancingPolicy policy =
        spy(
            new BasicLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME) {
              @Override
              protected void shuffleHead(Object[] currentNodes, int headLength) {
                // nothing (keep in same order)
              }

              @Override
              protected void shuffleHeadRemote(Object[] currentNodes, int headLength) {
                // nothing (keep in same order)
              }
            });
    Map<UUID, Node> nodes =
        ImmutableMap.<UUID, Node>builder()
            .put(UUID.randomUUID(), node1)
            .put(UUID.randomUUID(), node2)
            .put(UUID.randomUUID(), node3)
            .put(UUID.randomUUID(), node4)
            .put(UUID.randomUUID(), node5)
            .put(UUID.randomUUID(), node6)
            .put(UUID.randomUUID(), node7)
            .put(UUID.randomUUID(), node8)
            .put(UUID.randomUUID(), node9)
            .put(UUID.randomUUID(), node10)
            .put(UUID.randomUUID(), node11)
            .put(UUID.randomUUID(), node12)
            .build();
    policy.init(nodes, distanceReporter);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2, node3, node4, node5);
    assertThat(policy.getLiveNodes().dc("dc2")).containsExactly(node6, node7); // only 2 allowed
    assertThat(policy.getLiveNodes().dc("dc3")).containsExactly(node9, node10); // only 2 allowed
    assertThat(policy.getLiveNodes().dc("dc4")).containsExactly(node12); // only 1 allowed
    return policy;
  }
}
