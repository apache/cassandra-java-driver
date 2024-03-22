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
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class LatencySensitiveLoadBalancingPolicyTest extends BasicLoadBalancingPolicyQueryPlanTest {

  private static final long T1 = 100;

  @Mock protected ChannelPool pool1;
  @Mock protected ChannelPool pool2;
  @Mock protected ChannelPool pool3;
  @Mock protected ChannelPool pool4;
  @Mock protected ChannelPool pool5;

  long nanoTime;
  int diceRoll;

  private LatencySensitiveLoadBalancingPolicy latencyPolicy;

  @Before
  @Override
  public void setup() {
    nanoTime = T1;
    diceRoll = 4;
    given(node4.getDatacenter()).willReturn("dc1");
    given(node5.getDatacenter()).willReturn("dc1");
    given(session.getPools())
        .willReturn(
            ImmutableMap.of(
                node1, pool1,
                node2, pool2,
                node3, pool3,
                node4, pool4,
                node5, pool5));
    given(context.getMetadataManager()).willReturn(metadataManager);
    given(metadataManager.getMetadata()).willReturn(metadata);
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    given(metadata.getTokenMap()).willAnswer(invocation -> Optional.of(tokenMap));
    super.setup();
    latencyPolicy = (LatencySensitiveLoadBalancingPolicy) policy;
    // Note: this assertion relies on the fact that policy.getLiveNodes() implementation preserves
    // insertion order.
    assertThat(latencyPolicy.getLiveNodes().dc("dc1"))
        .containsExactly(node1, node2, node3, node4, node5);
  }

  @Override
  protected BasicLoadBalancingPolicy createAndInitPolicy() {
    // Use a subclass to disable shuffling, we just spy to make sure that the shuffling method was
    // called (makes tests easier)
    BasicLoadBalancingPolicy policy =
        spy(
            new LatencySensitiveLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME) {
              @Override
              protected void shuffleHead(Object[] currentNodes, int headLength) {
                // nothing (keep in same order)
              }
            });
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3,
            UUID.randomUUID(), node4,
            UUID.randomUUID(), node5),
        distanceReporter);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2, node3, node4, node5);
    return policy;
  }

  @Test
  public void should_put_slowest_node_at_end_when_3_local_replicas() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    for (int i = 0; i < 100; i++) {
      latencyPolicy.onNodeSuccess(null, 100000000, null, node1, "");
      latencyPolicy.onNodeSuccess(null, 90000000, null, node3, "");
      latencyPolicy.onNodeSuccess(null, 90000000, null, node5, "");
    }

    // When
    Queue<Node> plan1 = latencyPolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = latencyPolicy.newQueryPlan(request, session);
    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // node 3 faster -> swap
    assertThat(plan1).containsExactly(node3, node5, node1, node2, node4);
    assertThat(plan2).containsExactly(node3, node5, node1, node4, node2);

    then(latencyPolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(latencyPolicy).should(times(2)).nanoTime();
    then(latencyPolicy).should(never()).diceRoll1d4();
  }

  @Test
  public void should_put_slowest_replicas_at_end_when_4_replicas() throws InterruptedException {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node4, node5));

    for (int i = 0; i < 100; i++) {
      latencyPolicy.onNodeSuccess(null, 100000000, null, node1, "");
      latencyPolicy.onNodeSuccess(null, 95000000, null, node3, "");
      latencyPolicy.onNodeSuccess(null, 90000000, null, node4, "");
      latencyPolicy.onNodeSuccess(null, 90000000, null, node5, "");
    }

    // When
    Queue<Node> plan1 = latencyPolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = latencyPolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // node 3 faster -> swap
    assertThat(plan1).containsExactly(node4, node5, node3, node1, node2);
    assertThat(plan2).containsExactly(node4, node5, node3, node1, node2);

    then(latencyPolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(latencyPolicy).should(times(2)).nanoTime();
    then(latencyPolicy).should(never()).diceRoll1d4();
  }
}
