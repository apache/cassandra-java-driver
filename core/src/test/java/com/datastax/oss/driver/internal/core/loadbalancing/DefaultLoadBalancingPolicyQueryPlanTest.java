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

import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLongArray;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class DefaultLoadBalancingPolicyQueryPlanTest extends BasicLoadBalancingPolicyQueryPlanTest {

  private static final long T0 = Long.MIN_VALUE;
  private static final long T1 = 100;
  private static final long T2 = 200;
  private static final long T3 = 300;

  @Mock protected ChannelPool pool1;
  @Mock protected ChannelPool pool2;
  @Mock protected ChannelPool pool3;
  @Mock protected ChannelPool pool4;
  @Mock protected ChannelPool pool5;

  long nanoTime;
  int diceRoll;

  private DefaultLoadBalancingPolicy dsePolicy;

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
    dsePolicy = (DefaultLoadBalancingPolicy) policy;
    // Note: this assertion relies on the fact that policy.getLiveNodes() implementation preserves
    // insertion order.
    assertThat(dsePolicy.getLiveNodes().dc("dc1"))
        .containsExactly(node1, node2, node3, node4, node5);
  }

  @Test
  public void should_prioritize_and_shuffle_2_replicas() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY)).willReturn(ImmutableSet.of(node3, node5));

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan3 = dsePolicy.newQueryPlan(request, session);

    // Then
    // node3 and node5 always first, round-robin on the rest
    assertThat(plan1).containsExactly(node3, node5, node1, node2, node4);
    assertThat(plan2).containsExactly(node3, node5, node2, node4, node1);
    assertThat(plan3).containsExactly(node3, node5, node4, node1, node2);

    then(dsePolicy).should(times(3)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(never()).nanoTime();
    then(dsePolicy).should(never()).diceRoll1d4();
  }

  @Test
  public void should_prioritize_and_shuffle_3_or_more_replicas_when_all_healthy_and_all_newly_up() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    dsePolicy.upTimes.put(node1, T1);
    dsePolicy.upTimes.put(node3, T2);
    dsePolicy.upTimes.put(node5, T3); // newest up replica
    given(pool1.getInFlight()).willReturn(0);
    given(pool3.getInFlight()).willReturn(0);

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // newest up replica is 5, not in first or second position
    assertThat(plan1).containsExactly(node1, node3, node5, node2, node4);
    assertThat(plan2).containsExactly(node1, node3, node5, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(never()).diceRoll1d4();
  }

  @Test
  public void
      should_prioritize_and_shuffle_3_or_more_replicas_when_all_healthy_and_some_newly_up_and_dice_roll_4() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    dsePolicy.upTimes.put(node1, T2); // newest up replica
    dsePolicy.upTimes.put(node3, T1);
    given(pool3.getInFlight()).willReturn(0);
    given(pool5.getInFlight()).willReturn(0);

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // newest up replica is node1 in first position and diceRoll = 4 -> bubbles down
    assertThat(plan1).containsExactly(node3, node5, node1, node2, node4);
    assertThat(plan2).containsExactly(node3, node5, node1, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(times(2)).diceRoll1d4();
  }

  @Test
  public void
      should_prioritize_and_shuffle_3_or_more_replicas_when_all_healthy_and_some_newly_up_and_dice_roll_1() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    dsePolicy.upTimes.put(node1, T2); // newest up replica
    dsePolicy.upTimes.put(node3, T1);
    given(pool1.getInFlight()).willReturn(0);
    given(pool3.getInFlight()).willReturn(0);
    diceRoll = 1;

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // newest up replica is node1 in first position and diceRoll = 1 -> does not bubble down
    assertThat(plan1).containsExactly(node1, node3, node5, node2, node4);
    assertThat(plan2).containsExactly(node1, node3, node5, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(times(2)).diceRoll1d4();
  }

  @Test
  public void should_prioritize_and_shuffle_3_or_more_replicas_when_first_unhealthy() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    given(pool1.getInFlight()).willReturn(100); // unhealthy
    given(pool3.getInFlight()).willReturn(0);
    given(pool5.getInFlight()).willReturn(0);

    dsePolicy.responseTimes.put(node1, new AtomicLongArray(new long[] {T0, T0})); // unhealthy

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // node1 is unhealthy = 1 -> bubbles down
    assertThat(plan1).containsExactly(node3, node5, node1, node2, node4);
    assertThat(plan2).containsExactly(node3, node5, node1, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(never()).diceRoll1d4();
  }

  @Test
  public void
      should_not_treat_node_as_unhealthy_if_has_in_flight_exceeded_but_response_times_normal() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    given(pool1.getInFlight()).willReturn(100); // unhealthy
    given(pool3.getInFlight()).willReturn(0);
    given(pool5.getInFlight()).willReturn(0);

    dsePolicy.responseTimes.put(node1, new AtomicLongArray(new long[] {T1, T1})); // healthy

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // node1 has more in-flight than node3 -> swap
    assertThat(plan1).containsExactly(node3, node1, node5, node2, node4);
    assertThat(plan2).containsExactly(node3, node1, node5, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(never()).diceRoll1d4();
  }

  @Test
  public void should_prioritize_and_shuffle_3_or_more_replicas_when_last_unhealthy() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    given(pool1.getInFlight()).willReturn(0);
    given(pool3.getInFlight()).willReturn(0);
    given(pool5.getInFlight()).willReturn(100); // unhealthy

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // node5 is unhealthy -> noop
    assertThat(plan1).containsExactly(node1, node3, node5, node2, node4);
    assertThat(plan2).containsExactly(node1, node3, node5, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(never()).diceRoll1d4();
  }

  @Test
  public void should_prioritize_and_shuffle_3_or_more_replicas_when_majority_unhealthy() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    given(pool1.getInFlight()).willReturn(100);
    given(pool3.getInFlight()).willReturn(100);
    given(pool5.getInFlight()).willReturn(0);

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // majority of nodes unhealthy -> noop
    assertThat(plan1).containsExactly(node1, node3, node5, node2, node4);
    assertThat(plan2).containsExactly(node1, node3, node5, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(never()).diceRoll1d4();
  }

  @Test
  public void should_reorder_first_two_replicas_when_first_has_more_in_flight_than_second() {
    // Given
    given(request.getRoutingKeyspace()).willReturn(KEYSPACE);
    given(request.getRoutingKey()).willReturn(ROUTING_KEY);
    given(tokenMap.getReplicas(KEYSPACE, ROUTING_KEY))
        .willReturn(ImmutableSet.of(node1, node3, node5));
    given(pool1.getInFlight()).willReturn(200);
    given(pool3.getInFlight()).willReturn(100);

    // When
    Queue<Node> plan1 = dsePolicy.newQueryPlan(request, session);
    Queue<Node> plan2 = dsePolicy.newQueryPlan(request, session);

    // Then
    // nodes 1, 3 and 5 always first, round-robin on the rest
    // node1 has more in-flight than node3 -> swap
    assertThat(plan1).containsExactly(node3, node1, node5, node2, node4);
    assertThat(plan2).containsExactly(node3, node1, node5, node4, node2);

    then(dsePolicy).should(times(2)).shuffleHead(any(), anyInt());
    then(dsePolicy).should(times(2)).nanoTime();
    then(dsePolicy).should(never()).diceRoll1d4();
  }

  @Override
  protected DefaultLoadBalancingPolicy createAndInitPolicy() {
    DefaultLoadBalancingPolicy policy =
        spy(
            new DefaultLoadBalancingPolicy(context, DEFAULT_NAME) {
              @Override
              protected void shuffleHead(Object[] array, int n) {}

              @Override
              protected long nanoTime() {
                return nanoTime;
              }

              @Override
              protected int diceRoll1d4() {
                return diceRoll;
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
    return policy;
  }
}
