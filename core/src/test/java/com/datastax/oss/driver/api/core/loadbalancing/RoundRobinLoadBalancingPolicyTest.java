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

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy.DistanceReporter;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static org.assertj.core.api.Assertions.assertThat;

public class RoundRobinLoadBalancingPolicyTest {
  @Mock private DriverContext context;
  @Mock private DistanceReporter distanceReporter;
  @Mock private Node node1, node2, node3;

  private RoundRobinLoadBalancingPolicy policy;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    Mockito.when(node1.getState()).thenReturn(NodeState.UP);
    Mockito.when(node2.getState()).thenReturn(NodeState.UP);
    Mockito.when(node3.getState()).thenReturn(NodeState.UP);

    policy = new RoundRobinLoadBalancingPolicy(context);
  }

  @Test
  public void should_set_all_nodes_to_local_distance() {
    policy.init(ImmutableSet.of(node1, node2, node3), distanceReporter);

    Mockito.verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
  }

  @Test
  public void should_return_round_robin_query_plans() {
    policy.init(ImmutableSet.of(node1, node2, node3), distanceReporter);

    assertThat(policy.newQueryPlan()).containsExactly(node1, node2, node3);
    assertThat(policy.newQueryPlan()).containsExactly(node2, node3, node1);
    assertThat(policy.newQueryPlan()).containsExactly(node3, node1, node2);
  }

  @Test
  public void should_only_include_unknown_or_up_nodes_at_init() {
    Mockito.when(node1.getState()).thenReturn(NodeState.DOWN);
    Mockito.when(node2.getState()).thenReturn(NodeState.UP);
    Mockito.when(node3.getState()).thenReturn(NodeState.UNKNOWN);

    policy.init(ImmutableSet.of(node1, node2, node3), distanceReporter);

    Mockito.verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);

    assertThat(policy.newQueryPlan()).containsExactly(node2, node3);
  }

  @Test
  public void should_remove_node_from_query_plan_if_down() {
    policy.init(ImmutableSet.of(node1, node2, node3), distanceReporter);

    Mockito.verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);

    assertThat(policy.newQueryPlan()).containsExactly(node1, node2, node3);

    policy.onDown(node1);

    assertThat(policy.newQueryPlan()).containsExactly(node3, node2);
    Mockito.verifyNoMoreInteractions(distanceReporter);
  }

  @Test
  public void should_add_node_to_query_plan_if_up() {
    Mockito.when(node1.getState()).thenReturn(NodeState.DOWN);

    policy.init(ImmutableSet.of(node1, node2, node3), distanceReporter);

    Mockito.verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);

    assertThat(policy.newQueryPlan()).containsExactly(node2, node3);

    policy.onUp(node1);

    assertThat(policy.newQueryPlan()).containsExactly(node3, node1, node2);
    Mockito.verifyNoMoreInteractions(distanceReporter);
  }
}
