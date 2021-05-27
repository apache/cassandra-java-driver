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
package com.datastax.oss.driver.internal.core.loadbalancing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

// TODO fix unnecessary stubbing of config option in parent class (and stop using "silent" runner)
@RunWith(MockitoJUnitRunner.Silent.class)
public class BasicLoadBalancingPolicyEventsTest extends LoadBalancingPolicyTestBase {

  @Mock private NodeDistanceEvaluator nodeDistanceEvaluator;

  private BasicLoadBalancingPolicy policy;

  @Before
  @Override
  public void setup() {
    super.setup();
    when(context.getNodeDistanceEvaluator(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(nodeDistanceEvaluator);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    policy = createAndInitPolicy();
    reset(distanceReporter);
  }

  @Test
  public void should_remove_down_node_from_live_set() {
    // When
    policy.onDown(node2);

    // Then
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1);
    verify(distanceReporter, never()).setDistance(eq(node2), any(NodeDistance.class));
    // should have been called only once, during initialization, but not during onDown
    verify(nodeDistanceEvaluator).evaluateDistance(node2, "dc1");
  }

  @Test
  public void should_remove_removed_node_from_live_set() {
    // When
    policy.onRemove(node2);

    // Then
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1);
    verify(distanceReporter, never()).setDistance(eq(node2), any(NodeDistance.class));
    // should have been called only once, during initialization, but not during onRemove
    verify(nodeDistanceEvaluator).evaluateDistance(node2, "dc1");
  }

  @Test
  public void should_set_added_node_to_local() {
    // When
    policy.onAdd(node3);

    // Then
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    verify(nodeDistanceEvaluator).evaluateDistance(node3, "dc1");
    // Not added to the live set yet, we're waiting for the pool to open
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2);
  }

  @Test
  public void should_ignore_added_node_when_filtered() {
    // Given
    when(nodeDistanceEvaluator.evaluateDistance(node3, "dc1")).thenReturn(NodeDistance.IGNORED);

    // When
    policy.onAdd(node3);

    // Then
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2);
  }

  @Test
  public void should_ignore_added_node_when_remote_dc() {
    // Given
    when(node3.getDatacenter()).thenReturn("dc2");

    // When
    policy.onAdd(node3);

    // Then
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2);
    assertThat(policy.getLiveNodes().dc("dc2")).isEmpty();
  }

  @Test
  public void should_add_up_node_to_live_set() {
    // When
    policy.onUp(node3);

    // Then
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    verify(nodeDistanceEvaluator).evaluateDistance(node3, "dc1");
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2, node3);
  }

  @Test
  public void should_ignore_up_node_when_filtered() {
    // Given
    when(nodeDistanceEvaluator.evaluateDistance(node3, "dc1")).thenReturn(NodeDistance.IGNORED);

    // When
    policy.onUp(node3);

    // Then
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    verify(nodeDistanceEvaluator).evaluateDistance(node3, "dc1");
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2);
  }

  @Test
  public void should_ignore_up_node_when_remote_dc() {
    // Given
    when(node3.getDatacenter()).thenReturn("dc2");

    // When
    policy.onUp(node3);

    // Then
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2);
    assertThat(policy.getLiveNodes().dc("dc2")).isEmpty();
  }

  @NonNull
  protected BasicLoadBalancingPolicy createAndInitPolicy() {
    BasicLoadBalancingPolicy policy =
        new BasicLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
    policy.init(
        ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2), distanceReporter);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2);
    return policy;
  }
}
