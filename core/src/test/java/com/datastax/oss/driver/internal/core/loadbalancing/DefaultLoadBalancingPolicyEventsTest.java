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

import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultLoadBalancingPolicyEventsTest extends DefaultLoadBalancingPolicyTestBase {

  private DefaultLoadBalancingPolicy policy;

  @Before
  @Override
  public void setup() {
    super.setup();

    policy = new DefaultLoadBalancingPolicy("dc1", filter, context, true);
    policy.init(
        ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2),
        distanceReporter,
        ImmutableSet.of(ADDRESS1));
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2);

    Mockito.reset(distanceReporter);
  }

  @Test
  public void should_remove_down_node_from_live_set() {
    // When
    policy.onDown(node2);

    // Then
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1);
    Mockito.verify(distanceReporter, never()).setDistance(eq(node2), any(NodeDistance.class));
  }

  @Test
  public void should_remove_down_node_from_live_set_when_filtered() {
    Mockito.when(filter.test(node2)).thenReturn(true);
    should_remove_down_node_from_live_set();
  }

  @Test
  public void should_remove_removed_node_from_live_set() {
    // When
    policy.onRemove(node2);

    // Then
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1);
    Mockito.verify(distanceReporter, never()).setDistance(eq(node2), any(NodeDistance.class));
  }

  @Test
  public void should_remove_removed_node_from_live_set_when_filtered() {
    Mockito.when(filter.test(node2)).thenReturn(true);
    should_remove_removed_node_from_live_set();
  }

  @Test
  public void should_set_added_node_to_local() {
    // When
    policy.onAdd(node3);

    // Then
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    // Not added to the live set yet, we're waiting for the pool to open
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void should_ignore_added_node_when_filtered() {
    // Given
    Mockito.when(filter.test(node3)).thenReturn(false);

    // When
    policy.onAdd(node3);

    // Then
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void should_ignore_added_node_when_remote_dc() {
    // Given
    Mockito.when(node3.getDatacenter()).thenReturn("dc2");

    // When
    policy.onAdd(node3);

    // Then
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void should_add_up_node_to_live_set() {
    // When
    policy.onUp(node3);

    // Then
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2, node3);
  }

  @Test
  public void should_ignore_up_node_when_filtered() {
    // Given
    Mockito.when(filter.test(node3)).thenReturn(false);

    // When
    policy.onUp(node3);

    // Then
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2);
  }

  @Test
  public void should_ignore_up_node_when_remote_dc() {
    // Given
    Mockito.when(node3.getDatacenter()).thenReturn("dc2");

    // When
    policy.onUp(node3);

    // Then
    Mockito.verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2);
  }
}
