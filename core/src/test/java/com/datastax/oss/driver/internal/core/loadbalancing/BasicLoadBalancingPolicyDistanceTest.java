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
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistanceEvaluator;
import com.datastax.oss.driver.api.core.metadata.Node;
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
public class BasicLoadBalancingPolicyDistanceTest extends LoadBalancingPolicyTestBase {

  @Mock private NodeDistanceEvaluator nodeDistanceEvaluator;

  private ImmutableMap<UUID, Node> nodes;

  @Before
  @Override
  public void setup() {
    super.setup();
    when(context.getNodeDistanceEvaluator(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(nodeDistanceEvaluator);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2, node3));
    nodes =
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3);
  }

  @Test
  public void should_report_distance_reported_by_user_distance_reporter() {
    // Given
    given(node2.getDatacenter()).willReturn("dc2");
    given(nodeDistanceEvaluator.evaluateDistance(node1, "dc1")).willReturn(NodeDistance.LOCAL);
    given(nodeDistanceEvaluator.evaluateDistance(node2, "dc1")).willReturn(NodeDistance.REMOTE);
    given(nodeDistanceEvaluator.evaluateDistance(node3, "dc1")).willReturn(NodeDistance.IGNORED);
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1);
  }

  @Test
  public void should_report_LOCAL_when_dc_agnostic() {
    // Given
    given(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .willReturn(false);
    given(node1.getDatacenter()).willReturn(null);
    given(node2.getDatacenter()).willReturn("dc1");
    given(node3.getDatacenter()).willReturn("dc2");
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    assertThat(policy.getLiveNodes().dc(null)).containsExactly(node1, node2, node3);
  }

  @Test
  public void should_report_LOCAL_when_node_in_local_dc() {
    // Given
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2, node3);
  }

  @Test
  public void should_report_IGNORED_when_node_not_in_local_dc() {
    // Given
    given(node1.getDatacenter()).willReturn(null);
    given(node2.getDatacenter()).willReturn("dc2");
    given(node3.getDatacenter()).willReturn("dc3");
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    // Then
    // Note: driver 3 would have reported LOCAL for node1 since its datacenter is null
    verify(distanceReporter).setDistance(node1, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node2, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc(null)).isEmpty();
    assertThat(policy.getLiveNodes().dc("dc1")).isEmpty();
    assertThat(policy.getLiveNodes().dc("dc2")).isEmpty();
    assertThat(policy.getLiveNodes().dc("dc3")).isEmpty();
  }

  @Test
  public void should_report_REMOTE_when_node_not_in_local_dc_and_dc_failover_enabled() {
    // Given
    given(node1.getDatacenter()).willReturn("dc2");
    given(node2.getDatacenter()).willReturn("dc3");
    given(node3.getDatacenter()).willReturn("dc4");
    given(
            defaultProfile.getInt(
                DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .willReturn(1);
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node2, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node3, NodeDistance.REMOTE);
    assertThat(policy.getLiveNodes().dc("dc1")).isEmpty();
    assertThat(policy.getLiveNodes().dc("dc2")).containsExactly(node1);
    assertThat(policy.getLiveNodes().dc("dc3")).containsExactly(node2);
    assertThat(policy.getLiveNodes().dc("dc4")).containsExactly(node3);
  }

  @Test
  public void should_report_IGNORED_when_node_not_in_local_dc_and_too_many_nodes_for_dc_failover() {
    // Given
    given(node1.getDatacenter()).willReturn("dc2");
    given(node2.getDatacenter()).willReturn("dc2");
    given(node3.getDatacenter()).willReturn("dc2");
    given(
            defaultProfile.getInt(
                DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .willReturn(2);
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node2, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).isEmpty();
    assertThat(policy.getLiveNodes().dc("dc2")).containsExactly(node1, node2);
  }

  @Test
  public void should_report_REMOTE_when_remote_node_up_and_dc_failover() {
    // Given
    given(node1.getDatacenter()).willReturn("dc2");
    given(node2.getDatacenter()).willReturn("dc2");
    given(node3.getDatacenter()).willReturn("dc2");
    given(node4.getDatacenter()).willReturn("dc2");
    given(
            defaultProfile.getInt(
                DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .willReturn(4);
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    policy.onUp(node4);
    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node2, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node3, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node4, NodeDistance.REMOTE);
    assertThat(policy.getLiveNodes().dc("dc1")).isEmpty();
    assertThat(policy.getLiveNodes().dc("dc2")).containsExactly(node1, node2, node3, node4);
  }

  @Test
  public void should_report_IGNORED_when_remote_node_up_and_too_many_nodes_for_dc_failover() {
    // Given
    given(node1.getDatacenter()).willReturn("dc2");
    given(node2.getDatacenter()).willReturn("dc2");
    given(node3.getDatacenter()).willReturn("dc2");
    given(node4.getDatacenter()).willReturn("dc2");
    given(
            defaultProfile.getInt(
                DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .willReturn(3);
    BasicLoadBalancingPolicy policy = createPolicy();
    // When
    policy.init(nodes, distanceReporter);
    policy.onUp(node4);
    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node2, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node3, NodeDistance.REMOTE);
    verify(distanceReporter).setDistance(node4, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).isEmpty();
    assertThat(policy.getLiveNodes().dc("dc2")).containsExactly(node1, node2, node3);
  }

  @NonNull
  protected BasicLoadBalancingPolicy createPolicy() {
    return new BasicLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
  }
}
