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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.filter;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.UUID;
import org.junit.Test;

public class DcInferringLoadBalancingPolicyInitTest extends LoadBalancingPolicyTestBase {

  @Test
  public void should_use_local_dc_if_provided_via_config() {
    // Given
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    // the parent class sets the config option to "dc1"
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_use_local_dc_if_provided_via_context() {
    // Given
    when(context.getLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME)).thenReturn("dc1");
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    // note: programmatic takes priority, the config won't even be inspected so no need to stub the
    // option to null
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
    verify(defaultProfile, never())
        .getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null);
  }

  @Test
  public void should_infer_local_dc_from_contact_points() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then
    assertThat(policy.getLocalDatacenter()).isEqualTo("dc1");
  }

  @Test
  public void should_require_local_dc_if_contact_points_from_different_dcs() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    when(node2.getDatacenter()).thenReturn("dc2");
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    Throwable t =
        catchThrowable(
            () ->
                policy.init(
                    ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2),
                    distanceReporter));

    // Then
    assertThat(t)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "No local DC was provided, but the contact points are from different DCs: node1=dc1, node2=dc2");
  }

  @Test
  public void should_require_local_dc_if_contact_points_have_null_dcs() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    when(node1.getDatacenter()).thenReturn(null);
    when(node2.getDatacenter()).thenReturn(null);
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    Throwable t =
        catchThrowable(
            () ->
                policy.init(
                    ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2),
                    distanceReporter));

    // Then
    assertThat(t)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "The local DC could not be inferred from contact points, please set it explicitly");
  }

  @Test
  public void should_warn_if_contact_points_not_in_local_dc() {
    // Given
    when(node2.getDatacenter()).thenReturn("dc2");
    when(node3.getDatacenter()).thenReturn("dc3");
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2, node3));
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    verify(appender, atLeast(1)).doAppend(loggingEventCaptor.capture());
    Iterable<ILoggingEvent> warnLogs =
        filter(loggingEventCaptor.getAllValues()).with("level", Level.WARN).get();
    assertThat(warnLogs).hasSize(1);
    assertThat(warnLogs.iterator().next().getFormattedMessage())
        .contains(
            "You specified dc1 as the local DC, but some contact points are from a different DC")
        .contains("node2=dc2")
        .contains("node3=dc3");
  }

  @Test
  public void should_include_nodes_from_local_dc() {
    // Given
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    when(node1.getState()).thenReturn(NodeState.UP);
    when(node2.getState()).thenReturn(NodeState.DOWN);
    when(node3.getState()).thenReturn(NodeState.UNKNOWN);
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    // Set distance for all nodes in the local DC
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    // But only include UP or UNKNOWN nodes in the live set
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node3);
  }

  @Test
  public void should_ignore_nodes_from_remote_dcs() {
    // Given
    when(node2.getDatacenter()).thenReturn("dc2");
    when(node3.getDatacenter()).thenReturn("dc3");
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1);
  }

  @Test
  public void should_ignore_nodes_excluded_by_distance_reporter() {
    // Given
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    when(context.getNodeDistanceEvaluator(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn((node, dc) -> node.equals(node1) ? NodeDistance.IGNORED : null);

    BasicLoadBalancingPolicy policy = createPolicy();

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node2, node3);
  }

  @NonNull
  protected DcInferringLoadBalancingPolicy createPolicy() {
    return new DcInferringLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);
  }
}
