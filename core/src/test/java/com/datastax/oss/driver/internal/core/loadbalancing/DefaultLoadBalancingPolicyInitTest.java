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
import com.datastax.oss.driver.internal.core.metadata.MetadataManager;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Collections;
import org.junit.Test;

public class DefaultLoadBalancingPolicyInitTest extends DefaultLoadBalancingPolicyTestBase {

  @Test
  public void should_use_local_dc_if_provided_via_config() {
    // Given
    when(defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null))
        .thenReturn("dc1");

    // When
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    // Then
    assertThat(policy.localDc).isEqualTo("dc1");
  }

  @Test
  public void should_use_local_dc_if_provided_via_context() {
    // Given
    when(context.getLocalDatacenter(DriverExecutionProfile.DEFAULT_NAME)).thenReturn("dc1");

    // When
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    // Then
    assertThat(policy.localDc).isEqualTo("dc1");
    verify(defaultProfile, never())
        .getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null);
  }

  @Test
  public void should_infer_local_dc_if_no_explicit_contact_points() {
    // Given
    when(defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null))
        .thenReturn(null);
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(MetadataManager.DEFAULT_CONTACT_POINT, node1),
        distanceReporter,
        Collections.emptySet());

    // Then
    assertThat(policy.localDc).isEqualTo("dc1");
  }

  @Test
  public void should_require_local_dc_if_explicit_contact_points() {
    // Given
    when(defaultProfile.getString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, null))
        .thenReturn(null);
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("You provided explicit contact points, the local DC must be specified");

    // When
    policy.init(ImmutableMap.of(ADDRESS2, node2), distanceReporter, ImmutableSet.of(ADDRESS2));
  }

  @Test
  public void should_warn_if_contact_points_not_in_local_dc() {
    // Given
    when(node2.getDatacenter()).thenReturn("dc2");
    when(node3.getDatacenter()).thenReturn("dc3");
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2, ADDRESS3, node3),
        distanceReporter,
        ImmutableSet.of(ADDRESS1, ADDRESS2, ADDRESS3));

    // Then
    verify(appender, atLeast(1)).doAppend(loggingEventCaptor.capture());
    Iterable<ILoggingEvent> warnLogs =
        filter(loggingEventCaptor.getAllValues()).with("level", Level.WARN).get();
    assertThat(warnLogs).hasSize(1);
    assertThat(warnLogs.iterator().next().getFormattedMessage())
        .contains(
            "You specified dc1 as the local DC, but some contact points are from a different DC")
        .contains("/127.0.0.2:9042=dc2")
        .contains("/127.0.0.3:9042=dc3");
  }

  @Test
  public void should_include_nodes_from_local_dc() {
    // Given
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2, ADDRESS3, node3),
        distanceReporter,
        ImmutableSet.of(ADDRESS1, ADDRESS2)); // make node3 not a contact point to cover all cases

    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node3, NodeDistance.LOCAL);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1, node2, node3);
  }

  @Test
  public void should_ignore_nodes_from_remote_dcs() {
    // Given
    when(node2.getDatacenter()).thenReturn("dc2");
    when(node3.getDatacenter()).thenReturn("dc3");
    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2, ADDRESS3, node3),
        distanceReporter,
        ImmutableSet.of(ADDRESS1, ADDRESS2)); // make node3 not a contact point to cover all cases

    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1);
  }

  @Test
  public void should_ignore_nodes_excluded_by_filter() {
    // Given
    when(context.getNodeFilter(DriverExecutionProfile.DEFAULT_NAME))
        .thenReturn(node -> node.equals(node1));

    DefaultLoadBalancingPolicy policy =
        new DefaultLoadBalancingPolicy(context, DriverExecutionProfile.DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(ADDRESS1, node1, ADDRESS2, node2, ADDRESS3, node3),
        distanceReporter,
        ImmutableSet.of(ADDRESS1, ADDRESS2)); // make node3 not a contact point to cover all cases

    // Then
    verify(distanceReporter).setDistance(node1, NodeDistance.LOCAL);
    verify(distanceReporter).setDistance(node2, NodeDistance.IGNORED);
    verify(distanceReporter).setDistance(node3, NodeDistance.IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactlyInAnyOrder(node1);
  }
}
