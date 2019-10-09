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
package com.datastax.dse.driver.internal.core.loadbalancing;

import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_FILTER_CLASS;
import static com.datastax.oss.driver.api.core.config.DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER;
import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static com.datastax.oss.driver.api.core.loadbalancing.NodeDistance.IGNORED;
import static com.datastax.oss.driver.api.core.loadbalancing.NodeDistance.LOCAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.filter;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.verify;
import static org.mockito.BDDMockito.when;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.UUID;
import java.util.function.Predicate;
import org.junit.Test;

public class DseLoadBalancingPolicyInitTest extends DseLoadBalancingPolicyTestBase {

  @Test
  public void should_infer_local_dc_if_no_explicit_contact_points() {
    // Given
    given(profile.getString(LOAD_BALANCING_LOCAL_DATACENTER, null)).willReturn(null);
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    given(metadataManager.wasImplicitContactPoint()).willReturn(true);
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // When
    policy.init(ImmutableMap.of(UUID.randomUUID(), node1), distanceReporter);

    // Then
    assertThat(policy.localDc).isEqualTo("dc1");
  }

  @Test
  public void should_require_local_dc_if_explicit_contact_points() {
    // Given
    given(profile.getString(LOAD_BALANCING_LOCAL_DATACENTER, null)).willReturn(null);
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node2));
    given(metadataManager.wasImplicitContactPoint()).willReturn(false);
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // When
    Throwable error =
        catchThrowable(
            () -> policy.init(ImmutableMap.of(UUID.randomUUID(), node2), distanceReporter));

    // Then
    assertThat(error)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "You provided explicit contact points, the local DC must be specified");
  }

  @Test
  public void should_warn_if_contact_points_not_in_local_dc() {
    // Given
    given(node2.getDatacenter()).willReturn("dc2");
    given(node3.getDatacenter()).willReturn("dc3");
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1, node2, node3));
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    then(appender).should(atLeast(1)).doAppend(loggingEventCaptor.capture());
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
  public void should_not_warn_if_contact_points_not_in_local_dc_and_profile_not_default() {
    // Given
    given(node2.getDatacenter()).willReturn("dc2");
    given(node3.getDatacenter()).willReturn("dc3");
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1, node2, node3));
    given(config.getProfile("Non default")).willReturn(profile);
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, "Non default");

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    then(appender).should(never()).doAppend(loggingEventCaptor.capture());
    Iterable<ILoggingEvent> warnLogs =
        filter(loggingEventCaptor.getAllValues()).with("level", Level.WARN).get();
    assertThat(warnLogs).isEmpty();
  }

  @Test
  public void should_include_nodes_from_local_dc() {
    // Given
    // make node3 not a contact point to cover all cases
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1, node2));
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);
    given(node1.getState()).willReturn(NodeState.UP);
    given(node2.getState()).willReturn(NodeState.DOWN);
    given(node3.getState()).willReturn(NodeState.UNKNOWN);

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    // Set distance for all nodes in the local DC
    then(distanceReporter).should().setDistance(node1, LOCAL);
    then(distanceReporter).should().setDistance(node2, LOCAL);
    then(distanceReporter).should().setDistance(node3, LOCAL);
    // But only include UP or UNKNOWN nodes in the live set
    assertThat(policy.localDcLiveNodes).containsExactly(node1, node3);
  }

  @Test
  public void should_ignore_nodes_from_remote_dcs() {
    // Given
    given(node2.getDatacenter()).willReturn("dc2");
    given(node3.getDatacenter()).willReturn("dc3");
    // make node3 not a contact point to cover all cases
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1, node2));
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    then(distanceReporter).should().setDistance(node1, LOCAL);
    then(distanceReporter).should().setDistance(node2, IGNORED);
    then(distanceReporter).should().setDistance(node3, IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactly(node1);
  }

  @Test
  public void should_ignore_nodes_excluded_by_programmatic_filter() {
    // Given
    given(filter.test(node2)).willReturn(false);
    given(filter.test(node3)).willReturn(false);
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    then(distanceReporter).should().setDistance(node1, LOCAL);
    then(distanceReporter).should().setDistance(node2, IGNORED);
    then(distanceReporter).should().setDistance(node3, IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactly(node1);
  }

  @Test
  public void should_ignore_nodes_excluded_by_configured_filter() {
    // Given
    given(context.getNodeFilter(DEFAULT_NAME)).willReturn(null);
    given(metadataManager.getContactPoints()).willReturn(ImmutableSet.of(node1));
    given(profile.isDefined(LOAD_BALANCING_FILTER_CLASS)).willReturn(true);
    given(profile.getString(LOAD_BALANCING_FILTER_CLASS)).willReturn(MyFilter.class.getName());
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // When
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1, UUID.randomUUID(), node2, UUID.randomUUID(), node3),
        distanceReporter);

    // Then
    then(distanceReporter).should().setDistance(node1, LOCAL);
    then(distanceReporter).should().setDistance(node2, IGNORED);
    then(distanceReporter).should().setDistance(node3, IGNORED);
    assertThat(policy.localDcLiveNodes).containsExactly(node1);
  }

  @Test
  public void should_use_local_dc_if_provided_via_config() {
    // Given
    // the parent class sets the config option to "dc1"

    // When
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // Then
    assertThat(policy.localDc).isEqualTo("dc1");
  }

  @Test
  public void should_use_local_dc_if_provided_via_context() {
    // Given
    when(context.getLocalDatacenter(DEFAULT_NAME)).thenReturn("dc1");
    // note: programmatic takes priority, the config won't even be inspected so no need to stub the
    // option to null

    // When
    DseLoadBalancingPolicy policy = new DseLoadBalancingPolicy(context, DEFAULT_NAME);

    // Then
    assertThat(policy.localDc).isEqualTo("dc1");
    verify(profile, never()).getString(LOAD_BALANCING_LOCAL_DATACENTER, null);
  }

  public static class MyFilter implements Predicate<Node> {
    @SuppressWarnings("unused")
    public MyFilter(DriverContext context, String profileName) {}

    @Override
    public boolean test(Node node) {
      return node.toString().equals("node1");
    }
  }
}
