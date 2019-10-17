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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.UUID;
import org.junit.Test;

/** Specific tests for the DC inferring logic in LoadBalancingPolicyBase. */
public class LoadBalancingPolicyBaseInitTest extends DefaultLoadBalancingPolicyTestBase {

  @Override
  public void setup() {
    super.setup();
    reset(defaultProfile);
  }

  @Test
  public void should_infer_local_dc_even_with_explicit_contact_points() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    LoadBalancingPolicyBase policy =
        new LoadBalancingPolicyBase(context, DriverExecutionProfile.DEFAULT_NAME) {};

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
    LoadBalancingPolicyBase policy =
        new LoadBalancingPolicyBase(context, DriverExecutionProfile.DEFAULT_NAME) {};

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "No local DC was provided, but the contact points are from different DCs: node1=dc1, node2=dc2");

    // When
    policy.init(
        ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2), distanceReporter);
  }

  @Test
  public void should_require_local_dc_if_contact_points_have_null_dcs() {
    // Given
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1, node2));
    when(node1.getDatacenter()).thenReturn(null);
    when(node2.getDatacenter()).thenReturn(null);
    LoadBalancingPolicyBase policy =
        new LoadBalancingPolicyBase(context, DriverExecutionProfile.DEFAULT_NAME) {};

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(
        "The local DC could not be inferred from contact points, please set it explicitly");

    // When
    policy.init(
        ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2), distanceReporter);
  }
}
