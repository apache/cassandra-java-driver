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
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.util.Optional;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

// TODO fix unnecessary stubbing of config option in parent class (and stop using "silent" runner)
@RunWith(MockitoJUnitRunner.Silent.class)
public class BasicLoadBalancingPolicyDcAgnosticTest extends BasicLoadBalancingPolicyQueryPlanTest {

  @Before
  @Override
  public void setup() {
    super.setup();
    when(defaultProfile.isDefined(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER))
        .thenReturn(false);
    when(metadataManager.getContactPoints()).thenReturn(ImmutableSet.of(node1));
    when(metadataManager.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenAnswer(invocation -> Optional.of(this.tokenMap));

    // since there is no local datacenter defined, the policy should behave with DC awareness
    // disabled and pick nodes regardless of their datacenters; we therefore expect all tests of
    // BasicLoadBalancingPolicyQueryPlanTest to pass even with the below DC distribution.
    when(node1.getDatacenter()).thenReturn("dc1");
    when(node2.getDatacenter()).thenReturn("dc2");
    when(node3.getDatacenter()).thenReturn("dc3");
    when(node4.getDatacenter()).thenReturn("dc4");
    when(node5.getDatacenter()).thenReturn(null);

    policy = createAndInitPolicy();

    assertThat(policy.getLiveNodes().dc(null)).containsExactly(node1, node2, node3, node4, node5);
    assertThat(policy.getLiveNodes().dcs()).isEmpty();
  }
}
