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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Map;
import java.util.UUID;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

// TODO fix unnecessary stubbing of config option in parent class (and stop using "silent" runner)
@RunWith(MockitoJUnitRunner.Silent.class)
public class DefaultLoadBalancingPolicyDcFailoverTest
    extends BasicLoadBalancingPolicyDcFailoverTest {

  @Override
  protected DefaultLoadBalancingPolicy createAndInitPolicy() {
    when(node4.getDatacenter()).thenReturn("dc2");
    when(node5.getDatacenter()).thenReturn("dc2");
    when(node6.getDatacenter()).thenReturn("dc2");
    when(node7.getDatacenter()).thenReturn("dc3");
    when(node8.getDatacenter()).thenReturn("dc3");
    when(node9.getDatacenter()).thenReturn("dc3");
    // Accept 2 nodes per remote DC
    when(defaultProfile.getInt(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC))
        .thenReturn(2);
    when(defaultProfile.getBoolean(
            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS))
        .thenReturn(false);
    // Use a subclass to disable shuffling, we just spy to make sure that the shuffling method was
    // called (makes tests easier)
    DefaultLoadBalancingPolicy policy =
        spy(
            new DefaultLoadBalancingPolicy(context, DEFAULT_NAME) {
              @Override
              protected void shuffleHead(Object[] array, int n) {}
            });
    Map<UUID, Node> nodes =
        ImmutableMap.<UUID, Node>builder()
            .put(UUID.randomUUID(), node1)
            .put(UUID.randomUUID(), node2)
            .put(UUID.randomUUID(), node3)
            .put(UUID.randomUUID(), node4)
            .put(UUID.randomUUID(), node5)
            .put(UUID.randomUUID(), node6)
            .put(UUID.randomUUID(), node7)
            .put(UUID.randomUUID(), node8)
            .put(UUID.randomUUID(), node9)
            .build();
    policy.init(nodes, distanceReporter);
    assertThat(policy.getLiveNodes().dc("dc1")).containsExactly(node1, node2, node3);
    assertThat(policy.getLiveNodes().dc("dc2")).containsExactly(node4, node5); // only 2 allowed
    assertThat(policy.getLiveNodes().dc("dc3")).containsExactly(node7, node8); // only 2 allowed
    return policy;
  }
}
