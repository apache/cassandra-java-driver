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

import static com.datastax.oss.driver.Assertions.assertThat;
import static com.datastax.oss.driver.api.core.config.DriverExecutionProfile.DEFAULT_NAME;
import static org.mockito.Mockito.reset;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.UUID;

public class DefaultLoadBalancingPolicyEventsTest extends BasicLoadBalancingPolicyEventsTest {

  @Override
  @NonNull
  protected DefaultLoadBalancingPolicy createAndInitPolicy() {
    DefaultLoadBalancingPolicy policy = new DefaultLoadBalancingPolicy(context, DEFAULT_NAME);
    policy.init(
        ImmutableMap.of(UUID.randomUUID(), node1, UUID.randomUUID(), node2), distanceReporter);
    assertThat(policy.getLiveNodes().dc("dc1")).containsOnly(node1, node2);
    reset(distanceReporter);
    return policy;
  }
}
