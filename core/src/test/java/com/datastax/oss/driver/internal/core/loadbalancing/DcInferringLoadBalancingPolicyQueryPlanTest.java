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

import static org.mockito.Mockito.spy;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.UUID;

public class DcInferringLoadBalancingPolicyQueryPlanTest
    extends BasicLoadBalancingPolicyQueryPlanTest {

  @Override
  protected DcInferringLoadBalancingPolicy createAndInitPolicy() {
    // Use a subclass to disable shuffling, we just spy to make sure that the shuffling method was
    // called (makes tests easier)
    NonShufflingDcInferringLoadBalancingPolicy policy =
        spy(
            new NonShufflingDcInferringLoadBalancingPolicy(
                context, DriverExecutionProfile.DEFAULT_NAME));
    policy.init(
        ImmutableMap.of(
            UUID.randomUUID(), node1,
            UUID.randomUUID(), node2,
            UUID.randomUUID(), node3,
            UUID.randomUUID(), node4,
            UUID.randomUUID(), node5),
        distanceReporter);
    return policy;
  }

  static class NonShufflingDcInferringLoadBalancingPolicy extends DcInferringLoadBalancingPolicy {
    NonShufflingDcInferringLoadBalancingPolicy(DriverContext context, String profileName) {
      super(context, profileName);
    }

    @Override
    protected void shuffleHead(Object[] currentNodes, int replicaCount) {
      // nothing (keep in same order)
    }
  }
}
