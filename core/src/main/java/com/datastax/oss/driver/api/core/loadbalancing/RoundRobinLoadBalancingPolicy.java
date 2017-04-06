/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.api.core.loadbalancing;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.ImmutableList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;

public class RoundRobinLoadBalancingPolicy implements LoadBalancingPolicy {

  private static final IntUnaryOperator INCREMENT = i -> (i == Integer.MAX_VALUE) ? 0 : i + 1;

  private final AtomicInteger startIndex = new AtomicInteger();
  private ImmutableList<Node> nodes;

  public RoundRobinLoadBalancingPolicy(@SuppressWarnings("unused") DriverContext context) {
    // nothing to do
  }

  @Override
  public void init(Set<Node> nodes, DistanceReporter distanceReporter) {
    // TODO handle node states (this is a temporary impl. to kickstart things)
    this.nodes = ImmutableList.copyOf(nodes);
    for (Node node : this.nodes) {
      distanceReporter.setDistance(node, NodeDistance.LOCAL);
    }
  }

  @Override
  public Queue<Node> newQueryPlan() {
    int myStartIndex = startIndex.getAndUpdate(INCREMENT);
    ConcurrentLinkedQueue<Node> plan = new ConcurrentLinkedQueue<>();
    for (int i = 0; i < nodes.size(); i++) {
      plan.offer(nodes.get((myStartIndex + i) % nodes.size()));
    }
    return plan;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
