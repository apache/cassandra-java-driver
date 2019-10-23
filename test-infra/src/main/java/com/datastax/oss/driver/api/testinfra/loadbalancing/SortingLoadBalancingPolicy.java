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
package com.datastax.oss.driver.api.testinfra.loadbalancing;

import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public class SortingLoadBalancingPolicy implements LoadBalancingPolicy {

  private final Set<Node> nodes = new TreeSet<>(NodeComparator.INSTANCE);

  @SuppressWarnings("unused")
  public SortingLoadBalancingPolicy(DriverContext context, String profileName) {
    // constructor needed for loading via config.
  }

  public SortingLoadBalancingPolicy() {}

  @Override
  public void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter) {
    this.nodes.addAll(nodes.values());
    this.nodes.forEach(n -> distanceReporter.setDistance(n, NodeDistance.LOCAL));
  }

  @NonNull
  @Override
  public Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session) {
    return new ArrayDeque<>(nodes);
  }

  @Override
  public void onAdd(@NonNull Node node) {
    this.nodes.add(node);
  }

  @Override
  public void onUp(@NonNull Node node) {
    onAdd(node);
  }

  @Override
  public void onDown(@NonNull Node node) {
    onRemove(node);
  }

  @Override
  public void onRemove(@NonNull Node node) {
    this.nodes.remove(node);
  }

  @Override
  public void close() {}
}
