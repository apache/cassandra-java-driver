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

import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.net.InetAddress;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;

public class SortingLoadBalancingPolicy implements LoadBalancingPolicy {

  @SuppressWarnings("unused")
  public SortingLoadBalancingPolicy(DriverContext context, DriverOption option) {
    // constructor needed for loading via config.
  }

  private byte[] empty = {};
  private final Set<Node> nodes =
      new TreeSet<>(
          (node1, node2) -> {
            // compare address bytes, byte by byte.
            byte[] address1 =
                node1.getBroadcastAddress().map(InetAddress::getAddress).orElse(empty);
            byte[] address2 =
                node2.getBroadcastAddress().map(InetAddress::getAddress).orElse(empty);

            // ipv6 vs ipv4, favor ipv6.
            if (address1.length != address2.length) {
              return address1.length - address2.length;
            }

            for (int i = 0; i < address1.length; i++) {
              int b1 = address1[i] & 0xFF;
              int b2 = address2[i] & 0xFF;
              if (b1 != b2) {
                return b1 - b2;
              }
            }
            return 0;
          });

  public SortingLoadBalancingPolicy() {}

  @Override
  public void init(Set<Node> nodes, DistanceReporter distanceReporter) {
    this.nodes.addAll(nodes);
    this.nodes.forEach(n -> distanceReporter.setDistance(n, NodeDistance.LOCAL));
  }

  @Override
  public Queue<Node> newQueryPlan() {
    return new LinkedList<>(nodes);
  }

  @Override
  public void onAdd(Node node) {
    this.nodes.add(node);
  }

  @Override
  public void onUp(Node node) {
    onAdd(node);
  }

  @Override
  public void onDown(Node node) {
    onRemove(node);
  }

  @Override
  public void onRemove(Node node) {
    this.nodes.remove(node);
  }

  @Override
  public void close() {}
}
