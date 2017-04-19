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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FullNodeListRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(FullNodeListRefresh.class);
  private final Iterable<TopologyMonitor.NodeInfo> nodeInfos;

  FullNodeListRefresh(DefaultMetadata current, Iterable<TopologyMonitor.NodeInfo> nodeInfos) {
    super(current);
    this.nodeInfos = nodeInfos;
  }

  protected Map<InetSocketAddress, Node> computeNewNodes() {

    Map<InetSocketAddress, Node> oldNodes = oldMetadata.getNodes();

    Map<InetSocketAddress, Node> added = new HashMap<>();
    Set<InetSocketAddress> seen = new HashSet<>();

    for (TopologyMonitor.NodeInfo nodeInfo : nodeInfos) {
      InetSocketAddress address = nodeInfo.getConnectAddress();
      if (address == null) {
        // TODO more advanced row validation (see 3.x), here or in TopologyMonitor?
        LOG.debug("Ignoring node info with missing connect address");
        continue;
      }
      seen.add(address);
      DefaultNode node = (DefaultNode) oldNodes.get(address);
      if (node == null) {
        node = new DefaultNode(address);
        LOG.debug("Adding new node {}", node);
        added.put(address, node);
      }
      copyInfos(nodeInfo, node);
    }

    Set<InetSocketAddress> removed = Sets.difference(oldNodes.keySet(), seen);

    if (added.isEmpty() && removed.isEmpty()) {
      return oldNodes;
    } else {
      ImmutableMap.Builder<InetSocketAddress, Node> newNodesBuilder = ImmutableMap.builder();
      newNodesBuilder.putAll(added);
      for (Map.Entry<InetSocketAddress, Node> entry : oldNodes.entrySet()) {
        if (!removed.contains(entry.getKey())) {
          newNodesBuilder.put(entry.getKey(), entry.getValue());
        }
      }

      for (Node node : added.values()) {
        events.add(NodeStateEvent.added((DefaultNode) node));
      }
      for (InetSocketAddress address : removed) {
        Node node = oldNodes.get(address);
        events.add(NodeStateEvent.removed((DefaultNode) node));
      }

      return newNodesBuilder.build();
    }
  }
}
