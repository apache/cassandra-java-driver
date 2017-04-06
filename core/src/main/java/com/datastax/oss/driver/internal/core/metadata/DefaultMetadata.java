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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is immutable, so that metadata changes are atomic for the client. Every mutation
 * operation must return a new instance, that will replace the existing one in {@link
 * MetadataManager}'s volatile field.
 */
public class DefaultMetadata implements Metadata {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadata.class);

  public static final DefaultMetadata EMPTY = new DefaultMetadata(Collections.emptyMap());

  private final Map<InetSocketAddress, Node> nodes;
  // TODO add metadata and schema

  public DefaultMetadata(Map<InetSocketAddress, Node> nodes) {
    this.nodes = ImmutableMap.copyOf(nodes);
  }

  @Override
  public Map<InetSocketAddress, Node> getNodes() {
    return nodes;
  }

  /** Create minimal node info about the contact points, before the first connection. */
  public DefaultMetadata initNodes(Set<InetSocketAddress> addresses) {
    assert nodes.isEmpty();
    ImmutableMap.Builder<InetSocketAddress, Node> newNodes = ImmutableMap.builder();
    for (InetSocketAddress address : addresses) {
      newNodes.put(address, new DefaultNode(address));
    }
    return new DefaultMetadata(newNodes.build());
  }

  public DefaultMetadata refreshNodes(Iterable<TopologyMonitor.NodeInfo> nodeInfos) {
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
      DefaultNode node = (DefaultNode) nodes.get(address);
      if (node == null) {
        node = new DefaultNode(address);
        LOG.debug("Adding new node {}", node);
        added.put(address, node);
      }
      copyInfos(nodeInfo, node);
    }

    Set<InetSocketAddress> removed = Sets.difference(nodes.keySet(), seen);

    if (added.isEmpty() && removed.isEmpty()) {
      return this;
    } else {
      ImmutableMap.Builder<InetSocketAddress, Node> builder = ImmutableMap.builder();
      builder.putAll(added);
      for (Map.Entry<InetSocketAddress, Node> entry : nodes.entrySet()) {
        if (!removed.contains(entry.getKey())) {
          builder.put(entry.getKey(), entry.getValue());
        }
      }
      // TODO fire node added/removed events
      // TODO recompute token map
      return new DefaultMetadata(builder.build());
    }
  }

  private void copyInfos(TopologyMonitor.NodeInfo nodeInfo, DefaultNode node) {
    // TODO add new properties in Node, fill them
  }

  public DefaultMetadata addNode(Node toAdd) {
    Map<InetSocketAddress, Node> newNodes;
    if (nodes.containsKey(toAdd.getConnectAddress())) {
      return this;
    } else {
      newNodes =
          ImmutableMap.<InetSocketAddress, Node>builder()
              .putAll(nodes)
              .put(toAdd.getConnectAddress(), toAdd)
              .build();
      // TODO recompute token map
      return new DefaultMetadata(newNodes);
    }
  }

  public DefaultMetadata removeNode(InetSocketAddress toRemove) {
    Map<InetSocketAddress, Node> newNodes;
    if (!nodes.containsKey(toRemove)) {
      return this;
    } else {
      ImmutableMap.Builder<InetSocketAddress, Node> builder = ImmutableMap.builder();
      for (Map.Entry<InetSocketAddress, Node> entry : nodes.entrySet()) {
        if (!entry.getKey().equals(toRemove)) {
          builder.put(entry.getKey(), entry.getValue());
        }
      }
      newNodes = builder.build();
      // TODO recompute token map
      return new DefaultMetadata(newNodes);
    }
  }
}
