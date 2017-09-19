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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
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

  @VisibleForTesting final Iterable<NodeInfo> nodeInfos;
  private final TokenFactoryRegistry tokenFactoryRegistry;

  FullNodeListRefresh(Iterable<NodeInfo> nodeInfos, InternalDriverContext context) {
    super(context.clusterName());
    this.nodeInfos = nodeInfos;
    this.tokenFactoryRegistry = context.tokenFactoryRegistry();
  }

  @Override
  public Result compute(DefaultMetadata oldMetadata, boolean tokenMapEnabled) {
    Map<InetSocketAddress, Node> oldNodes = oldMetadata.getNodes();

    Map<InetSocketAddress, Node> added = new HashMap<>();
    Set<InetSocketAddress> seen = new HashSet<>();

    TokenFactory tokenFactory =
        oldMetadata.getTokenMap().map(m -> ((DefaultTokenMap) m).getTokenFactory()).orElse(null);
    boolean tokensChanged = false;

    for (NodeInfo nodeInfo : nodeInfos) {
      InetSocketAddress address = nodeInfo.getConnectAddress();
      if (address == null) {
        LOG.warn("[{}] Got node info with no connect address, ignoring", logPrefix);
        continue;
      }
      seen.add(address);
      DefaultNode node = (DefaultNode) oldNodes.get(address);
      if (node == null) {
        node = new DefaultNode(address);
        LOG.debug("[{}] Adding new node {}", logPrefix, node);
        added.put(address, node);
      }
      if (tokenFactory == null && nodeInfo.getPartitioner() != null) {
        tokenFactory = tokenFactoryRegistry.tokenFactoryFor(nodeInfo.getPartitioner());
      }
      tokensChanged |= copyInfos(nodeInfo, node, tokenFactory, logPrefix);
    }

    Set<InetSocketAddress> removed = Sets.difference(oldNodes.keySet(), seen);

    if (added.isEmpty() && removed.isEmpty()) {
      // Edge case: if all the nodes of the cluster were listed as contact points, and this is the
      // first refresh, we get here so we need to set the token factory and trigger a token map
      // rebuild:
      if (!oldMetadata.getTokenMap().isPresent() && tokenFactory != null) {
        return new Result(
            oldMetadata.withNodes(oldMetadata.getNodes(), tokenMapEnabled, true, tokenFactory));
      } else {
        return new Result(oldMetadata);
      }
    } else {
      ImmutableMap.Builder<InetSocketAddress, Node> newNodesBuilder = ImmutableMap.builder();
      ImmutableList.Builder<Object> eventsBuilder = ImmutableList.builder();

      newNodesBuilder.putAll(added);
      for (Map.Entry<InetSocketAddress, Node> entry : oldNodes.entrySet()) {
        if (!removed.contains(entry.getKey())) {
          newNodesBuilder.put(entry.getKey(), entry.getValue());
        }
      }

      for (Node node : added.values()) {
        eventsBuilder.add(NodeStateEvent.added((DefaultNode) node));
      }
      for (InetSocketAddress address : removed) {
        Node node = oldNodes.get(address);
        eventsBuilder.add(NodeStateEvent.removed((DefaultNode) node));
      }

      return new Result(
          oldMetadata.withNodes(
              newNodesBuilder.build(), tokenMapEnabled, tokensChanged, tokenFactory),
          eventsBuilder.build());
    }
  }
}
