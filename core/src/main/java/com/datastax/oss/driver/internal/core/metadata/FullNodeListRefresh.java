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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
class FullNodeListRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(FullNodeListRefresh.class);

  @VisibleForTesting final Iterable<NodeInfo> nodeInfos;

  FullNodeListRefresh(Iterable<NodeInfo> nodeInfos) {
    this.nodeInfos = nodeInfos;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {

    String logPrefix = context.getSessionName();
    TokenFactoryRegistry tokenFactoryRegistry = context.getTokenFactoryRegistry();

    Map<UUID, Node> oldNodes = oldMetadata.getNodes();

    Map<UUID, Node> added = new HashMap<>();
    Set<UUID> seen = new HashSet<>();

    TokenFactory tokenFactory =
        oldMetadata.getTokenMap().map(m -> ((DefaultTokenMap) m).getTokenFactory()).orElse(null);
    boolean tokensChanged = false;

    for (NodeInfo nodeInfo : nodeInfos) {
      UUID id = nodeInfo.getHostId();
      if (seen.contains(id)) {
        LOG.warn(
            "[{}] Found duplicate entries with host_id {} in system.peers, "
                + "keeping only the first one",
            logPrefix,
            id);
      } else {
        seen.add(id);
        DefaultNode node = (DefaultNode) oldNodes.get(id);
        if (node == null) {
          node = new DefaultNode(nodeInfo.getEndPoint(), context);
          LOG.debug("[{}] Adding new node {}", logPrefix, node);
          added.put(id, node);
        }
        if (tokenFactory == null && nodeInfo.getPartitioner() != null) {
          tokenFactory = tokenFactoryRegistry.tokenFactoryFor(nodeInfo.getPartitioner());
        }
        tokensChanged |= copyInfos(nodeInfo, node, context);
      }
    }

    Set<UUID> removed = Sets.difference(oldNodes.keySet(), seen);

    if (added.isEmpty() && removed.isEmpty()) { // The list didn't change
      if (!oldMetadata.getTokenMap().isPresent() && tokenFactory != null) {
        // First time we found out what the partitioner is => set the token factory and trigger a
        // token map rebuild:
        return new Result(
            oldMetadata.withNodes(
                oldMetadata.getNodes(), tokenMapEnabled, true, tokenFactory, context));
      } else {
        // No need to create a new metadata instance
        return new Result(oldMetadata);
      }
    } else {
      ImmutableMap.Builder<UUID, Node> newNodesBuilder = ImmutableMap.builder();
      ImmutableList.Builder<Object> eventsBuilder = ImmutableList.builder();

      newNodesBuilder.putAll(added);
      for (Map.Entry<UUID, Node> entry : oldNodes.entrySet()) {
        if (!removed.contains(entry.getKey())) {
          newNodesBuilder.put(entry.getKey(), entry.getValue());
        }
      }

      for (Node node : added.values()) {
        eventsBuilder.add(NodeStateEvent.added((DefaultNode) node));
      }
      for (UUID id : removed) {
        Node node = oldNodes.get(id);
        eventsBuilder.add(NodeStateEvent.removed((DefaultNode) node));
      }

      return new Result(
          oldMetadata.withNodes(
              newNodesBuilder.build(), tokenMapEnabled, tokensChanged, tokenFactory, context),
          eventsBuilder.build());
    }
  }
}
