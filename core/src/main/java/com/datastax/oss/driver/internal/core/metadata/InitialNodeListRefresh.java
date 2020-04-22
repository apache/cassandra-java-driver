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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The first node list refresh: contact points are not in the metadata yet, we need to copy them
 * over.
 */
@ThreadSafe
class InitialNodeListRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(InitialNodeListRefresh.class);

  @VisibleForTesting final Iterable<NodeInfo> nodeInfos;
  @VisibleForTesting final Set<DefaultNode> contactPoints;

  InitialNodeListRefresh(Iterable<NodeInfo> nodeInfos, Set<DefaultNode> contactPoints) {
    this.nodeInfos = nodeInfos;
    this.contactPoints = contactPoints;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {

    String logPrefix = context.getSessionName();
    TokenFactoryRegistry tokenFactoryRegistry = context.getTokenFactoryRegistry();

    // Since this is the first refresh, and we've stored contact points separately until now, the
    // metadata is empty.
    assert oldMetadata == DefaultMetadata.EMPTY;
    TokenFactory tokenFactory = null;

    Map<UUID, DefaultNode> newNodes = new HashMap<>();

    for (NodeInfo nodeInfo : nodeInfos) {
      UUID hostId = nodeInfo.getHostId();
      if (newNodes.containsKey(hostId)) {
        LOG.warn(
            "[{}] Found duplicate entries with host_id {} in system.peers, "
                + "keeping only the first one",
            logPrefix,
            hostId);
      } else {
        EndPoint endPoint = nodeInfo.getEndPoint();
        DefaultNode node = findIn(contactPoints, endPoint);
        if (node == null) {
          node = new DefaultNode(endPoint, context);
          LOG.debug("[{}] Adding new node {}", logPrefix, node);
        } else {
          LOG.debug("[{}] Copying contact point {}", logPrefix, node);
        }
        if (tokenMapEnabled && tokenFactory == null && nodeInfo.getPartitioner() != null) {
          tokenFactory = tokenFactoryRegistry.tokenFactoryFor(nodeInfo.getPartitioner());
        }
        copyInfos(nodeInfo, node, context);
        newNodes.put(hostId, node);
      }
    }

    ImmutableList.Builder<Object> eventsBuilder = ImmutableList.builder();

    for (DefaultNode newNode : newNodes.values()) {
      if (findIn(contactPoints, newNode.getEndPoint()) == null) {
        eventsBuilder.add(NodeStateEvent.added(newNode));
      }
    }
    for (DefaultNode contactPoint : contactPoints) {
      if (findIn(newNodes.values(), contactPoint.getEndPoint()) == null) {
        eventsBuilder.add(NodeStateEvent.removed(contactPoint));
      }
    }

    return new Result(
        oldMetadata.withNodes(
            ImmutableMap.copyOf(newNodes), tokenMapEnabled, true, tokenFactory, context),
        eventsBuilder.build());
  }

  private DefaultNode findIn(Iterable<? extends Node> nodes, EndPoint endPoint) {
    for (Node node : nodes) {
      if (node.getEndPoint().equals(endPoint)) {
        return (DefaultNode) node;
      }
    }
    return null;
  }
}
