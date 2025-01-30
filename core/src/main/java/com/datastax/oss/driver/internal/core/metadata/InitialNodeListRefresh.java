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

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactoryRegistry;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
    // Contact point nodes don't have host ID as well as other info yet, so we fill them with node
    // info found on first match by endpoint
    Set<EndPoint> matchedContactPoints = new HashSet<>();
    List<DefaultNode> addedNodes = new ArrayList<>();

    for (NodeInfo nodeInfo : nodeInfos) {
      UUID hostId = nodeInfo.getHostId();
      if (newNodes.containsKey(hostId)) {
        LOG.warn(
            "[{}] Found duplicate entries with host_id {} in system.peers, "
                + "keeping only the first one {}",
            logPrefix,
            hostId,
            newNodes.get(hostId));
      } else {
        EndPoint endPoint = nodeInfo.getEndPoint();
        DefaultNode contactPointNode = findContactPointNode(endPoint);
        DefaultNode node;
        if (contactPointNode == null || matchedContactPoints.contains(endPoint)) {
          node = new DefaultNode(endPoint, context);
          addedNodes.add(node);
          LOG.debug("[{}] Adding new node {}", logPrefix, node);
        } else {
          matchedContactPoints.add(contactPointNode.getEndPoint());
          node = contactPointNode;
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
    for (DefaultNode addedNode : addedNodes) {
      eventsBuilder.add(NodeStateEvent.added(addedNode));
    }
    for (DefaultNode contactPoint : contactPoints) {
      if (!matchedContactPoints.contains(contactPoint.getEndPoint())) {
        eventsBuilder.add(NodeStateEvent.removed(contactPoint));
      }
    }

    return new Result(
        oldMetadata.withNodes(
            ImmutableMap.copyOf(newNodes), tokenMapEnabled, true, tokenFactory, context),
        eventsBuilder.build());
  }

  private DefaultNode findContactPointNode(EndPoint endPoint) {
    for (DefaultNode node : contactPoints) {
      if (node.getEndPoint().equals(endPoint)) {
        return node;
      }
    }
    return null;
  }
}
