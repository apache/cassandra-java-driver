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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.UUID;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
public class RemoveNodeRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveNodeRefresh.class);

  @VisibleForTesting final InetSocketAddress broadcastRpcAddressToRemove;

  RemoveNodeRefresh(InetSocketAddress broadcastRpcAddressToRemove) {
    this.broadcastRpcAddressToRemove = broadcastRpcAddressToRemove;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {

    String logPrefix = context.getSessionName();

    Map<UUID, Node> oldNodes = oldMetadata.getNodes();

    ImmutableMap.Builder<UUID, Node> newNodesBuilder = ImmutableMap.builder();
    Node removedNode = null;
    for (Node node : oldNodes.values()) {
      if (node.getBroadcastRpcAddress().isPresent()
          && node.getBroadcastRpcAddress().get().equals(broadcastRpcAddressToRemove)) {
        removedNode = node;
      } else {
        assert node.getHostId() != null; // nodes in metadata.getNodes() always have their id set
        newNodesBuilder.put(node.getHostId(), node);
      }
    }

    if (removedNode == null) {
      // This should never happen because we already check the event in NodeStateManager, but handle
      // just in case.
      LOG.debug("[{}] Couldn't find node {} to remove", logPrefix, broadcastRpcAddressToRemove);
      return new Result(oldMetadata);
    } else {
      LOG.debug("[{}] Removing node {}", logPrefix, removedNode);
      LOG.debug("[{}] Tablet metadata will be wiped and rebuilt due to node removal.", logPrefix);
      DefaultMetadata newerMetadata = oldMetadata.withTabletMap(DefaultTabletMap.emptyMap());
      return new Result(
          newerMetadata.withNodes(newNodesBuilder.build(), tokenMapEnabled, false, null, context),
          ImmutableList.of(NodeStateEvent.removed((DefaultNode) removedNode)));
    }
  }
}
