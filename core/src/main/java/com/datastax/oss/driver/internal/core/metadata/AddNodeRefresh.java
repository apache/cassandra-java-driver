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
import java.net.InetSocketAddress;
import java.util.Map;

public class AddNodeRefresh extends NodesRefresh {
  private final TopologyMonitor.NodeInfo newNodeInfo;

  public AddNodeRefresh(DefaultMetadata oldMetadata, TopologyMonitor.NodeInfo newNodeInfo) {
    super(oldMetadata);
    this.newNodeInfo = newNodeInfo;
  }

  @Override
  protected Map<InetSocketAddress, Node> computeNewNodes() {
    Map<InetSocketAddress, Node> oldNodes = oldMetadata.getNodes();
    if (oldNodes.containsKey(newNodeInfo.getConnectAddress())) {
      return oldNodes;
    } else {
      DefaultNode newNode = new DefaultNode(newNodeInfo.getConnectAddress());
      copyInfos(newNodeInfo, newNode);
      events.add(NodeStateEvent.added(newNode));
      return ImmutableMap.<InetSocketAddress, Node>builder()
          .putAll(oldNodes)
          .put(newNode.getConnectAddress(), newNode)
          .build();
    }
  }
}
