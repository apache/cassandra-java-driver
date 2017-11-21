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

import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;

public class AddNodeRefresh extends NodesRefresh {

  @VisibleForTesting final NodeInfo newNodeInfo;

  AddNodeRefresh(NodeInfo newNodeInfo, String logPrefix) {
    super(logPrefix);
    this.newNodeInfo = newNodeInfo;
  }

  @Override
  public Result compute(DefaultMetadata oldMetadata, boolean tokenMapEnabled) {
    Map<InetSocketAddress, Node> oldNodes = oldMetadata.getNodes();
    if (oldNodes.containsKey(newNodeInfo.getConnectAddress())) {
      return new Result(oldMetadata);
    } else {
      DefaultNode newNode = new DefaultNode(newNodeInfo.getConnectAddress());
      copyInfos(newNodeInfo, newNode, null, logPrefix);
      Map<InetSocketAddress, Node> newNodes =
          ImmutableMap.<InetSocketAddress, Node>builder()
              .putAll(oldNodes)
              .put(newNode.getConnectAddress(), newNode)
              .build();
      return new Result(
          oldMetadata.withNodes(newNodes, tokenMapEnabled, false, null),
          ImmutableList.of(NodeStateEvent.added(newNode)));
    }
  }
}
