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
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class AddNodeRefresh extends NodesRefresh {

  @VisibleForTesting final NodeInfo newNodeInfo;

  AddNodeRefresh(NodeInfo newNodeInfo) {
    this.newNodeInfo = newNodeInfo;
  }

  @Override
  public Result compute(
      DefaultMetadata oldMetadata, boolean tokenMapEnabled, InternalDriverContext context) {
    Map<InetSocketAddress, Node> oldNodes = oldMetadata.getNodes();
    if (oldNodes.containsKey(newNodeInfo.getConnectAddress())) {
      return new Result(oldMetadata);
    } else {
      DefaultNode newNode = new DefaultNode(newNodeInfo.getConnectAddress(), context);
      copyInfos(newNodeInfo, newNode, null, context.getSessionName());
      Map<InetSocketAddress, Node> newNodes =
          ImmutableMap.<InetSocketAddress, Node>builder()
              .putAll(oldNodes)
              .put(newNode.getConnectAddress(), newNode)
              .build();
      return new Result(
          oldMetadata.withNodes(newNodes, tokenMapEnabled, false, null, context),
          ImmutableList.of(NodeStateEvent.added(newNode)));
    }
  }
}
