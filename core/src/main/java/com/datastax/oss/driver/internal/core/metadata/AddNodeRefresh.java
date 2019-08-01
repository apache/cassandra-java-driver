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
import java.util.Map;
import java.util.UUID;
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
    Map<UUID, Node> oldNodes = oldMetadata.getNodes();
    Node existing = oldNodes.get(newNodeInfo.getHostId());
    if (existing == null) {
      DefaultNode newNode = new DefaultNode(newNodeInfo.getEndPoint(), context);
      copyInfos(newNodeInfo, newNode, null, context);
      Map<UUID, Node> newNodes =
          ImmutableMap.<UUID, Node>builder()
              .putAll(oldNodes)
              .put(newNode.getHostId(), newNode)
              .build();
      return new Result(
          oldMetadata.withNodes(newNodes, tokenMapEnabled, false, null, context),
          ImmutableList.of(NodeStateEvent.added(newNode)));
    } else {
      // If a node is restarted after changing its broadcast RPC address, Cassandra considers that
      // an addition, even though the host_id hasn't changed :(
      // Update the existing instance and emit an UP event to trigger a pool reconnection.
      if (!existing.getEndPoint().equals(newNodeInfo.getEndPoint())) {
        copyInfos(newNodeInfo, ((DefaultNode) existing), null, context);
        assert newNodeInfo.getBroadcastRpcAddress().isPresent(); // always for peer nodes
        return new Result(
            oldMetadata,
            ImmutableList.of(TopologyEvent.suggestUp(newNodeInfo.getBroadcastRpcAddress().get())));
      } else {
        return new Result(oldMetadata);
      }
    }
  }
}
