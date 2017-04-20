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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveNodeRefresh extends NodesRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveNodeRefresh.class);

  @VisibleForTesting final InetSocketAddress toRemove;

  RemoveNodeRefresh(DefaultMetadata current, InetSocketAddress toRemove) {
    super(current);
    this.toRemove = toRemove;
  }

  @Override
  protected Map<InetSocketAddress, Node> computeNewNodes() {
    Map<InetSocketAddress, Node> oldNodes = oldMetadata.getNodes();
    Node node = oldNodes.get(toRemove);
    if (node == null) {
      // Normally this should already be checked before calling MetadataManager, but it doesn't
      // hurt to fail gracefully just in case
      return null;
    } else {
      LOG.debug("Removing node {}", node);
      events.add(NodeStateEvent.removed((DefaultNode) node));
      ImmutableMap.Builder<InetSocketAddress, Node> newNodesBuilder = ImmutableMap.builder();
      for (Map.Entry<InetSocketAddress, Node> entry : oldNodes.entrySet()) {
        if (!entry.getKey().equals(toRemove)) {
          newNodesBuilder.put(entry.getKey(), entry.getValue());
        }
      }
      return newNodesBuilder.build();
    }
  }
}
