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

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class NodesRefresh extends MetadataRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(NodesRefresh.class);

  protected NodesRefresh(DefaultMetadata current) {
    super(current);
  }

  /** @return null if the nodes haven't changed */
  protected abstract Map<InetSocketAddress, Node> computeNewNodes();

  @Override
  void compute() {
    Map<InetSocketAddress, Node> newNodes = computeNewNodes();
    newMetadata = (newNodes == null) ? oldMetadata : new DefaultMetadata(newNodes);
    // TODO recompute token map (even if node list hasn't changed, b/c tokens might have changed)
  }

  protected static void copyInfos(TopologyMonitor.NodeInfo nodeInfo, DefaultNode node) {
    node.broadcastAddress = nodeInfo.getBroadcastAddress();
    node.listenAddress = nodeInfo.getListenAddress();
    node.datacenter = nodeInfo.getDatacenter();
    node.rack = nodeInfo.getRack();
    String versionString = nodeInfo.getCassandraVersion();
    try {
      node.cassandraVersion = CassandraVersion.parse(versionString);
    } catch (IllegalArgumentException e) {
      LOG.warn("Error converting Cassandra version '{}'", versionString);
    }
    node.extras =
        (nodeInfo.getExtras() == null)
            ? Collections.emptyMap()
            : ImmutableMap.copyOf(nodeInfo.getExtras());
  }
}
