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

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Objects;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
abstract class NodesRefresh implements MetadataRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(NodesRefresh.class);

  /**
   * @return whether the node's token have changed as a result of this operation (unfortunately we
   *     mutate the tokens in-place, so there is no way to check this after the fact).
   */
  protected static boolean copyInfos(
      NodeInfo nodeInfo, DefaultNode node, InternalDriverContext context) {

    node.setEndPoint(nodeInfo.getEndPoint(), context);
    node.broadcastRpcAddress = nodeInfo.getBroadcastRpcAddress().orElse(null);
    node.broadcastAddress = nodeInfo.getBroadcastAddress().orElse(null);
    node.listenAddress = nodeInfo.getListenAddress().orElse(null);
    node.datacenter = nodeInfo.getDatacenter();
    node.rack = nodeInfo.getRack();
    node.hostId = Objects.requireNonNull(nodeInfo.getHostId());
    node.schemaVersion = nodeInfo.getSchemaVersion();
    String versionString = nodeInfo.getCassandraVersion();
    try {
      node.cassandraVersion = Version.parse(versionString);
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "[{}] Error converting Cassandra version '{}' for {}",
          context.getSessionName(),
          versionString,
          node.getEndPoint());
    }
    boolean tokensChanged = !node.rawTokens.equals(nodeInfo.getTokens());
    if (tokensChanged) {
      node.rawTokens = nodeInfo.getTokens();
    }
    node.extras =
        (nodeInfo.getExtras() == null)
            ? Collections.emptyMap()
            : ImmutableMap.copyOf(nodeInfo.getExtras());
    return tokensChanged;
  }
}
