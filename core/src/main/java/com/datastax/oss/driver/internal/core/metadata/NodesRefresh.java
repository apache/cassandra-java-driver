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

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class NodesRefresh extends MetadataRefresh {

  private static final Logger LOG = LoggerFactory.getLogger(NodesRefresh.class);

  protected NodesRefresh(String logPrefix) {
    super(logPrefix);
  }

  /**
   * @return whether the node's token have changed as a result of this operation (unfortunately we
   *     mutate the tokens in-place, so there is no way to check this after the fact).
   */
  protected static boolean copyInfos(
      NodeInfo nodeInfo, DefaultNode node, TokenFactory tokenFactory, String logPrefix) {
    node.broadcastAddress = nodeInfo.getBroadcastAddress();
    node.listenAddress = nodeInfo.getListenAddress();
    node.datacenter = nodeInfo.getDatacenter();
    node.rack = nodeInfo.getRack();
    String versionString = nodeInfo.getCassandraVersion();
    try {
      node.cassandraVersion = CassandraVersion.parse(versionString);
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "[{}] Error converting Cassandra version '{}' for {}",
          logPrefix,
          versionString,
          node.getConnectAddress());
    }
    boolean tokensChanged = tokenFactory != null && !node.rawTokens.equals(nodeInfo.getTokens());
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
