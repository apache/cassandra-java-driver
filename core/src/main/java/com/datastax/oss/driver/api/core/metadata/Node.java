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
package com.datastax.oss.driver.api.core.metadata;

import java.net.InetSocketAddress;

/** Metadata about a Cassandra node in the cluster. */
public interface Node {
  /**
   * The address that the driver uses to connect to the node. This is the node's broadcast RPC
   * address, <b>transformed by the address translator</b> if one is configured.
   *
   * <p>The driver also uses this to uniquely identify a node.
   */
  InetSocketAddress getConnectAddress();

  NodeState getState();

  /**
   * @return the total number of active connections currently open by this driver instance to the
   *     node. This can be either pooled connections, or the control connection.
   */
  int getOpenConnections();

  /**
   * @return whether the driver is currently trying to reconnect to this node. That is, whether the
   *     active connection count is below the value mandated by the configuration. This does not
   *     mean that the node is down, there could be some active connections but not enough.
   */
  boolean isReconnecting();

  // TODO expose distance (set by LBP)
}
