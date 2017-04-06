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

/** The state of a node, as viewed from the driver. */
public enum NodeState {
  /**
   * The driver has never tried to connect to the node, nor received any topology events about it.
   *
   * <p>This happens if your load balancing policy ignores some of the nodes. Since the driver does
   * not connect to them, the only way it can assess their states is from topology events.
   */
  UNKNOWN,
  /**
   * A node is considered up in either of the following situations:
   *
   * <ul>
   *   <li>the driver has at least one active connection to the node.
   *   <li>the driver is not actively trying to connect to the node (because it's ignored by the
   *       load balancing policy), but it has received a topology event indicating that the node is
   *       up.
   * </ul>
   */
  UP,
  /**
   * A node is considered down in either of the following situations:
   *
   * <ul>
   *   <li>the driver has lost all connections to the node (and is currently trying to reconnect).
   *   <li>the driver is not actively trying to connect to the node (because it's ignored by the
   *       load balancing policy), but it has received a topology event indicating that the node is
   *       down.
   * </ul>
   */
  DOWN,
  /**
   * The node was forced down externally, the driver will never try to reconnect to it, whatever the
   * load balancing policy says.
   *
   * <p>This is used for edge error cases, for example when the driver detects that it's trying to
   * connect to a node that does not belong to the Cassandra cluster (e.g. a wrong address was
   * provided in the contact points).
   */
  FORCED_DOWN,
}
