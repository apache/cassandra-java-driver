/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.internal.core.metadata.TopologyEvent;
import java.net.InetSocketAddress;

/** The state of a node, as viewed from the driver. */
public enum NodeState {
  /**
   * The driver has never tried to connect to the node, nor received any topology events about it.
   *
   * <p>This happens when nodes are first added to the cluster, and will persist if your {@link
   * LoadBalancingPolicy} decides to ignore them. Since the driver does not connect to them, the
   * only way it can assess their states is from topology events.
   */
  UNKNOWN,
  /**
   * A node is considered up in either of the following situations: 1) the driver has at least one
   * active connection to the node, or 2) the driver is not actively trying to connect to the node
   * (because it's ignored by the {@link LoadBalancingPolicy}), but it has received a topology event
   * indicating that the node is up.
   */
  UP,
  /**
   * A node is considered down in either of the following situations: 1) the driver has lost all
   * connections to the node (and is currently trying to reconnect), or 2) the driver is not
   * actively trying to connect to the node (because it's ignored by the {@link
   * LoadBalancingPolicy}), but it has received a topology event indicating that the node is down.
   */
  DOWN,
  /**
   * The node was forced down externally, the driver will never try to reconnect to it, whatever the
   * {@link LoadBalancingPolicy} says.
   *
   * <p>This is used for edge error cases, for example when the driver detects that it's trying to
   * connect to a node that does not belong to the Cassandra cluster (e.g. a wrong address was
   * provided in the contact points). It can also be {@link
   * TopologyEvent#forceDown(InetSocketAddress) triggered explicitly} by components (for example a
   * custom load balancing policy) that want to limit the number of nodes that the driver connects
   * to.
   */
  FORCED_DOWN,
}
