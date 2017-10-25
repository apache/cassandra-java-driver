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
package com.datastax.oss.driver.api.core.loadbalancing;

import com.datastax.oss.driver.api.core.ClusterBuilder;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/** Decides which Cassandra nodes to contact for each query. */
public interface LoadBalancingPolicy extends AutoCloseable {

  /**
   * Initializes this policy with the nodes discovered during driver initialization.
   *
   * <p>This method is guaranteed to be called exactly once per instance, and before any other
   * method in this class.
   *
   * @param nodes the nodes discovered by the driver when it connected to the cluster. When this
   *     method is invoked, their state is guaranteed to be either {@link NodeState#UP} or {@link
   *     NodeState#UNKNOWN}. Node states may be updated concurrently while this method executes, but
   *     if so you will receive a notification
   * @param distanceReporter an object that will be used by the policy to signal distance changes.
   * @param contactPoints the set of contact points that the driver was initialized with (see {@link
   *     ClusterBuilder#addContactPoints(Collection)}). This is provided for reference, in case the
   *     policy needs to handle those nodes in a particular way. Each address in this set should
   *     normally have a corresponding entry in {@code nodes}, except for contact points that were
   *     down or invalid. If no contact points were provided, the driver defaults to 127.0.0.1:9042,
   *     but the set will be empty.
   */
  void init(
      Map<InetSocketAddress, Node> nodes,
      DistanceReporter distanceReporter,
      Set<InetSocketAddress> contactPoints);

  /**
   * Returns the coordinators to use for a new query.
   *
   * <p>Each new query will call this method, and try the returned nodes sequentially.
   *
   * @param request the request that is being routed. Note that this can be null for some internal
   *     uses.
   * @param session the session that is executing the request. Note that this can be null for some
   *     internal uses.
   * @return the list of coordinators to try. <b>This must be a concurrent queue</b>; {@link
   *     java.util.concurrent.ConcurrentLinkedQueue} is a good choice.
   */
  Queue<Node> newQueryPlan(Request request, Session session);

  /**
   * Called when a node is added to the cluster.
   *
   * <p>The new node will have the state {@link NodeState#UNKNOWN}. The actual state will be known
   * when:
   *
   * <ul>
   *   <li>the load balancing policy signals an active distance for the node, and the driver tries
   *       to connect to it.
   *   <li>or a topology event is received from the cluster.
   * </ul>
   */
  void onAdd(Node node);

  /** Called when a node is determined to be up. */
  void onUp(Node node);

  /** Called when a node is determined to be down. */
  void onDown(Node node);

  /** Called when a node is removed from the cluster. */
  void onRemove(Node node);

  /** Called when the cluster that this policy is associated with closes. */
  @Override
  void close();

  /** An object that the policy uses to signal decisions it makes about node distances. */
  interface DistanceReporter {
    void setDistance(Node node, NodeDistance distance);
  }
}
