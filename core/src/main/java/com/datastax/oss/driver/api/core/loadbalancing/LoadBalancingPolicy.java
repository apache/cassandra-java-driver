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
package com.datastax.oss.driver.api.core.loadbalancing;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.tracker.RequestTracker;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;

/** Decides which Cassandra nodes to contact for each query. */
public interface LoadBalancingPolicy extends AutoCloseable {

  /**
   * Returns an optional {@link RequestTracker} to be registered with the session. Registering a
   * request tracker allows load-balancing policies to track node latencies in order to pick the
   * fastest ones.
   *
   * <p>This method is invoked only once during session configuration, and before any other methods
   * in this interface. Note that at this point, the driver hasn't connected to any node yet.
   *
   * @since 4.13.0
   */
  @NonNull
  default Optional<RequestTracker> getRequestTracker() {
    return Optional.empty();
  }

  /**
   * Initializes this policy with the nodes discovered during driver initialization.
   *
   * <p>This method is guaranteed to be called exactly once per instance, and before any other
   * method in this interface except {@link #getRequestTracker()}. At this point, the driver has
   * successfully connected to one of the contact points, and performed a first refresh of topology
   * information (by default, the contents of {@code system.peers}), to discover other nodes in the
   * cluster.
   *
   * <p>This method must call {@link DistanceReporter#setDistance(Node, NodeDistance)
   * distanceReporter.setDistance} for each provided node (otherwise that node will stay at distance
   * {@link NodeDistance#IGNORED IGNORED}, and the driver won't open connections to it). Note that
   * the node's {@link Node#getState() state} can be either {@link NodeState#UP UP} (for the
   * successful contact point), {@link NodeState#DOWN DOWN} (for contact points that were tried
   * unsuccessfully), or {@link NodeState#UNKNOWN UNKNOWN} (for contact points that weren't tried,
   * or any other node discovered from the topology refresh). Node states may be updated
   * concurrently while this method executes, but if so this policy will get notified after this
   * method has returned, through other methods such as {@link #onUp(Node)} or {@link
   * #onDown(Node)}.
   *
   * @param nodes all the nodes that are known to exist in the cluster (regardless of their state)
   *     at the time of invocation.
   * @param distanceReporter an object that will be used by the policy to signal distance changes.
   *     Implementations will typically store this in a field, since new nodes may get {@link
   *     #onAdd(Node) added} later and will need to have their distance set (or the policy might
   *     change distances dynamically over time).
   */
  void init(@NonNull Map<UUID, Node> nodes, @NonNull DistanceReporter distanceReporter);

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
  @NonNull
  Queue<Node> newQueryPlan(@Nullable Request request, @Nullable Session session);

  /**
   * Called when a node is added to the cluster.
   *
   * <p>The new node will be at distance {@link NodeDistance#IGNORED IGNORED}, and have the state
   * {@link NodeState#UNKNOWN UNKNOWN}.
   *
   * <p>If this method assigns an active distance to the node, the driver will try to create a
   * connection pool to it (resulting in a state change to {@link #onUp(Node) UP} or {@link
   * #onDown(Node) DOWN} depending on the outcome).
   *
   * <p>If it leaves it at distance {@link NodeDistance#IGNORED IGNORED}, the driver won't attempt
   * any connection. The node state will remain unknown, but might be updated later if a topology
   * event is received from the cluster.
   *
   * @see #init(Map, DistanceReporter)
   */
  void onAdd(@NonNull Node node);

  /** Called when a node is determined to be up. */
  void onUp(@NonNull Node node);

  /** Called when a node is determined to be down. */
  void onDown(@NonNull Node node);

  /** Called when a node is removed from the cluster. */
  void onRemove(@NonNull Node node);

  /** Called when the cluster that this policy is associated with closes. */
  @Override
  void close();

  /** An object that the policy uses to signal decisions it makes about node distances. */
  interface DistanceReporter {
    void setDistance(@NonNull Node node, @NonNull NodeDistance distance);
  }
}
