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
package com.datastax.oss.driver.api.core.loadbalancing;

import com.datastax.oss.driver.api.core.metadata.Node;
import java.util.Queue;
import java.util.Set;

/** Decides which Cassandra nodes to contact for each query. */
public interface LoadBalancingPolicy {

  /**
   * Initializes this policy with the nodes discovered during driver initialization.
   *
   * <p>This method is guaranteed to be called exactly once per instance, and before any other
   * method in this class.
   */
  void init(Set<Node> nodes, DistanceReporter distanceReporter);

  /**
   * Returns the coordinators to use for a new query.
   *
   * <p>Each new query will call this method, and try the returned nodes sequentially.
   *
   * @return the list of coordinators to try. <b>This must be a concurrent queue</b>; {@link
   *     java.util.concurrent.ConcurrentLinkedQueue} is a good choice.
   */
  Queue<Node> newQueryPlan(/*TODO keyspace, statement*/ );

  // TODO node state methods (onUp, etc.)
  // TODO way to emit distance events

  /**
   * Invoked at cluster shutdown. This gives the policy the opportunity to perform some cleanup, for
   * instance stop threads that it might have started.
   */
  void close();

  /** An object that the policy uses to push the decisions it makes about node distances. */
  interface DistanceReporter {
    void setDistance(Node node, NodeDistance distance);
  }
}
