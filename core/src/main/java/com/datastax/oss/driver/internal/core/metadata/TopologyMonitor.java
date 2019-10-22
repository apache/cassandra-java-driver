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

import com.datastax.oss.driver.api.core.AsyncAutoCloseable;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Monitors the state of the Cassandra cluster.
 *
 * <p>It can either push {@link TopologyEvent topology events} to the rest of the driver (to do
 * that, retrieve the {@link EventBus}) from the {@link InternalDriverContext}), or receive requests
 * to refresh data about the nodes.
 *
 * <p>The default implementation uses the control connection: {@code TOPOLOGY_CHANGE} and {@code
 * STATUS_CHANGE} events on the connection are converted into {@code TopologyEvent}s, and node
 * refreshes are done with queries to system tables. If you prefer to rely on an external monitoring
 * tool, this can be completely overridden.
 */
public interface TopologyMonitor extends AsyncAutoCloseable {

  /**
   * Triggers the initialization of the monitor.
   *
   * <p>The completion of the future returned by this method marks the point when the driver
   * considers itself "connected" to the cluster, and proceeds with the rest of the initialization:
   * refreshing the list of nodes and the metadata, opening connection pools, etc. By then, the
   * topology monitor should be ready to accept calls to its other methods; in particular, {@link
   * #refreshNodeList()} will be called shortly after the completion of the future, to load the
   * initial list of nodes to connect to.
   *
   * <p>If {@code advanced.reconnect-on-init = true} in the configuration, this method is
   * responsible for handling reconnection. That is, if the initial attempt to "connect" to the
   * cluster fails, it must schedule reattempts, and only complete the returned future when
   * connection eventually succeeds. If the user cancels the returned future, then the reconnection
   * attempts should stop.
   *
   * <p>If this method is called multiple times, it should trigger initialization only once, and
   * return the same future on subsequent invocations.
   */
  CompletionStage<Void> init();

  /**
   * The future returned by {@link #init()}.
   *
   * <p>Note that this method may be called before {@link #init()}; at that stage, the future should
   * already exist, but be incomplete.
   */
  CompletionStage<Void> initFuture();

  /**
   * Invoked when the driver needs to refresh the information about an existing node. This is called
   * when the node was back and comes back up.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * @param node the node to refresh.
   * @return a future that completes with the information. If the monitor can't fulfill the request
   *     at this time, it should reply with {@link Optional#empty()}, and the driver will carry on
   *     with its current information.
   */
  CompletionStage<Optional<NodeInfo>> refreshNode(Node node);

  /**
   * Invoked when the driver needs to get information about a newly discovered node.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * @param broadcastRpcAddress the node's broadcast RPC address,.
   * @return a future that completes with the information. If the monitor doesn't know any node with
   *     this address, it should reply with {@link Optional#empty()}; the new node will be ignored.
   * @see Node#getBroadcastRpcAddress()
   */
  CompletionStage<Optional<NodeInfo>> getNewNodeInfo(InetSocketAddress broadcastRpcAddress);

  /**
   * Invoked when the driver needs to refresh information about all the nodes.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * <p>The driver calls this at initialization, and uses the result to initialize the {@link
   * LoadBalancingPolicy}; successful initialization of the {@link Session} object depends on that
   * initial call succeeding.
   *
   * @return a future that completes with the information. We assume that the full node list will
   *     always be returned in a single message (no paging).
   */
  CompletionStage<Iterable<NodeInfo>> refreshNodeList();

  /**
   * Checks whether the nodes in the cluster agree on a common schema version.
   *
   * <p>This should typically be implemented with a few retries and a timeout, as the schema can
   * take a while to replicate across nodes.
   */
  CompletionStage<Boolean> checkSchemaAgreement();
}
