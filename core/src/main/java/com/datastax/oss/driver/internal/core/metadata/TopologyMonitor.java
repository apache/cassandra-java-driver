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

import com.datastax.oss.driver.api.core.Cluster;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.context.EventBus;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
public interface TopologyMonitor {

  /**
   * Triggers the initialization of the monitor.
   *
   * <p>This will be invoked at startup, and is how the driver determines when it is "successfully
   * connected" to the Cassandra cluster. In particular, the initialization of the {@link Cluster}
   * instance depends on the result of this method.
   */
  CompletionStage<Void> init();

  /**
   * Invoked when the driver needs to refresh information about a node.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * @param address the address that the driver uses to connect to the node. This is the node's
   *     broadcast RPC address, <b>transformed by the address translator</b> if one is configured.
   * @return a future that completes with the information.
   */
  CompletionStage<NodeInfo> refreshNode(InetSocketAddress address);

  /**
   * Invoked when the driver needs to refresh information about all the nodes.
   *
   * <p>This will be invoked directly from a driver's internal thread; if the refresh involves
   * blocking I/O or heavy computations, it should be scheduled on a separate thread.
   *
   * <p>Implementation note: as shown by the signature, it is assumed that the full node list will
   * always be returned in a single message (no paging).
   *
   * @return a future that completes with the information.
   */
  CompletionStage<Iterable<NodeInfo>> refreshNodeList();

  /**
   * Information about a node, as it will be returned by the monitor.
   *
   * <p>This is distinct from what we expose in the public driver metadata.
   */
  interface NodeInfo {
    /**
     * The address that the driver uses to connect to the node. This is the node's broadcast RPC
     * address, <b>transformed by the address translator</b> if one is configured.
     */
    InetSocketAddress getConnectAddress();

    /**
     * The node's broadcast address. That is, the address that other nodes use to communicate with
     * that node.
     */
    Optional<InetAddress> getBroadcastAddress();

    /** The node's listen address. That is, the address that the Cassandra process binds to. */
    Optional<InetAddress> getListenAddress();

    String getDatacenter();

    String getRack();

    String getCassandraVersion();

    Set<String> getTokens();

    /**
     * An additional map of free-form properties, that can be used by custom implementations. They
     * will be copied as-is into the driver metadata's {@link Node}.
     */
    Map<String, Object> getExtras();
  }
}
