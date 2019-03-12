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

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Information about a node, returned by the {@link TopologyMonitor}.
 *
 * <p>This information will be copied to the corresponding {@link Node} in the metadata.
 */
public interface NodeInfo {

  /** The endpoint that the driver will use to connect to the node. */
  EndPoint getEndPoint();

  /**
   * The node's broadcast RPC address.
   *
   * <p>This is used to match status events coming in on the control connection. Note that it's not
   * possible to fill it for the control node for some Cassandra versions, but that's less important
   * because the control node doesn't receive events for itself.
   *
   * @see Node#getBroadcastRpcAddress()
   */
  Optional<InetSocketAddress> getBroadcastRpcAddress();

  /**
   * The node's broadcast address and port. That is, the address that other nodes use to communicate
   * with that node.
   *
   * <p>This is only used by the default topology monitor, so if you are writing a custom one and
   * don't need this information, you can leave it empty.
   */
  Optional<InetSocketAddress> getBroadcastAddress();

  /**
   * The node's listen address and port. That is, the address that the Cassandra process binds to.
   *
   * <p>This is currently not used anywhere in the driver. If you write a custom topology monitor
   * and don't need this information, you can leave it empty.
   */
  Optional<InetSocketAddress> getListenAddress();

  /**
   * The data center that this node belongs to, according to the Cassandra snitch.
   *
   * <p>This is used by some {@link LoadBalancingPolicy} implementations to compute the {@link
   * NodeDistance}.
   */
  String getDatacenter();

  /**
   * The rack that this node belongs to, according to the Cassandra snitch.
   *
   * <p>This is used by some {@link LoadBalancingPolicy} implementations to compute the {@link
   * NodeDistance}.
   */
  String getRack();

  /**
   * The Cassandra version that this node runs.
   *
   * <p>This is used when parsing the schema (schema tables sometimes change from one version to the
   * next, even if the protocol version stays the same). If this is null, schema parsing will use
   * the lowest version for the current protocol version, which might lead to inaccuracies.
   */
  String getCassandraVersion();

  /**
   * The fully-qualifier name of the partitioner class that distributes data across the nodes, as it
   * appears in {@code system.local.partitioner}.
   *
   * <p>This is used to compute the driver-side token metadata (in particular, token-aware routing
   * relies on this information). It is only really needed for the first node of the initial node
   * list refresh (but it doesn't hurt to always include it if possible). If it is absent, {@link
   * Metadata#getTokenMap()} will remain empty.
   */
  String getPartitioner();

  /**
   * The tokens that this node owns on the ring.
   *
   * <p>This is used to compute the driver-side token metadata (in particular, token-aware routing
   * relies on this information). If you're not using token metadata in any way, you may return an
   * empty set here.
   */
  Set<String> getTokens();

  /**
   * An additional map of free-form properties, that can be used by custom implementations. They
   * will be copied as-is into {@link Node#getExtras()}.
   */
  Map<String, Object> getExtras();

  /**
   * The host ID that is assigned to this host by cassandra. The driver uses this to uniquely
   * identify a node.
   */
  UUID getHostId();

  /** The current version that is associated with the nodes schema. */
  UUID getSchemaVersion();
}
