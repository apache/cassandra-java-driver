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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
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

  /**
   * The endpoint that the driver will use to connect to the node.
   *
   * <p>This information is required; the driver will not function properly if this method returns
   * {@code null}.
   */
  @NonNull
  EndPoint getEndPoint();

  /**
   * The node's broadcast RPC address and port. That is, the address that clients are supposed to
   * use to communicate with that node.
   *
   * <p>This is currently only used to match broadcast RPC addresses received in status events
   * coming in on the control connection. The driver does not use this value to actually connect to
   * the node, but rather uses {@link #getEndPoint()}.
   *
   * @see Node#getBroadcastRpcAddress()
   */
  @NonNull
  Optional<InetSocketAddress> getBroadcastRpcAddress();

  /**
   * The node's broadcast address and port. That is, the address that other nodes use to communicate
   * with that node.
   *
   * <p>This is only used by the default topology monitor, so if you are writing a custom one and
   * don't need this information, you can leave it empty.
   */
  @NonNull
  Optional<InetSocketAddress> getBroadcastAddress();

  /**
   * The node's listen address and port. That is, the address that the Cassandra process binds to.
   *
   * <p>This is currently not used anywhere in the driver. If you write a custom topology monitor
   * and don't need this information, you can leave it empty.
   */
  @NonNull
  Optional<InetSocketAddress> getListenAddress();

  /**
   * The data center that this node belongs to, according to the Cassandra snitch.
   *
   * <p>This is used by some {@link LoadBalancingPolicy} implementations to compute the {@link
   * NodeDistance}.
   */
  @Nullable
  String getDatacenter();

  /**
   * The rack that this node belongs to, according to the Cassandra snitch.
   *
   * <p>This is used by some {@link LoadBalancingPolicy} implementations to compute the {@link
   * NodeDistance}.
   */
  @Nullable
  String getRack();

  /**
   * The Cassandra version that this node runs.
   *
   * <p>This is used when parsing the schema (schema tables sometimes change from one version to the
   * next, even if the protocol version stays the same). If this is null, schema parsing will use
   * the lowest version for the current protocol version, which might lead to inaccuracies.
   */
  @Nullable
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
  @Nullable
  String getPartitioner();

  /**
   * The tokens that this node owns on the ring.
   *
   * <p>This is used to compute the driver-side token metadata (in particular, token-aware routing
   * relies on this information). If you're not using token metadata in any way, you may return an
   * empty set here.
   */
  @Nullable
  Set<String> getTokens();

  /**
   * An additional map of free-form properties, that can be used by custom implementations. They
   * will be copied as-is into {@link Node#getExtras()}.
   *
   * <p>This is not required; if you don't have anything specific to report here, it can be null or
   * empty.
   */
  @Nullable
  Map<String, Object> getExtras();

  /**
   * The host ID that is assigned to this host by cassandra. The driver uses this to uniquely
   * identify a node.
   *
   * <p>This information is required; the driver will not function properly if this method returns
   * {@code null}.
   */
  @NonNull
  UUID getHostId();

  /**
   * The current version that is associated with the node's schema.
   *
   * <p>This is not required; the driver reports it in {@link Node#getSchemaVersion()}, but for
   * informational purposes only. It is not used anywhere internally (schema agreement is checked
   * with {@link TopologyMonitor#checkSchemaAgreement()}, which by default queries system tables
   * directly, not this field).
   */
  @Nullable
  UUID getSchemaVersion();
}
