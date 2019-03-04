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
package com.datastax.oss.driver.api.core.metadata;

import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Metadata about a Cassandra node in the cluster.
 *
 * <p>This object is mutable, all of its properties may be updated at runtime to reflect the latest
 * state of the node.
 */
public interface Node {

  /** The information that the driver uses to connect to the node. */
  @NonNull
  EndPoint getEndPoint();

  /**
   * The node's broadcast RPC address.
   *
   * <p>This is the address that the node expects clients to connect to, as reported in {@code
   * system.peers.rpc_address} (Cassandra 3) or {@code system.peers_v2.native_address/native_port}
   * (Cassandra 4+). However, it might not be what the driver uses directly, if the node is accessed
   * through a proxy.
   *
   * <p>This may not be known at all times. In particular, some Cassandra versions (less than
   * 2.0.16, 2.1.6 or 2.2.0-rc1) don't store it in the {@code system.local} table, so this will be
   * unknown for the control node, until the control connection reconnects to another node.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-9436">CASSANDRA-9436 (where the
   *     information was added to system.local)</a>
   */
  @NonNull
  Optional<InetSocketAddress> getBroadcastRpcAddress();

  /**
   * The node's broadcast address. That is, the address that other nodes use to communicate with
   * that node. This is also the value of the {@code peer} column in {@code system.peers}. If the
   * port is set to 0 it is unknown.
   *
   * <p>This may not be known at all times. In particular, some Cassandra versions (less than
   * 2.0.16, 2.1.6 or 2.2.0-rc1) don't store it in the {@code system.local} table, so this will be
   * unknown for the control node, until the control connection reconnects to another node.
   *
   * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-9436">CASSANDRA-9436 (where the
   *     information was added to system.local)</a>
   */
  @NonNull
  Optional<InetSocketAddress> getBroadcastAddress();

  /**
   * The node's listen address. That is, the address that the Cassandra process binds to. If the
   * port is set to 0 it is unknown.
   *
   * <p>This may not be know at all times. In particular, current Cassandra versions (3.10) only
   * store it in {@code system.local}, so this will be known only for the control node.
   */
  @NonNull
  Optional<InetSocketAddress> getListenAddress();

  /**
   * The datacenter that this node belongs to (according to the server-side snitch).
   *
   * <p>This should be non-null in a healthy deployment, but the driver will still function, and
   * report {@code null} here, if the server metadata was corrupted.
   */
  @Nullable
  String getDatacenter();

  /**
   * The rack that this node belongs to (according to the server-side snitch).
   *
   * <p>This should be non-null in a healthy deployment, but the driver will still function, and
   * report {@code null} here, if the server metadata was corrupted.
   */
  @Nullable
  String getRack();

  /**
   * The Cassandra version of the server.
   *
   * <p>This should be non-null in a healthy deployment, but the driver will still function, and
   * report {@code null} here, if the server metadata was corrupted or the reported version could
   * not be parsed.
   */
  @Nullable
  Version getCassandraVersion();

  /**
   * An additional map of free-form properties.
   *
   * <p>This is intended for future evolution or custom driver extensions. The contents of this map
   * are unspecified and may change at any point in time, always check for the existence of a key
   * before using it.
   *
   * <p>Note that the returned map is immutable: if the properties change, this is reflected by
   * publishing a new map instance, therefore you must call this method again to see the changes.
   */
  @NonNull
  Map<String, Object> getExtras();

  @NonNull
  NodeState getState();

  /**
   * The last time that this node transitioned to the UP state, in milliseconds since the epoch, or
   * -1 if it's not up at the moment.
   */
  long getUpSinceMillis();

  /**
   * The total number of active connections currently open by this driver instance to the node. This
   * can be either pooled connections, or the control connection.
   */
  int getOpenConnections();

  /**
   * Whether the driver is currently trying to reconnect to this node. That is, whether the active
   * connection count is below the value mandated by the configuration. This does not mean that the
   * node is down, there could be some active connections but not enough.
   */
  boolean isReconnecting();

  /**
   * The distance assigned to this node by the {@link LoadBalancingPolicy}, that controls certain
   * aspects of connection management.
   *
   * <p>This is exposed here for information only. Distance events are handled internally by the
   * driver.
   */
  @NonNull
  NodeDistance getDistance();

  /**
   * The host ID that is assigned to this node by Cassandra. This value can be used to uniquely
   * identify a node even when the underling IP address changes.
   *
   * <p>This should be non-null in a healthy deployment, but the driver will still function, and
   * report {@code null} here, if the server metadata was corrupted.
   */
  @Nullable
  UUID getHostId();

  /**
   * The current version that is associated with the node's schema.
   *
   * <p>This should be non-null in a healthy deployment, but the driver will still function, and
   * report {@code null} here, if the server metadata was corrupted.
   */
  @Nullable
  UUID getSchemaVersion();
}
