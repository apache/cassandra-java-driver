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

import com.datastax.oss.driver.api.core.CassandraVersion;
import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

/**
 * Metadata about a Cassandra node in the cluster.
 *
 * <p>This object is mutable, all of its properties may be updated at runtime to reflect the latest
 * state of the node.
 */
public interface Node {
  /**
   * The address that the driver uses to connect to the node. This is the node's broadcast RPC
   * address, <b>transformed by the {@link AddressTranslator}</b> if one is configured.
   *
   * <p>The driver also uses this to uniquely identify a node.
   */
  InetSocketAddress getConnectAddress();

  /**
   * The node's broadcast address. That is, the address that other nodes use to communicate with
   * that node. This is also the value of the {@code peer} column in {@code system.peers}.
   *
   * <p>This may not be known at all times. In particular, some Cassandra versions don't store it in
   * the {@code system.local} table, so this will be unknown for the control node, until the control
   * connection reconnects to another node.
   */
  Optional<InetAddress> getBroadcastAddress();

  /**
   * The node's listen address. That is, the address that the Cassandra process binds to.
   *
   * <p>This may not be know at all times. In particular, current Cassandra versions (3.10) only
   * store it in {@code system.local}, so this will be known only for the control node.
   */
  Optional<InetAddress> getListenAddress();

  String getDatacenter();

  String getRack();

  CassandraVersion getCassandraVersion();

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
  Map<String, Object> getExtras();

  NodeState getState();

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
  NodeDistance getDistance();
}
