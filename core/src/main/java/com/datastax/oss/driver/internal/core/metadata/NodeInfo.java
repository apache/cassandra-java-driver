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

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import com.datastax.oss.driver.api.core.loadbalancing.NodeDistance;
import com.datastax.oss.driver.api.core.metadata.Node;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Information about a node, returned by the {@link TopologyMonitor}.
 *
 * <p>This information will be copied to the corresponding {@link Node} in the metadata.
 */
public interface NodeInfo {
  /**
   * The address that the driver uses to connect to the node. This is the node's broadcast RPC
   * address, <b>transformed by the {@link AddressTranslator}</b> if one is configured.
   *
   * <p>The driver uses this to uniquely identify a node.
   *
   * <p>This must not be null. If this instance is the reponse to a {@link
   * TopologyMonitor#refreshNode(Node) refresh node} request, it must also match the address with
   * which the request was made, otherwise the new node will be ignored.
   */
  InetSocketAddress getConnectAddress();

  /**
   * The node's broadcast address. That is, the address that other nodes use to communicate with
   * that node.
   *
   * <p>This is only used by the default topology monitor, so if you are writing a custom one and
   * don't need this information, you can leave it empty.
   */
  Optional<InetAddress> getBroadcastAddress();

  /**
   * The node's listen address. That is, the address that the Cassandra process binds to.
   *
   * <p>This is currently not used anywhere in the driver. If you write a custom topology monitor
   * and don't need this information, you can leave it empty.
   */
  Optional<InetAddress> getListenAddress();

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
}
