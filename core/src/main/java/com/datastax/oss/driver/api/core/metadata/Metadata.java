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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The metadata of the Cassandra cluster that this driver instance is connected to.
 *
 * <p>Updates to this object are guaranteed to be atomic: the node list, schema, and token metadata
 * are immutable, and will always be consistent for a given metadata instance. The node instances
 * are the only mutable objects in the hierarchy, and some of their fields will be modified
 * dynamically (in particular the node state).
 *
 * @see Session#getMetadata()
 */
public interface Metadata {
  /**
   * The nodes known to the driver, indexed by their unique identifier ({@code host_id} in {@code
   * system.local}/{@code system.peers}). This might include nodes that are currently viewed as
   * down, or ignored by the load balancing policy.
   */
  @NonNull
  Map<UUID, Node> getNodes();

  /**
   * Finds the node with the given {@linkplain Node#getEndPoint() connection information}, if it
   * exists.
   *
   * <p>Note that this method performs a linear search of {@link #getNodes()}.
   */
  @NonNull
  default Optional<Node> findNode(@NonNull EndPoint endPoint) {
    for (Node node : getNodes().values()) {
      if (node.getEndPoint().equals(endPoint)) {
        return Optional.of(node);
      }
    }
    return Optional.empty();
  }

  /**
   * Finds the node with the given <em>untranslated</em> {@linkplain Node#getBroadcastRpcAddress()
   * broadcast RPC address}, if it exists.
   *
   * <p>Note that this method performs a linear search of {@link #getNodes()}.
   */
  @NonNull
  default Optional<Node> findNode(@NonNull InetSocketAddress broadcastRpcAddress) {
    for (Node node : getNodes().values()) {
      Optional<InetSocketAddress> o = node.getBroadcastRpcAddress();
      if (o.isPresent() && o.get().equals(broadcastRpcAddress)) {
        return Optional.of(node);
      }
    }
    return Optional.empty();
  }

  /**
   * The keyspaces defined in this cluster.
   *
   * <p>Note that schema metadata can be disabled or restricted to a subset of keyspaces, therefore
   * this map might be empty or incomplete.
   *
   * @see DefaultDriverOption#METADATA_SCHEMA_ENABLED
   * @see Session#setSchemaMetadataEnabled(Boolean)
   * @see DefaultDriverOption#METADATA_SCHEMA_REFRESHED_KEYSPACES
   */
  @NonNull
  Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces();

  @NonNull
  default Optional<KeyspaceMetadata> getKeyspace(@NonNull CqlIdentifier keyspaceId) {
    return Optional.ofNullable(getKeyspaces().get(keyspaceId));
  }

  /**
   * Shortcut for {@link #getKeyspace(CqlIdentifier)
   * getKeyspace(CqlIdentifier.fromCql(keyspaceName))}.
   */
  @NonNull
  default Optional<KeyspaceMetadata> getKeyspace(@NonNull String keyspaceName) {
    return getKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /**
   * The token map for this cluster.
   *
   * <p>Note that this property might be absent if token metadata was disabled, or if there was a
   * runtime error while computing the map (this would generate a warning log).
   *
   * @see DefaultDriverOption#METADATA_TOKEN_MAP_ENABLED
   */
  @NonNull
  Optional<TokenMap> getTokenMap();

  /**
   * The tablet map for this cluster.
   *
   * <p>Starts as an empty map that will gradually receive updates on each query of a yet unknown
   * tablet.
   */
  TabletMap getTabletMap();

  /**
   * The cluster name to which this session is connected. The Optional returned should contain the
   * value from the server for <b>system.local.cluster_name</b>.
   *
   * <p>Note that this method has a default implementation for backwards compatibility. It is
   * expected that any implementing classes override this method.
   */
  @NonNull
  default Optional<String> getClusterName() {
    return Optional.empty();
  }
}
