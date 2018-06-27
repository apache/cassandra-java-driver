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
   * The nodes known to the driver, indexed by the address that it uses to connect to them. This
   * might include nodes that are currently viewed as down, or ignored by the load balancing policy.
   */
  @NonNull
  Map<InetSocketAddress, Node> getNodes();

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
  Map<CqlIdentifier, ? extends KeyspaceMetadata> getKeyspaces();

  @NonNull
  default Optional<? extends KeyspaceMetadata> getKeyspace(@NonNull CqlIdentifier keyspaceId) {
    return Optional.ofNullable(getKeyspaces().get(keyspaceId));
  }

  /**
   * Shortcut for {@link #getKeyspace(CqlIdentifier)
   * getKeyspace(CqlIdentifier.fromCql(keyspaceName))}.
   */
  @NonNull
  default Optional<? extends KeyspaceMetadata> getKeyspace(@NonNull String keyspaceName) {
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
  Optional<? extends TokenMap> getTokenMap();
}
