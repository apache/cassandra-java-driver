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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;

/**
 * This class is immutable, so that metadata changes are atomic for the client. Every mutation
 * operation must return a new instance, that will replace the existing one in {@link
 * MetadataManager}'s volatile field.
 */
public class DefaultMetadata implements Metadata {
  public static final DefaultMetadata EMPTY = new DefaultMetadata(Collections.emptyMap());

  private final Map<InetSocketAddress, Node> nodes;
  private final Map<CqlIdentifier, KeyspaceMetadata> keyspaces;
  // TODO token map

  public DefaultMetadata(Map<InetSocketAddress, Node> nodes) {
    this(ImmutableMap.copyOf(nodes), Collections.emptyMap());
  }

  private DefaultMetadata(
      Map<InetSocketAddress, Node> nodes, Map<CqlIdentifier, KeyspaceMetadata> keyspaces) {
    this.nodes = nodes;
    this.keyspaces = keyspaces;
  }

  @Override
  public Map<InetSocketAddress, Node> getNodes() {
    return nodes;
  }

  @Override
  public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
    return keyspaces;
  }

  public DefaultMetadata withNodes(Map<InetSocketAddress, Node> newNodes) {
    // TODO recompute token map
    return new DefaultMetadata(ImmutableMap.copyOf(newNodes), this.keyspaces);
  }

  public DefaultMetadata withKeyspaces(Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces) {
    // TODO recompute token map
    return new DefaultMetadata(this.nodes, ImmutableMap.copyOf(newKeyspaces));
  }
}
