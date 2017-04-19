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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
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
  // TODO schema
  // TODO token map

  public DefaultMetadata(Map<InetSocketAddress, Node> nodes) {
    this.nodes = ImmutableMap.copyOf(nodes);
  }

  @Override
  public Map<InetSocketAddress, Node> getNodes() {
    return nodes;
  }

  public DefaultMetadata addNode(Node toAdd) {
    Map<InetSocketAddress, Node> newNodes;
    if (nodes.containsKey(toAdd.getConnectAddress())) {
      return this;
    } else {
      newNodes =
          ImmutableMap.<InetSocketAddress, Node>builder()
              .putAll(nodes)
              .put(toAdd.getConnectAddress(), toAdd)
              .build();
      // TODO recompute token map
      return new DefaultMetadata(newNodes);
    }
  }
}
