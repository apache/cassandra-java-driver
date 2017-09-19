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
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is immutable, so that metadata changes are atomic for the client. Every mutation
 * operation must return a new instance, that will replace the existing one in {@link
 * MetadataManager}'s volatile field.
 */
public class DefaultMetadata implements Metadata {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadata.class);
  public static DefaultMetadata EMPTY = new DefaultMetadata(Collections.emptyMap(), null);

  private final Map<InetSocketAddress, Node> nodes;
  private final Map<CqlIdentifier, KeyspaceMetadata> keyspaces;
  private final Optional<TokenMap> tokenMap;
  private final String logPrefix;

  public DefaultMetadata(Map<InetSocketAddress, Node> nodes, String logPrefix) {
    this(ImmutableMap.copyOf(nodes), Collections.emptyMap(), Optional.empty(), logPrefix);
  }

  private DefaultMetadata(
      Map<InetSocketAddress, Node> nodes,
      Map<CqlIdentifier, KeyspaceMetadata> keyspaces,
      Optional<TokenMap> tokenMap,
      String logPrefix) {
    this.nodes = nodes;
    this.keyspaces = keyspaces;
    this.tokenMap = tokenMap;
    this.logPrefix = logPrefix;
  }

  @Override
  public Map<InetSocketAddress, Node> getNodes() {
    return nodes;
  }

  @Override
  public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
    return keyspaces;
  }

  @Override
  public Optional<TokenMap> getTokenMap() {
    return tokenMap;
  }

  /**
   * @param tokenMapEnabled
   * @param tokensChanged whether we observed a change of tokens for at least one node. This will
   *     require a full rebuild of the token map.
   * @param tokenFactory only needed for the initial refresh, afterwards the existing one in the
   *     token map is used.
   */
  public DefaultMetadata withNodes(
      Map<InetSocketAddress, Node> newNodes,
      boolean tokenMapEnabled,
      boolean tokensChanged,
      TokenFactory tokenFactory) {

    // Force a rebuild if at least one node has different tokens, or there are new or removed nodes.
    boolean forceFullRebuild = tokensChanged || !newNodes.equals(nodes);

    return new DefaultMetadata(
        ImmutableMap.copyOf(newNodes),
        this.keyspaces,
        rebuildTokenMap(newNodes, keyspaces, tokenMapEnabled, forceFullRebuild, tokenFactory),
        logPrefix);
  }

  public DefaultMetadata withSchema(
      Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces, boolean tokenMapEnabled) {
    return new DefaultMetadata(
        this.nodes,
        ImmutableMap.copyOf(newKeyspaces),
        rebuildTokenMap(nodes, newKeyspaces, tokenMapEnabled, false, null),
        logPrefix);
  }

  private Optional<TokenMap> rebuildTokenMap(
      Map<InetSocketAddress, Node> newNodes,
      Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces,
      boolean tokenMapEnabled,
      boolean forceFullRebuild,
      TokenFactory tokenFactory) {

    if (!tokenMapEnabled) {
      LOG.debug("[{}] Token map is disabled, skipping", logPrefix);
      return this.tokenMap;
    }
    long start = System.nanoTime();
    try {
      DefaultTokenMap oldTokenMap = (DefaultTokenMap) this.tokenMap.orElse(null);
      if (oldTokenMap == null) {
        // Initial build, we need the token factory
        if (tokenFactory == null) {
          LOG.debug(
              "[{}] Building initial token map but the token factory is missing, skipping",
              logPrefix);
          return this.tokenMap;
        } else {
          LOG.debug("[{}] Building initial token map", logPrefix);
          return Optional.of(
              DefaultTokenMap.build(
                  newNodes.values(), newKeyspaces.values(), tokenFactory, logPrefix));
        }
      } else if (forceFullRebuild) {
        LOG.debug(
            "[{}] Updating token map but some nodes/tokens have changed, full rebuild", logPrefix);
        return Optional.of(
            DefaultTokenMap.build(
                newNodes.values(),
                newKeyspaces.values(),
                oldTokenMap.getTokenFactory(),
                logPrefix));
      } else {
        LOG.debug("[{}] Refreshing token map (only schema has changed)", logPrefix);
        return Optional.of(oldTokenMap.refresh(newNodes.values(), newKeyspaces.values()));
      }
    } catch (Throwable t) {
      LOG.warn(
          "[{}] Unexpected error while refreshing token map, keeping previous version",
          logPrefix,
          t);
      return this.tokenMap;
    } finally {
      LOG.debug("[{}] Rebuilding token map took {}", logPrefix, NanoTime.formatTimeSince(start));
    }
  }
}
