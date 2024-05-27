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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TabletMap;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationStrategyFactory;
import com.datastax.oss.driver.internal.core.metadata.token.TokenFactory;
import com.datastax.oss.driver.internal.core.util.Loggers;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is immutable, so that metadata changes are atomic for the client. Every mutation
 * operation must return a new instance, that will replace the existing one in {@link
 * MetadataManager}'s volatile field.
 */
@Immutable
public class DefaultMetadata implements Metadata {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetadata.class);
  public static DefaultMetadata EMPTY =
      new DefaultMetadata(
          Collections.emptyMap(), Collections.emptyMap(), null, null, DefaultTabletMap.emptyMap());

  protected final Map<UUID, Node> nodes;
  protected final Map<CqlIdentifier, KeyspaceMetadata> keyspaces;
  protected final TokenMap tokenMap;
  protected final String clusterName;
  protected final TabletMap tabletMap;

  protected DefaultMetadata(
      Map<UUID, Node> nodes,
      Map<CqlIdentifier, KeyspaceMetadata> keyspaces,
      TokenMap tokenMap,
      String clusterName) {
    this(nodes, keyspaces, tokenMap, clusterName, DefaultTabletMap.emptyMap());
  }

  protected DefaultMetadata(
      Map<UUID, Node> nodes,
      Map<CqlIdentifier, KeyspaceMetadata> keyspaces,
      TokenMap tokenMap,
      String clusterName,
      TabletMap tabletMap) {
    this.nodes = nodes;
    this.keyspaces = keyspaces;
    this.tokenMap = tokenMap;
    this.clusterName = clusterName;
    this.tabletMap = tabletMap;
  }

  @NonNull
  @Override
  public Map<UUID, Node> getNodes() {
    return nodes;
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, KeyspaceMetadata> getKeyspaces() {
    return keyspaces;
  }

  @NonNull
  @Override
  public Optional<TokenMap> getTokenMap() {
    return Optional.ofNullable(tokenMap);
  }

  @Override
  public TabletMap getTabletMap() {
    return tabletMap;
  }

  @NonNull
  @Override
  public Optional<String> getClusterName() {
    return Optional.ofNullable(clusterName);
  }

  /**
   * Refreshes the current metadata with the given list of nodes.
   *
   * <p>Does not rebuild the tablet map.
   *
   * @param tokenMapEnabled whether to rebuild the token map or not; if this is {@code false} the
   *     current token map will be copied into the new metadata without being recomputed.
   * @param tokensChanged whether we observed a change of tokens for at least one node. This will
   *     require a full rebuild of the token map.
   * @param tokenFactory only needed for the initial refresh, afterwards the existing one in the
   *     token map is used.
   * @return the new metadata.
   */
  public DefaultMetadata withNodes(
      Map<UUID, Node> newNodes,
      boolean tokenMapEnabled,
      boolean tokensChanged,
      TokenFactory tokenFactory,
      InternalDriverContext context) {

    // Force a rebuild if at least one node has different tokens, or there are new or removed nodes.
    boolean forceFullRebuild = tokensChanged || !newNodes.equals(nodes);

    return new DefaultMetadata(
        ImmutableMap.copyOf(newNodes),
        this.keyspaces,
        rebuildTokenMap(
            newNodes, keyspaces, tokenMapEnabled, forceFullRebuild, tokenFactory, context),
        context.getChannelFactory().getClusterName(),
        this.tabletMap);
  }

  /**
   * Refreshes the current metadata with new TabletMap.
   *
   * @param newTabletMap replacement TabletMap.
   * @return new metadata.
   */
  public DefaultMetadata withTabletMap(TabletMap newTabletMap) {
    return new DefaultMetadata(
        this.nodes, this.keyspaces, this.tokenMap, this.clusterName, newTabletMap);
  }

  public DefaultMetadata withSchema(
      Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces,
      boolean tokenMapEnabled,
      InternalDriverContext context) {
    return new DefaultMetadata(
        this.nodes,
        ImmutableMap.copyOf(newKeyspaces),
        rebuildTokenMap(nodes, newKeyspaces, tokenMapEnabled, false, null, context),
        context.getChannelFactory().getClusterName(),
        this.tabletMap);
  }

  @Nullable
  protected TokenMap rebuildTokenMap(
      Map<UUID, Node> newNodes,
      Map<CqlIdentifier, KeyspaceMetadata> newKeyspaces,
      boolean tokenMapEnabled,
      boolean forceFullRebuild,
      TokenFactory tokenFactory,
      InternalDriverContext context) {

    String logPrefix = context.getSessionName();
    ReplicationStrategyFactory replicationStrategyFactory = context.getReplicationStrategyFactory();

    if (!tokenMapEnabled) {
      LOG.debug("[{}] Token map is disabled, skipping", logPrefix);
      return this.tokenMap;
    }
    long start = System.nanoTime();
    try {
      DefaultTokenMap oldTokenMap = (DefaultTokenMap) this.tokenMap;
      if (oldTokenMap == null) {
        // Initial build, we need the token factory
        if (tokenFactory == null) {
          LOG.debug(
              "[{}] Building initial token map but the token factory is missing, skipping",
              logPrefix);
          return null;
        } else {
          LOG.debug("[{}] Building initial token map", logPrefix);
          return DefaultTokenMap.build(
              newNodes.values(),
              newKeyspaces.values(),
              tokenFactory,
              replicationStrategyFactory,
              logPrefix);
        }
      } else if (forceFullRebuild) {
        LOG.debug(
            "[{}] Updating token map but some nodes/tokens have changed, full rebuild", logPrefix);
        return DefaultTokenMap.build(
            newNodes.values(),
            newKeyspaces.values(),
            oldTokenMap.getTokenFactory(),
            replicationStrategyFactory,
            logPrefix);
      } else {
        LOG.debug("[{}] Refreshing token map (only schema has changed)", logPrefix);
        return oldTokenMap.refresh(
            newNodes.values(), newKeyspaces.values(), replicationStrategyFactory);
      }
    } catch (Throwable t) {
      Loggers.warnWithException(
          LOG,
          "[{}] Unexpected error while refreshing token map, keeping previous version",
          logPrefix,
          t);
      return this.tokenMap;
    } finally {
      LOG.debug("[{}] Rebuilding token map took {}", logPrefix, NanoTime.formatTimeSince(start));
    }
  }
}
