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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.datastax.oss.driver.internal.core.util.RoutingKey;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.SetMultimap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class DefaultTokenMap implements TokenMap {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultTokenMap.class);

  public static DefaultTokenMap build(
      @NonNull Collection<Node> nodes,
      @NonNull Collection<KeyspaceMetadata> keyspaces,
      @NonNull TokenFactory tokenFactory,
      @NonNull ReplicationStrategyFactory replicationStrategyFactory,
      @NonNull String logPrefix) {

    TokenToPrimaryAndRing tmp = buildTokenToPrimaryAndRing(nodes, tokenFactory);
    Map<Token, Node> tokenToPrimary = tmp.tokenToPrimary;
    List<Token> ring = tmp.ring;
    LOG.debug("[{}] Rebuilt ring ({} tokens)", logPrefix, ring.size());

    Set<TokenRange> tokenRanges = buildTokenRanges(ring, tokenFactory);

    ImmutableSetMultimap.Builder<Node, TokenRange> tokenRangesByPrimary =
        ImmutableSetMultimap.builder();
    for (TokenRange range : tokenRanges) {
      if (range.isFullRing()) {
        // The full ring is always ]min, min], so getEnd() doesn't match the node's token
        assert tokenToPrimary.size() == 1;
        tokenRangesByPrimary.put(tokenToPrimary.values().iterator().next(), range);
      } else {
        tokenRangesByPrimary.put(tokenToPrimary.get(range.getEnd()), range);
      }
    }

    Map<CqlIdentifier, Map<String, String>> replicationConfigs =
        buildReplicationConfigs(keyspaces, logPrefix);

    ImmutableMap.Builder<Map<String, String>, KeyspaceTokenMap> keyspaceMapsBuilder =
        ImmutableMap.builder();
    for (Map<String, String> config : ImmutableSet.copyOf(replicationConfigs.values())) {
      LOG.debug("[{}] Computing keyspace-level data for {}", logPrefix, config);
      keyspaceMapsBuilder.put(
          config,
          KeyspaceTokenMap.build(
              config,
              tokenToPrimary,
              ring,
              tokenRanges,
              tokenFactory,
              replicationStrategyFactory,
              logPrefix));
    }
    return new DefaultTokenMap(
        tokenFactory,
        tokenRanges,
        tokenRangesByPrimary.build(),
        replicationConfigs,
        keyspaceMapsBuilder.build(),
        logPrefix);
  }

  private final TokenFactory tokenFactory;
  @VisibleForTesting final Set<TokenRange> tokenRanges;
  @VisibleForTesting final SetMultimap<Node, TokenRange> tokenRangesByPrimary;
  @VisibleForTesting final Map<CqlIdentifier, Map<String, String>> replicationConfigs;
  @VisibleForTesting final Map<Map<String, String>, KeyspaceTokenMap> keyspaceMaps;
  private final String logPrefix;

  private DefaultTokenMap(
      TokenFactory tokenFactory,
      Set<TokenRange> tokenRanges,
      SetMultimap<Node, TokenRange> tokenRangesByPrimary,
      Map<CqlIdentifier, Map<String, String>> replicationConfigs,
      Map<Map<String, String>, KeyspaceTokenMap> keyspaceMaps,
      String logPrefix) {
    this.tokenFactory = tokenFactory;
    this.tokenRanges = tokenRanges;
    this.tokenRangesByPrimary = tokenRangesByPrimary;
    this.replicationConfigs = replicationConfigs;
    this.keyspaceMaps = keyspaceMaps;
    this.logPrefix = logPrefix;
  }

  public TokenFactory getTokenFactory() {
    return tokenFactory;
  }

  @NonNull
  @Override
  public Token parse(@NonNull String tokenString) {
    return tokenFactory.parse(tokenString);
  }

  @NonNull
  @Override
  public String format(@NonNull Token token) {
    return tokenFactory.format(token);
  }

  @NonNull
  @Override
  public Token newToken(@NonNull ByteBuffer... partitionKey) {
    return tokenFactory.hash(RoutingKey.compose(partitionKey));
  }

  @NonNull
  @Override
  public TokenRange newTokenRange(@NonNull Token start, @NonNull Token end) {
    return tokenFactory.range(start, end);
  }

  @NonNull
  @Override
  public Set<TokenRange> getTokenRanges() {
    return tokenRanges;
  }

  @NonNull
  @Override
  public Set<TokenRange> getTokenRanges(@NonNull Node node) {
    return tokenRangesByPrimary.get(node);
  }

  @NonNull
  @Override
  public Set<TokenRange> getTokenRanges(@NonNull CqlIdentifier keyspace, @NonNull Node replica) {
    KeyspaceTokenMap keyspaceMap = getKeyspaceMap(keyspace);
    return (keyspaceMap == null) ? Collections.emptySet() : keyspaceMap.getTokenRanges(replica);
  }

  @NonNull
  @Override
  public Set<Node> getReplicas(@NonNull CqlIdentifier keyspace, @NonNull ByteBuffer partitionKey) {
    KeyspaceTokenMap keyspaceMap = getKeyspaceMap(keyspace);
    return (keyspaceMap == null) ? Collections.emptySet() : keyspaceMap.getReplicas(partitionKey);
  }

  @NonNull
  @Override
  public Set<Node> getReplicas(@NonNull CqlIdentifier keyspace, @NonNull Token token) {
    KeyspaceTokenMap keyspaceMap = getKeyspaceMap(keyspace);
    return (keyspaceMap == null) ? Collections.emptySet() : keyspaceMap.getReplicas(token);
  }

  @NonNull
  @Override
  public String getPartitionerName() {
    return tokenFactory.getPartitionerName();
  }

  private KeyspaceTokenMap getKeyspaceMap(CqlIdentifier keyspace) {
    Map<String, String> config = replicationConfigs.get(keyspace);
    return (config == null) ? null : keyspaceMaps.get(config);
  }

  /** Called when only the schema has changed. */
  public DefaultTokenMap refresh(
      @NonNull Collection<Node> nodes,
      @NonNull Collection<KeyspaceMetadata> keyspaces,
      @NonNull ReplicationStrategyFactory replicationStrategyFactory) {

    Map<CqlIdentifier, Map<String, String>> newReplicationConfigs =
        buildReplicationConfigs(keyspaces, logPrefix);
    if (newReplicationConfigs.equals(replicationConfigs)) {
      LOG.debug("[{}] Schema changes do not impact the token map, no refresh needed", logPrefix);
      return this;
    }
    ImmutableMap.Builder<Map<String, String>, KeyspaceTokenMap> newKeyspaceMapsBuilder =
        ImmutableMap.builder();

    // Will only be built if needed:
    Map<Token, Node> tokenToPrimary = null;
    List<Token> ring = null;

    for (Map<String, String> config : ImmutableSet.copyOf(newReplicationConfigs.values())) {
      KeyspaceTokenMap oldKeyspaceMap = keyspaceMaps.get(config);
      if (oldKeyspaceMap != null) {
        LOG.debug("[{}] Reusing existing keyspace-level data for {}", logPrefix, config);
        newKeyspaceMapsBuilder.put(config, oldKeyspaceMap);
      } else {
        LOG.debug("[{}] Computing new keyspace-level data for {}", logPrefix, config);
        if (tokenToPrimary == null) {
          TokenToPrimaryAndRing tmp = buildTokenToPrimaryAndRing(nodes, tokenFactory);
          tokenToPrimary = tmp.tokenToPrimary;
          ring = tmp.ring;
        }
        newKeyspaceMapsBuilder.put(
            config,
            KeyspaceTokenMap.build(
                config,
                tokenToPrimary,
                ring,
                tokenRanges,
                tokenFactory,
                replicationStrategyFactory,
                logPrefix));
      }
    }
    return new DefaultTokenMap(
        tokenFactory,
        tokenRanges,
        tokenRangesByPrimary,
        newReplicationConfigs,
        newKeyspaceMapsBuilder.build(),
        logPrefix);
  }

  private static TokenToPrimaryAndRing buildTokenToPrimaryAndRing(
      Collection<Node> nodes, TokenFactory tokenFactory) {
    ImmutableMap.Builder<Token, Node> tokenToPrimaryBuilder = ImmutableMap.builder();
    SortedSet<Token> sortedTokens = new TreeSet<>();
    for (Node node : nodes) {
      for (String tokenString : ((DefaultNode) node).getRawTokens()) {
        Token token = tokenFactory.parse(tokenString);
        sortedTokens.add(token);
        tokenToPrimaryBuilder.put(token, node);
      }
    }
    return new TokenToPrimaryAndRing(
        tokenToPrimaryBuilder.build(), ImmutableList.copyOf(sortedTokens));
  }

  static class TokenToPrimaryAndRing {
    final Map<Token, Node> tokenToPrimary;
    final List<Token> ring;

    private TokenToPrimaryAndRing(Map<Token, Node> tokenToPrimary, List<Token> ring) {
      this.tokenToPrimary = tokenToPrimary;
      this.ring = ring;
    }
  }

  private static Map<CqlIdentifier, Map<String, String>> buildReplicationConfigs(
      Collection<KeyspaceMetadata> keyspaces, String logPrefix) {
    ImmutableMap.Builder<CqlIdentifier, Map<String, String>> builder = ImmutableMap.builder();
    for (KeyspaceMetadata keyspace : keyspaces) {
      if (!keyspace.isVirtual()) {
        builder.put(keyspace.getName(), keyspace.getReplication());
      }
    }
    ImmutableMap<CqlIdentifier, Map<String, String>> result = builder.build();
    LOG.debug("[{}] Computing keyspace-level data for {}", logPrefix, result);
    return result;
  }

  private static Set<TokenRange> buildTokenRanges(List<Token> ring, TokenFactory factory) {
    ImmutableSet.Builder<TokenRange> builder = ImmutableSet.builder();
    // JAVA-684: if there is only one token, return the full ring (]minToken, minToken])
    if (ring.size() == 1) {
      builder.add(factory.range(factory.minToken(), factory.minToken()));
    } else {
      for (int i = 0; i < ring.size(); i++) {
        Token start = ring.get(i);
        Token end = ring.get((i + 1) % ring.size());
        builder.add(factory.range(start, end));
      }
    }
    return builder.build();
  }
}
