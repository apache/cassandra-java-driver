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

/*
 * Copyright (C) 2020 ScyllaDB
 *
 * Modified by ScyllaDB
 */
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Partitioner;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.driver.shaded.guava.common.collect.SetMultimap;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The token data for a given replication configuration. It's shared by all keyspaces that use that
 * configuration.
 */
@Immutable
class KeyspaceTokenMap {

  private static final Logger LOG = LoggerFactory.getLogger(KeyspaceTokenMap.class);

  static KeyspaceTokenMap build(
      Map<String, String> replicationConfig,
      Map<Token, Node> tokenToPrimary,
      List<Token> ring,
      Set<TokenRange> tokenRanges,
      TokenFactory tokenFactory,
      ReplicationStrategyFactory replicationStrategyFactory,
      String logPrefix) {

    long start = System.nanoTime();
    try {
      ReplicationStrategy strategy = replicationStrategyFactory.newInstance(replicationConfig);

      Map<Token, Set<Node>> replicasByToken = strategy.computeReplicasByToken(tokenToPrimary, ring);
      SetMultimap<Node, TokenRange> tokenRangesByNode;
      if (ring.size() == 1) {
        // We forced the single range to ]minToken,minToken], make sure to use that instead of
        // relying
        // on the node's token
        ImmutableSetMultimap.Builder<Node, TokenRange> builder = ImmutableSetMultimap.builder();
        for (Node node : tokenToPrimary.values()) {
          builder.putAll(node, tokenRanges);
        }
        tokenRangesByNode = builder.build();
      } else {
        tokenRangesByNode = buildTokenRangesByNode(tokenRanges, replicasByToken);
      }
      return new KeyspaceTokenMap(ring, tokenRangesByNode, replicasByToken, tokenFactory);
    } finally {
      LOG.debug(
          "[{}] Computing keyspace-level data for {} took {}",
          logPrefix,
          replicationConfig,
          NanoTime.formatTimeSince(start));
    }
  }

  private final List<Token> ring;
  private final SetMultimap<Node, TokenRange> tokenRangesByNode;
  private final Map<Token, Set<Node>> replicasByToken;
  private final TokenFactory tokenFactory;

  private KeyspaceTokenMap(
      List<Token> ring,
      SetMultimap<Node, TokenRange> tokenRangesByNode,
      Map<Token, Set<Node>> replicasByToken,
      TokenFactory tokenFactory) {
    this.ring = ring;
    this.tokenRangesByNode = tokenRangesByNode;
    this.replicasByToken = replicasByToken;
    this.tokenFactory = tokenFactory;
  }

  Set<TokenRange> getTokenRanges(Node replica) {
    return tokenRangesByNode.get(replica);
  }

  Set<Node> getReplicas(Partitioner partitioner, ByteBuffer partitionKey) {
    if (partitioner == null) {
      partitioner = tokenFactory;
    }
    return getReplicas(partitioner.hash(partitionKey));
  }

  Set<Node> getReplicas(Token token) {
    // If the token happens to be one of the "primary" tokens, get result directly
    Set<Node> nodes = replicasByToken.get(token);
    if (nodes != null) {
      return nodes;
    }
    // Otherwise, find the closest "primary" token on the ring
    int i = Collections.binarySearch(ring, token);
    if (i < 0) {
      i = -i - 1;
      if (i >= ring.size()) {
        i = 0;
      }
    }
    return replicasByToken.get(ring.get(i));
  }

  private static SetMultimap<Node, TokenRange> buildTokenRangesByNode(
      Set<TokenRange> tokenRanges, Map<Token, Set<Node>> replicasByToken) {
    ImmutableSetMultimap.Builder<Node, TokenRange> result = ImmutableSetMultimap.builder();
    for (TokenRange range : tokenRanges) {
      for (Node node : replicasByToken.get(range.getEnd())) {
        result.put(node, range);
      }
    }
    return result.build();
  }
}
