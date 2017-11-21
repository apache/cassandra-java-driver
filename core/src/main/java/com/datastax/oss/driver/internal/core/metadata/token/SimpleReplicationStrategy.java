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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class SimpleReplicationStrategy implements ReplicationStrategy {

  private final int replicationFactor;

  SimpleReplicationStrategy(Map<String, String> replicationConfig) {
    this(extractReplicationFactor(replicationConfig));
  }

  @VisibleForTesting
  SimpleReplicationStrategy(int replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  @Override
  public SetMultimap<Token, Node> computeReplicasByToken(
      Map<Token, Node> tokenToPrimary, List<Token> ring) {

    int rf = Math.min(replicationFactor, ring.size());

    ImmutableSetMultimap.Builder<Token, Node> result = ImmutableSetMultimap.builder();
    for (int i = 0; i < ring.size(); i++) {
      // Consecutive sections of the ring can be assigned to the same node
      Set<Node> replicas = new LinkedHashSet<>();
      for (int j = 0; j < ring.size() && replicas.size() < rf; j++) {
        replicas.add(tokenToPrimary.get(getTokenWrapping(i + j, ring)));
      }
      result.putAll(ring.get(i), replicas);
    }
    return result.build();
  }

  private static Token getTokenWrapping(int i, List<Token> ring) {
    return ring.get(i % ring.size());
  }

  private static int extractReplicationFactor(Map<String, String> replicationConfig) {
    String factorString = replicationConfig.get("replication_factor");
    Preconditions.checkNotNull(factorString, "Missing replication factor in " + replicationConfig);
    return Integer.parseInt(factorString);
  }
}
