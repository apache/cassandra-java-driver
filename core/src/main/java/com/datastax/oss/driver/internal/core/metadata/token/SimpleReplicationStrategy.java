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
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
class SimpleReplicationStrategy implements ReplicationStrategy {

  private final ReplicationFactor replicationFactor;

  SimpleReplicationStrategy(Map<String, String> replicationConfig) {
    this(extractReplicationFactor(replicationConfig));
  }

  @VisibleForTesting
  SimpleReplicationStrategy(ReplicationFactor replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  @Override
  public Map<Token, Set<Node>> computeReplicasByToken(
      Map<Token, Node> tokenToPrimary, List<Token> ring) {

    int rf = Math.min(replicationFactor.fullReplicas(), ring.size());

    ImmutableMap.Builder<Token, Set<Node>> result = ImmutableMap.builder();
    CanonicalNodeSetBuilder replicasBuilder = new CanonicalNodeSetBuilder();

    for (int i = 0; i < ring.size(); i++) {
      replicasBuilder.clear();
      for (int j = 0; j < ring.size() && replicasBuilder.size() < rf; j++) {
        replicasBuilder.add(tokenToPrimary.get(getTokenWrapping(i + j, ring)));
      }
      result.put(ring.get(i), replicasBuilder.build());
    }
    return result.build();
  }

  private static Token getTokenWrapping(int i, List<Token> ring) {
    return ring.get(i % ring.size());
  }

  private static ReplicationFactor extractReplicationFactor(Map<String, String> replicationConfig) {
    String factorString = replicationConfig.get("replication_factor");
    Preconditions.checkNotNull(factorString, "Missing replication factor in " + replicationConfig);
    return ReplicationFactor.fromString(factorString);
  }
}
