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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.ring;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.ReplicationFactor;
import com.datastax.oss.driver.internal.core.util.ConsistencyLevels;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A {@link TokenRingDiagnosticGenerator} that checks if the configured consistency level is
 * achievable on all datacenters individually.
 *
 * <p>This reporter is only suitable for the special consistency level {@link
 * ConsistencyLevel#EACH_QUORUM}.
 */
public class EachQuorumTokenRingDiagnosticGenerator extends AbstractTokenRingDiagnosticGenerator {

  private final Map<String, Integer> requiredReplicasByDc;

  public EachQuorumTokenRingDiagnosticGenerator(
      @NonNull Metadata metadata,
      @NonNull KeyspaceMetadata keyspace,
      @NonNull Map<String, ReplicationFactor> replicationFactorsByDc) {
    super(metadata, keyspace);
    Objects.requireNonNull(replicationFactorsByDc, "replicationFactorsByDc cannot be null");
    this.requiredReplicasByDc =
        replicationFactorsByDc.entrySet().stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    Entry::getKey,
                    entry ->
                        ConsistencyLevels.requiredReplicas(
                            ConsistencyLevel.EACH_QUORUM, entry.getValue())));
  }

  @Override
  protected TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> aliveReplicas) {
    CompositeTokenRangeDiagnostic.Builder diagnostic =
        new CompositeTokenRangeDiagnostic.Builder(range);
    Map<String, Integer> aliveReplicasByDc = getAliveReplicasByDc(aliveReplicas);
    for (String datacenter : this.requiredReplicasByDc.keySet()) {
      int requiredReplicasInDc = this.requiredReplicasByDc.get(datacenter);
      int aliveReplicasInDc = aliveReplicasByDc.getOrDefault(datacenter, 0);
      TokenRangeDiagnostic childDiagnostic =
          new SimpleTokenRangeDiagnostic(range, requiredReplicasInDc, aliveReplicasInDc);
      diagnostic.addChildDiagnostic(datacenter, childDiagnostic);
    }
    return diagnostic.build();
  }

  @Override
  protected TokenRingDiagnostic generateRingDiagnostic(
      Set<TokenRangeDiagnostic> tokenRangeDiagnostics) {
    return new DefaultTokenRingDiagnostic(
        keyspace, ConsistencyLevel.EACH_QUORUM, null, tokenRangeDiagnostics);
  }

  private Map<String, Integer> getAliveReplicasByDc(Set<Node> aliveReplicas) {
    return aliveReplicas.stream()
        .collect(Collectors.toMap(Node::getDatacenter, (replica) -> 1, Integer::sum));
  }
}
