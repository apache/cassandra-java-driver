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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link TokenRingDiagnosticGenerator} that checks if the configured consistency level is
 * achievable locally, on a single datacenter.
 *
 * <p>This report is suitable only for when the configured consistency level is intended to be
 * {@linkplain ConsistencyLevel#isDcLocal() achieved locally} on a specific datacenter, such as with
 * {@link ConsistencyLevel#LOCAL_QUORUM}.
 */
public class LocalTokenRingDiagnosticGenerator extends DefaultTokenRingDiagnosticGenerator {

  private final String datacenter;

  public LocalTokenRingDiagnosticGenerator(
      @NonNull Metadata metadata,
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @NonNull String datacenter,
      @NonNull ReplicationFactor replicationFactor) {
    super(metadata, keyspace, consistencyLevel, replicationFactor);
    Objects.requireNonNull(datacenter, "datacenter cannot be null");
    Preconditions.checkArgument(
        consistencyLevel.isDcLocal(),
        "LocalTokenRingDiagnosticGenerator is not compatible with " + consistencyLevel);
    this.datacenter = datacenter;
  }

  @Override
  protected TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> allReplicas) {
    int pessimisticallyAliveReplicasInDc = getPessimisticallyAliveReplicasInDc(allReplicas);
    TokenRangeDiagnostic pessimistic =
        new SimpleTokenRangeDiagnostic(range, requiredReplicas, pessimisticallyAliveReplicasInDc);
    if (!pessimistic.isAvailable()) {
      int optimisticallyAliveReplicasInDc = getOptimisticallyAliveReplicasInDc(allReplicas);
      if (optimisticallyAliveReplicasInDc > pessimisticallyAliveReplicasInDc) {
        TokenRangeDiagnostic optimistic =
            new SimpleTokenRangeDiagnostic(
                range, requiredReplicas, optimisticallyAliveReplicasInDc);
        if (optimistic.isAvailable()) {
          throw new UnreliableTokenRangeDiagnosticException(range);
        }
      }
    }
    return pessimistic;
  }

  @Override
  protected TokenRingDiagnostic generateRingDiagnostic(
      Set<TokenRangeDiagnostic> tokenRangeDiagnostics) {
    return new DefaultTokenRingDiagnostic(
        keyspace, consistencyLevel, datacenter, tokenRangeDiagnostics);
  }

  private int getPessimisticallyAliveReplicasInDc(Set<Node> allReplicas) {
    return (int)
        allReplicas.stream()
            .filter(this::isPessimisticallyUp)
            .map(Node::getDatacenter)
            .filter(datacenter::equals)
            .count();
  }

  private int getOptimisticallyAliveReplicasInDc(Set<Node> allReplicas) {
    return (int)
        allReplicas.stream()
            .filter(this::isOptimisticallyUp)
            .map(Node::getDatacenter)
            .filter(datacenter::equals)
            .count();
  }
}
