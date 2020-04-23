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
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link TokenRingDiagnosticGenerator} that checks if the configured consistency level is
 * achievable globally, on the whole cluster.
 *
 * <p>This reporter is suitable only for SimpleStrategy replications, or for when the configured
 * consistency level is intended to be achieved globally, such as with {@link
 * ConsistencyLevel#QUORUM}.
 */
public class DefaultTokenRingDiagnosticGenerator extends AbstractTokenRingDiagnosticGenerator {

  protected final ConsistencyLevel consistencyLevel;

  protected final int requiredReplicas;

  public DefaultTokenRingDiagnosticGenerator(
      @NonNull Metadata metadata,
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @NonNull ReplicationFactor replicationFactor) {
    super(metadata, keyspace);
    Objects.requireNonNull(consistencyLevel, "consistencyLevel cannot be null");
    Objects.requireNonNull(replicationFactor, "replicationFactor cannot be null");
    Preconditions.checkArgument(
        consistencyLevel != ConsistencyLevel.EACH_QUORUM,
        "DefaultTokenRingDiagnosticGenerator is not compatible with EACH_QUORUM");
    this.consistencyLevel = consistencyLevel;
    this.requiredReplicas = ConsistencyLevels.requiredReplicas(consistencyLevel, replicationFactor);
  }

  @Override
  protected TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> allReplicas) {
    int pessimisticallyAliveReplicas = getPessimisticallyAliveReplicas(allReplicas);
    TokenRangeDiagnostic pessimistic =
        new SimpleTokenRangeDiagnostic(range, requiredReplicas, pessimisticallyAliveReplicas);
    if (!pessimistic.isAvailable()) {
      int optimisticallyAliveReplicas = getOptimisticallyAliveReplicas(allReplicas);
      if (optimisticallyAliveReplicas > pessimisticallyAliveReplicas) {
        TokenRangeDiagnostic optimistic =
            new SimpleTokenRangeDiagnostic(range, requiredReplicas, optimisticallyAliveReplicas);
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
    return new DefaultTokenRingDiagnostic(keyspace, consistencyLevel, null, tokenRangeDiagnostics);
  }

  private int getOptimisticallyAliveReplicas(Set<Node> allReplicas) {
    return (int) allReplicas.stream().filter(this::isOptimisticallyUp).count();
  }

  private int getPessimisticallyAliveReplicas(Set<Node> allReplicas) {
    return (int) allReplicas.stream().filter(this::isPessimisticallyUp).count();
  }
}
