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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractTokenRingDiagnosticGenerator implements TokenRingDiagnosticGenerator {

  protected final Metadata metadata;

  protected final KeyspaceMetadata keyspace;

  protected AbstractTokenRingDiagnosticGenerator(
      @NonNull Metadata metadata, @NonNull KeyspaceMetadata keyspace) {
    Objects.requireNonNull(metadata, "metadata cannot be null");
    Objects.requireNonNull(keyspace, "keyspace cannot be null");
    this.metadata = metadata;
    this.keyspace = keyspace;
  }

  @NonNull
  @Override
  public TokenRingDiagnostic generate() {
    Set<TokenRangeDiagnostic> tokenRangeDiagnostics;
    if (metadata.getTokenMap().isPresent()) {
      TokenMap tokenMap = metadata.getTokenMap().get();
      tokenRangeDiagnostics = generateTokenRangeDiagnostics(tokenMap);
    } else {
      tokenRangeDiagnostics = Collections.emptyNavigableSet();
    }
    return generateRingDiagnostic(tokenRangeDiagnostics);
  }

  protected Set<TokenRangeDiagnostic> generateTokenRangeDiagnostics(TokenMap tokenMap) {
    Set<TokenRange> tokenRanges = tokenMap.getTokenRanges();
    Set<TokenRangeDiagnostic> reports = Sets.newHashSetWithExpectedSize(tokenRanges.size());
    for (TokenRange range : tokenRanges) {
      Set<Node> allReplicas = tokenMap.getReplicas(keyspace.getName(), range);
      Set<Node> aliveReplicas =
          allReplicas.stream().filter(this::isAlive).collect(Collectors.toSet());
      TokenRangeDiagnostic report = generateTokenRangeDiagnostic(range, aliveReplicas);
      reports.add(report);
    }
    return reports;
  }

  protected boolean isAlive(Node node) {
    // Be optimistic and count nodes in unknown state as alive
    return node.getState() == NodeState.UP || node.getState() == NodeState.UNKNOWN;
  }

  protected abstract TokenRangeDiagnostic generateTokenRangeDiagnostic(
      TokenRange range, Set<Node> aliveReplicas);

  protected abstract TokenRingDiagnostic generateRingDiagnostic(
      Set<TokenRangeDiagnostic> tokenRangeDiagnostics);
}
