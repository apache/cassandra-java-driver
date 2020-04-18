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
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRangeDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TokenRingDiagnostic;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedSet;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

public class DefaultTokenRingDiagnostic implements TokenRingDiagnostic {

  private final KeyspaceMetadata keyspace;
  private final ConsistencyLevel consistencyLevel;
  private final SortedSet<TokenRangeDiagnostic> tokenRangeDiagnostics;

  public DefaultTokenRingDiagnostic(
      @NonNull KeyspaceMetadata keyspace,
      @NonNull ConsistencyLevel consistencyLevel,
      @NonNull Set<TokenRangeDiagnostic> tokenRangeDiagnostics) {
    Objects.requireNonNull(keyspace, "keyspace cannot be null");
    Objects.requireNonNull(consistencyLevel, "consistencyLevel cannot be null");
    Objects.requireNonNull(tokenRangeDiagnostics, "tokenRangeDiagnostics cannot be null");
    this.keyspace = keyspace;
    this.consistencyLevel = consistencyLevel;
    this.tokenRangeDiagnostics = ImmutableSortedSet.copyOf(tokenRangeDiagnostics);
  }

  @NonNull
  @Override
  public KeyspaceMetadata getKeyspace() {
    return keyspace;
  }

  @NonNull
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  @NonNull
  @Override
  public SortedSet<TokenRangeDiagnostic> getTokenRangeDiagnostics() {
    return tokenRangeDiagnostics;
  }

  @NonNull
  @Override
  public Map<String, Object> getDetails() {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    addBasicDetails(builder);
    addTokenRangeDiagnosticDetails(builder);
    return builder.build();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TokenRingDiagnostic)) {
      return false;
    }
    TokenRingDiagnostic that = (TokenRingDiagnostic) o;
    return getKeyspace().equals(that.getKeyspace())
        && getConsistencyLevel().equals(that.getConsistencyLevel())
        && getTokenRangeDiagnostics().equals(that.getTokenRangeDiagnostics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspace, consistencyLevel, tokenRangeDiagnostics);
  }

  protected void addBasicDetails(ImmutableMap.Builder<String, Object> builder) {
    builder
        .put("status", getStatus())
        .put("keyspace", keyspace.getName().asCql(true))
        .put("replication", keyspace.getReplication())
        .put("consistencyLevel", consistencyLevel);
  }

  protected void addTokenRangeDiagnosticDetails(ImmutableMap.Builder<String, Object> builder) {
    Map<Boolean, List<TokenRangeDiagnostic>> availableAndUnavailable =
        tokenRangeDiagnostics.stream()
            .collect(Collectors.partitioningBy(TokenRangeDiagnostic::isAvailable));
    List<TokenRangeDiagnostic> available =
        availableAndUnavailable.getOrDefault(true, Collections.emptyList());
    List<TokenRangeDiagnostic> unavailable =
        availableAndUnavailable.getOrDefault(false, Collections.emptyList());
    builder.put("availableRanges", available.size()).put("unavailableRanges", unavailable.size());
    if (!unavailable.isEmpty()) {
      addTop10UnavailableRanges(builder, unavailable);
    }
  }

  /**
   * Extracts up to 10 of the most severely unavailable ranges and returns a map of availability
   * reports keyed by token ranges, formatted for display.
   */
  private void addTop10UnavailableRanges(
      ImmutableMap.Builder<String, Object> builder, List<TokenRangeDiagnostic> unavailable) {
    builder.put(
        "top10UnavailableRanges",
        unavailable.stream()
            .sorted(this::compareReportsBySeverity)
            .limit(10)
            .collect(
                ImmutableMap.toImmutableMap(
                    diagnostic -> diagnostic.getTokenRange().format(),
                    TokenRangeDiagnostic::getDetails)));
  }

  /**
   * Compares two {@link TokenRangeDiagnostic} instances according to the severity of their
   * unavailability. The higher the number of replicas missing, the higher the severity. If two
   * reports have equal severities, their token ranges are compared lexicographically.
   */
  private int compareReportsBySeverity(
      TokenRangeDiagnostic diagnostic1, TokenRangeDiagnostic diagnostic2) {
    int severity1 = diagnostic1.getAliveReplicas() - diagnostic1.getRequiredReplicas();
    int severity2 = diagnostic2.getAliveReplicas() - diagnostic2.getRequiredReplicas();
    int result = Integer.compare(severity1, severity2);
    return (result == 0)
        ? diagnostic1.getTokenRange().compareTo(diagnostic2.getTokenRange())
        : result;
  }
}
