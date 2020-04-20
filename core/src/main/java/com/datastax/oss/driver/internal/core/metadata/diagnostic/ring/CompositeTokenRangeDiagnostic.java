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
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

/**
 * A composite {@link TokenRangeDiagnostic} that keeps track of the number of alive replicas in each
 * datacenter individually.
 *
 * <p>This report is only suitable for the special consistency level {@link
 * ConsistencyLevel#EACH_QUORUM}.
 */
public class CompositeTokenRangeDiagnostic implements TokenRangeDiagnostic {

  private final TokenRange range;

  private final Map<String, TokenRangeDiagnostic> childDiagnostics;

  public CompositeTokenRangeDiagnostic(
      @NonNull TokenRange range, @NonNull Map<String, TokenRangeDiagnostic> childDiagnostics) {
    Objects.requireNonNull(range, "range cannot be null");
    Objects.requireNonNull(childDiagnostics, "childDiagnostics cannot be null");
    this.range = range;
    this.childDiagnostics = ImmutableMap.copyOf(childDiagnostics);
  }

  @NonNull
  @Override
  public TokenRange getTokenRange() {
    return range;
  }

  @Override
  public boolean isAvailable() {
    return childDiagnostics.values().stream().allMatch(TokenRangeDiagnostic::isAvailable);
  }

  @Override
  public int getRequiredReplicas() {
    return childDiagnostics.values().stream()
        .map(TokenRangeDiagnostic::getRequiredReplicas)
        .reduce(0, Integer::sum);
  }

  @Override
  public int getAliveReplicas() {
    return childDiagnostics.values().stream()
        .map(TokenRangeDiagnostic::getAliveReplicas)
        .reduce(0, Integer::sum);
  }

  public Map<String, TokenRangeDiagnostic> getChildDiagnostics() {
    return childDiagnostics;
  }

  @NonNull
  @Override
  public Map<String, Object> getDetails() {
    return getChildDiagnostics().entrySet().stream()
        .collect(
            ImmutableMap.toImmutableMap(Entry::getKey, entry -> entry.getValue().getDetails()));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CompositeTokenRangeDiagnostic)) {
      return false;
    }
    CompositeTokenRangeDiagnostic that = (CompositeTokenRangeDiagnostic) o;
    return range.equals(that.range) && childDiagnostics.equals(that.childDiagnostics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(range, childDiagnostics);
  }

  public static class Builder {

    private final TokenRange range;

    private final ImmutableMap.Builder<String, TokenRangeDiagnostic> childDiagnosticsBuilder =
        ImmutableMap.builder();

    public Builder(@NonNull TokenRange range) {
      Objects.requireNonNull(range, "range cannot be null");
      this.range = range;
    }

    public void addChildDiagnostic(
        @NonNull String datacenter, @NonNull TokenRangeDiagnostic childDiagnostic) {
      Objects.requireNonNull(datacenter, "datacenter cannot be null");
      Objects.requireNonNull(childDiagnostic, "childDiagnostic cannot be null");
      childDiagnosticsBuilder.put(datacenter, childDiagnostic);
    }

    public CompositeTokenRangeDiagnostic build() {
      return new CompositeTokenRangeDiagnostic(range, childDiagnosticsBuilder.build());
    }
  }
}
