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
package com.datastax.oss.driver.internal.core.metadata.diagnostic.topology;

import com.datastax.oss.driver.api.core.metadata.diagnostic.TopologyDiagnostic;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class DefaultTopologyDiagnostic implements TopologyDiagnostic {

  private final int total;
  private final int up;
  private final int down;
  private final int unknown;
  private final SortedMap<String, TopologyDiagnostic> localDiagnostics;

  public DefaultTopologyDiagnostic(int total, int up, int down, int unknown) {
    this(total, up, down, unknown, Collections.emptyMap());
  }

  public DefaultTopologyDiagnostic(
      int total,
      int up,
      int down,
      int unknown,
      @NonNull Map<String, TopologyDiagnostic> localDiagnostics) {
    Objects.requireNonNull(localDiagnostics, "localDiagnostics must not be null");
    this.total = total;
    this.up = up;
    this.down = down;
    this.unknown = unknown;
    this.localDiagnostics = ImmutableSortedMap.copyOf(localDiagnostics);
  }

  @Override
  public int getTotal() {
    return this.total;
  }

  @Override
  public int getUp() {
    return this.up;
  }

  @Override
  public int getDown() {
    return this.down;
  }

  @Override
  public int getUnknown() {
    return unknown;
  }

  @NonNull
  @Override
  public Map<String, TopologyDiagnostic> getLocalDiagnostics() {
    return localDiagnostics;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DefaultTopologyDiagnostic)) {
      return false;
    }
    DefaultTopologyDiagnostic that = (DefaultTopologyDiagnostic) o;
    return total == that.total
        && up == that.up
        && down == that.down
        && unknown == that.unknown
        && localDiagnostics.equals(that.localDiagnostics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(total, up, down, unknown, localDiagnostics);
  }

  public static class Builder {

    private int total;
    private int up;
    private int down;
    private int unknown;
    private final Map<String, DefaultTopologyDiagnostic.Builder> localDiagnosticsBuilder =
        new TreeMap<>();

    public void incrementTotal() {
      this.total++;
    }

    public void incrementUp() {
      this.up++;
    }

    public void incrementDown() {
      this.down++;
    }

    public void incrementUnknown() {
      this.unknown++;
    }

    public DefaultTopologyDiagnostic.Builder getLocalDiagnosticsBuilder(@NonNull String localKey) {
      return localDiagnosticsBuilder.compute(
          localKey, (dc, diag) -> diag == null ? new Builder() : diag);
    }

    @NonNull
    public DefaultTopologyDiagnostic build() {
      return new DefaultTopologyDiagnostic(
          total,
          up,
          down,
          unknown,
          localDiagnosticsBuilder.entrySet().stream()
              .collect(Collectors.toMap(Entry::getKey, entry -> entry.getValue().build())));
    }
  }
}
