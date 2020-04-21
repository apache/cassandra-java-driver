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

import com.datastax.oss.driver.api.core.metadata.diagnostic.NodeGroupDiagnostic;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TopologyDiagnostic;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSortedMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

public class DefaultTopologyDiagnostic implements TopologyDiagnostic {

  private final NodeGroupDiagnostic globalDiagnostic;

  private final SortedMap<String, NodeGroupDiagnostic> localDiagnostics;

  public DefaultTopologyDiagnostic(
      @NonNull NodeGroupDiagnostic globalDiagnostic,
      @NonNull Map<String, NodeGroupDiagnostic> localDiagnostics) {
    Objects.requireNonNull(globalDiagnostic, "globalDiagnostic must not be null");
    Objects.requireNonNull(localDiagnostics, "localDiagnostics must not be null");
    this.globalDiagnostic = globalDiagnostic;
    this.localDiagnostics = ImmutableSortedMap.copyOf(localDiagnostics);
  }

  @NonNull
  @Override
  public NodeGroupDiagnostic getGlobalDiagnostic() {
    return globalDiagnostic;
  }

  @NonNull
  @Override
  public Map<String, NodeGroupDiagnostic> getLocalDiagnostics() {
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
    return globalDiagnostic.equals(that.globalDiagnostic)
        && localDiagnostics.equals(that.localDiagnostics);
  }

  @Override
  public int hashCode() {
    return Objects.hash(globalDiagnostic, localDiagnostics);
  }
}
