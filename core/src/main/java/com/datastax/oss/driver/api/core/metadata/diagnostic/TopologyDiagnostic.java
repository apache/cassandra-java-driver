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
package com.datastax.oss.driver.api.core.metadata.diagnostic;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

/**
 * A health {@link Diagnostic} on the availability of nodes in the cluster. It reports, globally and
 * for each datacenter, how many nodes were found, and how many were up and how many were down.
 */
public interface TopologyDiagnostic extends Diagnostic {

  /**
   * Returns a {@link NodeGroupDiagnostic} for the entire cluster as a whole, regardless of
   * datacenter boundaries.
   */
  @NonNull
  NodeGroupDiagnostic getGlobalDiagnostic();

  /**
   * Returns datacenter-specific {@link NodeGroupDiagnostic} instances, keyed by datacenter name.
   */
  @NonNull
  SortedMap<String, NodeGroupDiagnostic> getLocalDiagnostics();

  @NonNull
  @Override
  default Status getStatus() {
    return getGlobalDiagnostic().getStatus();
  }

  @NonNull
  @Override
  default Map<String, Object> getDetails() {
    return ImmutableMap.<String, Object>builder()
        .put("status", getStatus())
        .putAll(getGlobalDiagnostic().getDetails())
        .putAll(
            getLocalDiagnostics().entrySet().stream()
                .collect(
                    ImmutableMap.toImmutableMap(
                        Entry::getKey, entry -> entry.getValue().getDetails())))
        .build();
  }
}
