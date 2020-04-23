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

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.api.core.metadata.diagnostic.Status;
import com.datastax.oss.driver.api.core.metadata.diagnostic.TopologyDiagnostic;
import com.datastax.oss.driver.internal.core.metadata.diagnostic.ring.TokenRingDiagnosticGenerator;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * A component that checks the health of a Cassandra cluster, and reports which nodes are up or
 * down.
 */
public class TopologyDiagnosticGenerator {

  private final Metadata metadata;

  public TopologyDiagnosticGenerator(@NonNull Metadata metadata) {
    Objects.requireNonNull(metadata, "metadata must not be null");
    this.metadata = metadata;
  }

  /**
   * Generates a {@link TopologyDiagnostic}, that is, how many nodes were found, how many are up,
   * and how many are down, both globally and on a per-datacenter breakdown.
   *
   * <p>Since a Cassandra cluster is usually resilient to the loss of many nodes, a cluster can
   * still be healthy even if many nodes are missing. Therefore this generator should not force the
   * health status to {@link Status#UNAVAILABLE}, unless the entire cluster is down.
   *
   * <p>For a more fine-grained health report, use {@link TokenRingDiagnosticGenerator}, which is
   * capable of determining if the cluster is still healthy enough, even when some nodes are down.
   */
  public TopologyDiagnostic generate() {
    Map<UUID, Node> nodes = metadata.getNodes();
    DefaultTopologyDiagnostic.Builder globalDiagnostic = new DefaultTopologyDiagnostic.Builder();
    for (Node node : nodes.values()) {
      incrementCounters(globalDiagnostic, node);
      String datacenter = node.getDatacenter();
      if (datacenter != null) {
        DefaultTopologyDiagnostic.Builder dcDiagnostic =
            globalDiagnostic.getLocalDiagnosticsBuilder(datacenter);
        incrementCounters(dcDiagnostic, node);
        String rack = node.getRack();
        if (rack != null) {
          DefaultTopologyDiagnostic.Builder rackDiagnostic =
              dcDiagnostic.getLocalDiagnosticsBuilder(rack);
          incrementCounters(rackDiagnostic, node);
        }
      }
    }
    return globalDiagnostic.build();
  }

  protected void incrementCounters(DefaultTopologyDiagnostic.Builder diagnostic, Node node) {
    diagnostic.incrementTotal();
    if (node.getState() == NodeState.UP) {
      diagnostic.incrementUp();
    } else if (node.getState() == NodeState.UNKNOWN) {
      diagnostic.incrementUnknown();
    } else {
      diagnostic.incrementDown();
    }
  }
}
