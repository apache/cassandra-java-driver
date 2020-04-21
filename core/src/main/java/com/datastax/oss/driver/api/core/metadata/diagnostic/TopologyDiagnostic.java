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
  Map<String, NodeGroupDiagnostic> getLocalDiagnostics();

  /**
   * Returns the status of the cluster topology. By default this is the same as {@code
   * getGlobalDiagnostic().getStatus()}. See {@link NodeGroupDiagnostic#getStatus()} for details
   * about possible statuses and their meanings.
   */
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

  /**
   * A health {@link Diagnostic} for a group of nodes, detailing how many nodes were found in total,
   * how many were up, and how many were down.
   *
   * <p>The breadth of the report's scope depends on how it was created; it may refer to the entire
   * cluster, or to just a datacenter.
   */
  interface NodeGroupDiagnostic extends Diagnostic {

    /** @return the total number of nodes in this group. */
    int getTotal();

    /**
     * Returns the number of nodes in this group whose {@linkplain
     * com.datastax.oss.driver.api.core.metadata.Node#getState() state} is {@link
     * com.datastax.oss.driver.api.core.metadata.NodeState#UP UP}.
     *
     * @return the number of nodes in this group that are known to be up.
     */
    int getUp();

    /**
     * Returns the number of nodes in this group whose {@linkplain
     * com.datastax.oss.driver.api.core.metadata.Node#getState() state} is {@link
     * com.datastax.oss.driver.api.core.metadata.NodeState#DOWN DOWN} or {@link
     * com.datastax.oss.driver.api.core.metadata.NodeState#FORCED_DOWN FORCED_DOWN}.
     *
     * @return the number of nodes in this group that are known to be down.
     */
    int getDown();

    /**
     * Returns the number of nodes in this group whose {@linkplain
     * com.datastax.oss.driver.api.core.metadata.Node#getState() state} is {@link
     * com.datastax.oss.driver.api.core.metadata.NodeState#UNKNOWN UNKNOWN}.
     *
     * <p>Nodes may be in an unknown state if the driver hasn't connected to them at all, and it has
     * not received any Gossip event indicating their actual state. Most of such nodes should
     * actually be up.
     *
     * @return the number of nodes in this group that are in unknown state.
     */
    int getUnknown();

    /**
     * Returns the status of this node group. The status will be {@link Status#AVAILABLE} if all
     * nodes are up; {@link Status#UNAVAILABLE} if all nodes are down, or if the group has no node
     * at all. In all other cases the status will be {@link Status#PARTIALLY_AVAILABLE}.
     */
    @NonNull
    @Override
    default Status getStatus() {
      if (getTotal() == 0 || getDown() == getTotal()) {
        return Status.UNAVAILABLE;
      }
      if (getDown() == 0 && getUnknown() == 0) {
        return Status.AVAILABLE;
      }
      return Status.PARTIALLY_AVAILABLE;
    }

    @NonNull
    @Override
    default Map<String, Object> getDetails() {
      return ImmutableMap.of(
          "total", getTotal(), "up", getUp(), "down", getDown(), "unknown", getUnknown());
    }
  }
}
