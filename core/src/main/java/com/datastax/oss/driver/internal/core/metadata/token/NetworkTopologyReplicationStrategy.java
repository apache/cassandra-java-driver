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
package com.datastax.oss.driver.internal.core.metadata.token;

import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Maps;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
class NetworkTopologyReplicationStrategy implements ReplicationStrategy {

  private static final Logger LOG =
      LoggerFactory.getLogger(NetworkTopologyReplicationStrategy.class);

  private final Map<String, String> replicationConfig;
  private final Map<String, ReplicationFactor> replicationFactors;
  private final String logPrefix;

  NetworkTopologyReplicationStrategy(Map<String, String> replicationConfig, String logPrefix) {
    this.replicationConfig = replicationConfig;
    ImmutableMap.Builder<String, ReplicationFactor> factorsBuilder = ImmutableMap.builder();
    for (Map.Entry<String, String> entry : replicationConfig.entrySet()) {
      if (!entry.getKey().equals("class")) {
        factorsBuilder.put(entry.getKey(), ReplicationFactor.fromString(entry.getValue()));
      }
    }
    this.replicationFactors = factorsBuilder.build();
    this.logPrefix = logPrefix;
  }

  @Override
  public Map<Token, Set<Node>> computeReplicasByToken(
      Map<Token, Node> tokenToPrimary, List<Token> ring) {

    // The implementation of this method was adapted from
    // org.apache.cassandra.locator.NetworkTopologyStrategy

    ImmutableMap.Builder<Token, Set<Node>> result = ImmutableMap.builder();
    Map<String, Set<String>> racks = getRacksInDcs(tokenToPrimary.values());
    Map<String, Integer> dcNodeCount = Maps.newHashMapWithExpectedSize(replicationFactors.size());
    Set<String> warnedDcs = Sets.newHashSetWithExpectedSize(replicationFactors.size());
    CanonicalNodeSetBuilder replicasBuilder = new CanonicalNodeSetBuilder();

    // find maximum number of nodes in each DC
    for (Node node : Sets.newHashSet(tokenToPrimary.values())) {
      String dc = node.getDatacenter();
      dcNodeCount.merge(dc, 1, Integer::sum);
    }
    for (int i = 0; i < ring.size(); i++) {
      replicasBuilder.clear();

      Map<String, Set<Node>> allDcReplicas = new HashMap<>();
      Map<String, Set<String>> seenRacks = new HashMap<>();
      Map<String, Set<Node>> skippedDcEndpoints = new HashMap<>();
      for (String dc : replicationFactors.keySet()) {
        allDcReplicas.put(dc, new HashSet<>());
        seenRacks.put(dc, new HashSet<>());
        skippedDcEndpoints.put(dc, new LinkedHashSet<>()); // preserve order
      }

      for (int j = 0; j < ring.size() && !allDone(allDcReplicas, dcNodeCount); j++) {
        Node h = tokenToPrimary.get(getTokenWrapping(i + j, ring));
        String dc = h.getDatacenter();
        if (dc == null || !allDcReplicas.containsKey(dc)) {
          continue;
        }
        ReplicationFactor dcConfig = replicationFactors.get(dc);
        assert dcConfig != null; // since allDcReplicas.containsKey(dc)
        int rf = dcConfig.fullReplicas();
        Set<Node> dcReplicas = allDcReplicas.get(dc);
        if (dcReplicas.size() >= rf) {
          continue;
        }
        String rack = h.getRack();
        // Check if we already visited all racks in dc
        if (rack == null || seenRacks.get(dc).size() == racks.get(dc).size()) {
          replicasBuilder.add(h);
          dcReplicas.add(h);
        } else {
          // Is this a new rack?
          if (seenRacks.get(dc).contains(rack)) {
            skippedDcEndpoints.get(dc).add(h);
          } else {
            replicasBuilder.add(h);
            dcReplicas.add(h);
            seenRacks.get(dc).add(rack);
            // If we've run out of distinct racks, add the nodes skipped so far
            if (seenRacks.get(dc).size() == racks.get(dc).size()) {
              Iterator<Node> skippedIt = skippedDcEndpoints.get(dc).iterator();
              while (skippedIt.hasNext() && dcReplicas.size() < rf) {
                Node nextSkipped = skippedIt.next();
                replicasBuilder.add(nextSkipped);
                dcReplicas.add(nextSkipped);
              }
            }
          }
        }
      }
      // If we haven't found enough replicas after a whole trip around the ring, this probably
      // means that the replication factors are broken.
      // Warn the user because that leads to quadratic performance of this method (JAVA-702).
      for (Map.Entry<String, Set<Node>> entry : allDcReplicas.entrySet()) {
        String dcName = entry.getKey();
        int expectedFactor = replicationFactors.get(dcName).fullReplicas();
        int achievedFactor = entry.getValue().size();
        if (achievedFactor < expectedFactor && !warnedDcs.contains(dcName)) {
          LOG.warn(
              "[{}] Error while computing token map for replication settings {}: "
                  + "could not achieve replication factor {} for datacenter {} (found only {} replicas).",
              logPrefix,
              replicationConfig,
              expectedFactor,
              dcName,
              achievedFactor);
          // only warn once per DC
          warnedDcs.add(dcName);
        }
      }

      result.put(ring.get(i), replicasBuilder.build());
    }
    return result.build();
  }

  private boolean allDone(Map<String, Set<Node>> map, Map<String, Integer> dcNodeCount) {
    for (Map.Entry<String, Set<Node>> entry : map.entrySet()) {
      String dc = entry.getKey();
      int dcCount = (dcNodeCount.get(dc) == null) ? 0 : dcNodeCount.get(dc);
      if (entry.getValue().size() < Math.min(replicationFactors.get(dc).fullReplicas(), dcCount)) {
        return false;
      }
    }
    return true;
  }

  private Map<String, Set<String>> getRacksInDcs(Iterable<Node> nodes) {
    Map<String, Set<String>> result = new HashMap<>();
    for (Node node : nodes) {
      Set<String> racks = result.computeIfAbsent(node.getDatacenter(), k -> new HashSet<>());
      racks.add(node.getRack());
    }
    return result;
  }

  private static Token getTokenWrapping(int i, List<Token> ring) {
    return ring.get(i % ring.size());
  }
}
