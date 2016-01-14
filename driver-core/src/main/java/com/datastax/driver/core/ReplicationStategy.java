/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/*
 * Computes the token->list<replica> association, given the token ring and token->primary token map.
 *
 * Note: it's not an interface mainly because we don't want to expose it.
 */
abstract class ReplicationStrategy {

    static ReplicationStrategy create(Map<String, String> replicationOptions) {

        String strategyClass = replicationOptions.get("class");
        if (strategyClass == null)
            return null;

        try {
            if (strategyClass.contains("SimpleStrategy")) {
                String repFactorString = replicationOptions.get("replication_factor");
                return repFactorString == null ? null : new SimpleStrategy(Integer.parseInt(repFactorString));
            } else if (strategyClass.contains("NetworkTopologyStrategy")) {
                Map<String, Integer> dcRfs = new HashMap<String, Integer>();
                for (Map.Entry<String, String> entry : replicationOptions.entrySet()) {
                    if (entry.getKey().equals("class"))
                        continue;

                    dcRfs.put(entry.getKey(), Integer.parseInt(entry.getValue()));
                }
                return new NetworkTopologyStrategy(dcRfs);
            } else {
                // We might want to support oldNetworkTopologyStrategy, though not sure anyone still using that
                return null;
            }
        } catch (NumberFormatException e) {
            // Cassandra wouldn't let that pass in the first place so this really should never happen
            return null;
        }
    }

    abstract Map<Token, Set<Host>> computeTokenToReplicaMap(String keyspaceName, Map<Token, Host> tokenToPrimary, List<Token> ring);

    private static Token getTokenWrapping(int i, List<Token> ring) {
        return ring.get(i % ring.size());
    }

    static class SimpleStrategy extends ReplicationStrategy {

        private final int replicationFactor;

        private SimpleStrategy(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        @Override
        Map<Token, Set<Host>> computeTokenToReplicaMap(String keyspaceName, Map<Token, Host> tokenToPrimary, List<Token> ring) {

            int rf = Math.min(replicationFactor, ring.size());

            Map<Token, Set<Host>> replicaMap = new HashMap<Token, Set<Host>>(tokenToPrimary.size());
            for (int i = 0; i < ring.size(); i++) {
                // Consecutive sections of the ring can assigned to the same host
                Set<Host> replicas = new LinkedHashSet<Host>();
                for (int j = 0; j < ring.size() && replicas.size() < rf; j++)
                    replicas.add(tokenToPrimary.get(getTokenWrapping(i + j, ring)));
                replicaMap.put(ring.get(i), ImmutableSet.copyOf(replicas));
            }
            return replicaMap;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            SimpleStrategy that = (SimpleStrategy) o;

            return replicationFactor == that.replicationFactor;

        }

        @Override
        public int hashCode() {
            return replicationFactor;
        }
    }

    static class NetworkTopologyStrategy extends ReplicationStrategy {
        private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

        private final Map<String, Integer> replicationFactors;

        private NetworkTopologyStrategy(Map<String, Integer> replicationFactors) {
            this.replicationFactors = replicationFactors;
        }

        @Override
        Map<Token, Set<Host>> computeTokenToReplicaMap(String keyspaceName, Map<Token, Host> tokenToPrimary, List<Token> ring) {

            logger.debug("Computing token to replica map for keyspace: {}.", keyspaceName);

            // Track how long it takes to compute the token to replica map
            long startTime = System.currentTimeMillis();

            // This is essentially a copy of org.apache.cassandra.locator.NetworkTopologyStrategy
            Map<String, Set<String>> racks = getRacksInDcs(tokenToPrimary.values());
            Map<Token, Set<Host>> replicaMap = new HashMap<Token, Set<Host>>(tokenToPrimary.size());
            Map<String, Integer> dcHostCount = Maps.newHashMapWithExpectedSize(replicationFactors.size());
            Set<String> warnedDcs = Sets.newHashSetWithExpectedSize(replicationFactors.size());
            // find maximum number of nodes in each DC
            for (Host host : Sets.newHashSet(tokenToPrimary.values())) {
                String dc = host.getDatacenter();
                if (dcHostCount.get(dc) == null) {
                    dcHostCount.put(dc, 0);
                }
                dcHostCount.put(dc, dcHostCount.get(dc) + 1);
            }
            for (int i = 0; i < ring.size(); i++) {
                Map<String, Set<Host>> allDcReplicas = new HashMap<String, Set<Host>>();
                Map<String, Set<String>> seenRacks = new HashMap<String, Set<String>>();
                Map<String, Set<Host>> skippedDcEndpoints = new HashMap<String, Set<Host>>();
                for (String dc : replicationFactors.keySet()) {
                    allDcReplicas.put(dc, new HashSet<Host>());
                    seenRacks.put(dc, new HashSet<String>());
                    skippedDcEndpoints.put(dc, new LinkedHashSet<Host>()); // preserve order
                }

                // Preserve order - primary replica will be first
                Set<Host> replicas = new LinkedHashSet<Host>();
                for (int j = 0; j < ring.size() && !allDone(allDcReplicas, dcHostCount); j++) {
                    Host h = tokenToPrimary.get(getTokenWrapping(i + j, ring));
                    String dc = h.getDatacenter();
                    if (dc == null || !allDcReplicas.containsKey(dc))
                        continue;

                    Integer rf = replicationFactors.get(dc);
                    Set<Host> dcReplicas = allDcReplicas.get(dc);
                    if (rf == null || dcReplicas.size() >= rf)
                        continue;

                    String rack = h.getRack();
                    // Check if we already visited all racks in dc
                    if (rack == null || seenRacks.get(dc).size() == racks.get(dc).size()) {
                        replicas.add(h);
                        dcReplicas.add(h);
                    } else {
                        // Is this a new rack?
                        if (seenRacks.get(dc).contains(rack)) {
                            skippedDcEndpoints.get(dc).add(h);
                        } else {
                            replicas.add(h);
                            dcReplicas.add(h);
                            seenRacks.get(dc).add(rack);
                            // If we've run out of distinct racks, add the nodes skipped so far
                            if (seenRacks.get(dc).size() == racks.get(dc).size()) {
                                Iterator<Host> skippedIt = skippedDcEndpoints.get(dc).iterator();
                                while (skippedIt.hasNext() && dcReplicas.size() < rf) {
                                    Host nextSkipped = skippedIt.next();
                                    replicas.add(nextSkipped);
                                    dcReplicas.add(nextSkipped);
                                }
                            }
                        }
                    }
                }

                // If we haven't found enough replicas after a whole trip around the ring, this probably
                // means that the replication factors are broken.
                // Warn the user because that leads to quadratic performance of this method (JAVA-702).
                for (Map.Entry<String, Set<Host>> entry : allDcReplicas.entrySet()) {
                    String dcName = entry.getKey();
                    int expectedFactor = replicationFactors.get(dcName);
                    int achievedFactor = entry.getValue().size();
                    if (achievedFactor < expectedFactor && !warnedDcs.contains(dcName)) {
                        logger.warn("Error while computing token map for keyspace {} with datacenter {}: "
                                        + "could not achieve replication factor {} (found {} replicas only), "
                                        + "check your keyspace replication settings.",
                                keyspaceName, dcName, expectedFactor, achievedFactor);
                        // only warn once per DC
                        warnedDcs.add(dcName);
                    }
                }

                replicaMap.put(ring.get(i), ImmutableSet.copyOf(replicas));
            }

            long duration = System.currentTimeMillis() - startTime;
            logger.debug("Token to replica map computation for keyspace {} completed in {} milliseconds",
                    keyspaceName, duration);

            return replicaMap;
        }

        private boolean allDone(Map<String, Set<Host>> map, Map<String, Integer> dcHostCount) {
            for (Map.Entry<String, Set<Host>> entry : map.entrySet()) {
                String dc = entry.getKey();
                int dcCount = dcHostCount.get(dc) == null ? 0 : dcHostCount.get(dc);
                if (entry.getValue().size() < Math.min(replicationFactors.get(dc), dcCount))
                    return false;
            }
            return true;
        }

        private Map<String, Set<String>> getRacksInDcs(Iterable<Host> hosts) {
            Map<String, Set<String>> result = new HashMap<String, Set<String>>();
            for (Host host : hosts) {
                Set<String> racks = result.get(host.getDatacenter());
                if (racks == null) {
                    racks = new HashSet<String>();
                    result.put(host.getDatacenter(), racks);
                }
                racks.add(host.getRack());
            }
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            NetworkTopologyStrategy that = (NetworkTopologyStrategy) o;

            return replicationFactors.equals(that.replicationFactors);

        }

        @Override
        public int hashCode() {
            return replicationFactors.hashCode();
        }
    }

}
