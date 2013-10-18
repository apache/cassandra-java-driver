/*
 *      Copyright (C) 2012 DataStax Inc.
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/*
 * Computes the token->list<replica> association, given the token ring and token->primary token map.
 *
 * Note: it's not an interface mainly because we don't want to expose it.
 */
abstract class ReplicationStrategy {

    static ReplicationStrategy create(Map<String, String> replicationOptions) {

        String strategyClass = replicationOptions.get("class");
        String repFactorString = replicationOptions.get("replication_factor");
        if (strategyClass == null | repFactorString == null)
            return null;

        try {
            if (strategyClass.contains("SimpleStrategy")) {
                return new SimpleStrategy(Integer.parseInt(repFactorString));
            } else if (strategyClass.contains("SimpleStrategy")) {
                Map<String, Integer> dcRfs = new HashMap<String, Integer>();
                for (Map.Entry<String, String> entry : replicationOptions.entrySet())
                {
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

    abstract Map<Token, Set<Host>> computeTokenToReplicaMap(Map<Token, Host> tokenToPrimary, List<Token> ring);

    private static Token getTokenWrapping(int i, List<Token> ring) {
        return ring.get(i % ring.size());
    }

    static class SimpleStrategy extends ReplicationStrategy {

        private final int replicationFactor;

        private SimpleStrategy(int replicationFactor) {
            this.replicationFactor = replicationFactor;
        }

        Map<Token, Set<Host>> computeTokenToReplicaMap(Map<Token, Host> tokenToPrimary, List<Token> ring) {

            int rf = Math.min(replicationFactor, ring.size());

            Map<Token, Set<Host>> replicaMap = new HashMap<Token, Set<Host>>(tokenToPrimary.size());
            for (int i = 0; i < ring.size(); i++) {
                ImmutableSet.Builder<Host> builder = ImmutableSet.builder();
                for (int j = 0; j < rf; j++)
                    builder.add(tokenToPrimary.get(getTokenWrapping(i+j, ring)));
                replicaMap.put(ring.get(i), builder.build());
            }
            return replicaMap;
        }
    }

    static class NetworkTopologyStrategy extends ReplicationStrategy {

        private final Map<String, Integer> replicationFactors;

        private NetworkTopologyStrategy(Map<String, Integer> replicationFactors) {
            this.replicationFactors = replicationFactors;
        }

        Map<Token, Set<Host>> computeTokenToReplicaMap(Map<Token, Host> tokenToPrimary, List<Token> ring) {

            Map<Token, Set<Host>> replicaMap = new HashMap<Token, Set<Host>>(tokenToPrimary.size());
            for (int i = 0; i < ring.size(); i++) {
                Map<String, Integer> remainings = new HashMap<String, Integer>(replicationFactors);
                ImmutableSet.Builder<Host> builder = ImmutableSet.builder();
                for (int j = 0; j < ring.size(); j++) {
                    Host h = tokenToPrimary.get(getTokenWrapping(j, ring));
                    String dc = h.getDatacenter();
                    if (dc == null)
                        continue;

                    Integer remaining = remainings.get(dc);
                    if (remaining <= 0)
                        continue;

                    builder.add(h);
                    remainings.put(dc, remaining - 1);
                    if (allDone(remainings))
                        break;
                }
                replicaMap.put(ring.get(i), builder.build());
            }
            return replicaMap;
        }

        private boolean allDone(Map<String, Integer> map)
        {
            for (Map.Entry<String, Integer> entry : map.entrySet())
                if (entry.getValue() > 0)
                    return false;
            return true;
        }
    }
}
