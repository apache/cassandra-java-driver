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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.ColumnTypes;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;

/**
 * Launches multiple SCassandra instances, and mocks the appropriate request to make the driver think
 * that they belong to the same cluster.
 *
 * Note: this uses the same addresses as {@link CCMBridge}, so make sure IP aliases have been set up,
 * and a CCM cluster is not running at the same time.
 */
public class SCassandraCluster {
    private static final int BINARY_PORT = 9042;
    private static final int ADMIN_PORT = 9052;

    private final List<Scassandra> scassandras;
    private final List<InetAddress> addresses;
    private final List<PrimingClient> primingClients;

    public SCassandraCluster(String ipPrefix, int nodeCount) {
        scassandras = Lists.newArrayListWithCapacity(nodeCount);
        addresses = Lists.newArrayListWithCapacity(nodeCount);
        primingClients = Lists.newArrayListWithCapacity(nodeCount);

        for (int i = 1; i <= nodeCount; i++) {
            String ip = ipPrefix + i;
            try {
                addresses.add(InetAddress.getByName(ip));
            } catch (UnknownHostException e) {
                throw new AssertionError(e);
            }
            Scassandra scassandra = ScassandraFactory.createServer(ip, BINARY_PORT, ip, ADMIN_PORT);
            scassandra.start();
            scassandras.add(scassandra);

            // Currently Scassandra#primingClient() uses localhost by default, so build manually:
            primingClients.add(PrimingClient.builder()
                .withHost(ip).withPort(scassandra.getAdminPort())
                .build());
        }
        primePeers();
    }

    public List<InetAddress> addresses() {
        return addresses;
    }

    public void stop() {
        for (Scassandra scassandra : scassandras)
            scassandra.stop();
    }

    public SCassandraCluster prime(int node, PrimingRequest request) {
        primingClients.get(node - 1).prime(request);
        return this;
    }

    public void clearAllPrimes() {
        for (PrimingClient primingClient : primingClients)
            primingClient.clearAllPrimes();
        primePeers();
    }

    private void primePeers() {
        for (int i = 0; i < scassandras.size(); i++)
            primePeers(primingClients.get(i), scassandras.get(i));
    }

    @SuppressWarnings("unchecked")
    private void primePeers(PrimingClient primingClient, Scassandra toIgnore) {
        List<Map<String, ?>> rows = Lists.newArrayListWithCapacity(scassandras.size());
        for (int i = 0; i < scassandras.size(); i++) {
            if (scassandras.get(i).equals(toIgnore))
                continue;
            InetAddress address = addresses.get(i);
            rows.add(ImmutableMap.<String, Object>builder()
                .put("peer", address)
                .put("rpc_address", address)
                .put("data_center", "datacenter1")
                .put("rack", "rack1")
                .put("release_version", "2.0.1")
                .put("tokens", ImmutableSet.of(Long.toString(Long.MIN_VALUE + i)))
                .build());
        }
        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.peers")
                .withColumnTypes(SELECT_PEERS_COLUMN_TYPES)
                .withRows(rows)
                .build());
    }

    private static final ImmutableMap<String, ColumnTypes> SELECT_PEERS_COLUMN_TYPES =
        ImmutableMap.<String, ColumnTypes>builder()
            .put("peer", ColumnTypes.Inet)
            .put("rpc_address", ColumnTypes.Inet)
            .put("data_center", ColumnTypes.Text)
            .put("rack", ColumnTypes.Text)
            .put("release_version", ColumnTypes.Text)
            .put("tokens", ColumnTypes.TextSet)
            .build();
}
