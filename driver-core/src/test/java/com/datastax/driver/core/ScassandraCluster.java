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

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.*;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.types.ColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.scassandra.cql.PrimitiveType.*;
import static org.scassandra.cql.SetType.set;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

public class ScassandraCluster {

    private static final Logger logger = LoggerFactory.getLogger(ScassandraCluster.class);

    private final String ipPrefix;

    private final int binaryPort;

    private final List<Scassandra> instances;

    private final Map<Integer, List<Scassandra>> dcNodeMap;

    private final List<Map<String, ?>> keyspaceRows;

    private static final java.util.UUID schemaVersion = UUIDs.random();


    private final Map<Integer, Map<Integer, Map<String, Object>>> forcedPeerInfos;

    ScassandraCluster(Integer[] nodes, String ipPrefix, int binaryPort, int adminPort,
                      List<Map<String, ?>> keyspaceRows,
                      Map<Integer, Map<Integer, Map<String, Object>>> forcedPeerInfos) {
        this.ipPrefix = ipPrefix;
        this.binaryPort = binaryPort;
        this.forcedPeerInfos = forcedPeerInfos;

        int node = 1;
        ImmutableList.Builder<Scassandra> instanceListBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Integer, List<Scassandra>> dcNodeMapBuilder = ImmutableMap.builder();
        for (int dc = 0; dc < nodes.length; dc++) {
            ImmutableList.Builder<Scassandra> dcNodeListBuilder = ImmutableList.builder();
            for (int n = 0; n < nodes[dc]; n++) {
                String ip = ipPrefix + node++;
                Scassandra instance = ScassandraFactory.createServer(ip, binaryPort, ip, adminPort);
                instanceListBuilder = instanceListBuilder.add(instance);
                dcNodeListBuilder = dcNodeListBuilder.add(instance);
            }
            dcNodeMapBuilder.put(dc + 1, dcNodeListBuilder.build());
        }
        instances = instanceListBuilder.build();
        dcNodeMap = dcNodeMapBuilder.build();
        this.keyspaceRows = keyspaceRows;
    }

    public Scassandra node(int node) {
        return instances.get(node - 1);
    }

    public List<Scassandra> nodes() {
        return instances;
    }

    public Scassandra node(int dc, int node) {
        return dcNodeMap.get(dc).get(node - 1);
    }

    public List<Scassandra> nodes(int dc) {
        return dcNodeMap.get(dc);
    }

    public int ipSuffix(int dc, int node) {
        // TODO: Scassandra should be updated to include address to avoid O(n) lookup.
        int nodeCount = 0;
        for (Integer dcNum : new TreeSet<Integer>(dcNodeMap.keySet())) {
            List<Scassandra> nodesInDc = dcNodeMap.get(dc);
            for (int n = 0; n < nodesInDc.size(); n++) {
                nodeCount++;
                if (dcNum == dc && n + 1 == node) {
                    return nodeCount;
                }
            }
        }
        return -1;
    }

    public int getBinaryPort() {
        return binaryPort;
    }

    public InetSocketAddress address(int node) {
        return new InetSocketAddress(ipPrefix + node, binaryPort);
    }

    public InetSocketAddress address(int dc, int node) {
        // TODO: Scassandra should be updated to include address to avoid O(n) lookup.
        int ipSuffix = ipSuffix(dc, node);
        if (ipSuffix == -1)
            return null;
        return new InetSocketAddress(ipPrefix + ipSuffix, binaryPort);
    }

    public Host host(Cluster cluster, int dc, int node) {
        InetAddress address = address(dc, node).getAddress();
        for (Host host : cluster.getMetadata().getAllHosts()) {
            if (host.getAddress().equals(address)) {
                return host;
            }
        }
        return null;
    }

    public static String datacenter(int dc) {
        return "DC" + dc;
    }

    public void init() {
        for (Map.Entry<Integer, List<Scassandra>> dc : dcNodeMap.entrySet()) {
            for (Scassandra node : dc.getValue()) {
                node.start();
                primeMetadata(node);
            }
        }
    }

    public void stop() {
        logger.debug("Stopping ScassandraCluster.");
        for (Scassandra node : instances) {
            node.stop();
        }
    }

    /**
     * First stops each node in {@code dc} and then asserts that each node's {@link Host}
     * is marked down for the given {@link Cluster} instance within 10 seconds.
     * <p/>
     * If any of the nodes are the control host, this node is stopped last, to reduce
     * likelihood of control connection choosing a host that will be shut down.
     *
     * @param cluster cluster to wait for down statuses on.
     * @param dc      DC to stop.
     */
    public void stopDC(Cluster cluster, int dc) {
        logger.debug("Stopping all nodes in {}.", datacenter(dc));
        // If any node is the control host, stop it last.
        int controlHost = -1;
        for (int i = 1; i <= nodes(dc).size(); i++) {
            int id = ipSuffix(dc, i);
            Host host = TestUtils.findHost(cluster, id);
            if (cluster.manager.controlConnection.connectedHost() == host) {
                logger.debug("Node {} identified as control host.  Stopping last.", id);
                controlHost = id;
                continue;
            }
            stop(cluster, id);
        }

        if (controlHost != -1) {
            stop(cluster, controlHost);
        }
    }

    /**
     * Stops a node by id and then asserts that its {@link Host} is marked down
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for down status on.
     * @param node    Node to stop.
     */
    public void stop(Cluster cluster, int node) {
        logger.debug("Stopping node {}.", node);
        Scassandra scassandra = node(node);
        scassandra.stop();
        assertThat(cluster).host(node).goesDownWithin(10, TimeUnit.SECONDS);
    }

    /**
     * Stops a node by dc and id and then asserts that its {@link Host} is marked down
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for down status on.
     * @param dc      Data center node is in.
     * @param node    Node to stop.
     */
    public void stop(Cluster cluster, int dc, int node) {
        logger.debug("Stopping node {} in {}.", node, datacenter(dc));
        stop(cluster, ipSuffix(dc, node));
    }

    /**
     * First starts each node in {@code dc} and then asserts that each node's {@link Host}
     * is marked up for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for up statuses on.
     * @param dc      DC to start.
     */
    public void startDC(Cluster cluster, int dc) {
        logger.debug("Starting all nodes in {}.", datacenter(dc));
        for (int i = 1; i <= nodes(dc).size(); i++) {
            int id = ipSuffix(dc, i);
            start(cluster, id);
        }
    }

    /**
     * Starts a node by id and then asserts that its {@link Host} is marked up
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for up status on.
     * @param node    Node to start.
     */
    public void start(Cluster cluster, int node) {
        logger.debug("Starting node {}.", node);
        Scassandra scassandra = node(node);
        scassandra.start();
        assertThat(cluster).host(node).comesUpWithin(10, TimeUnit.SECONDS);
    }

    /**
     * Starts a node by dc and id and then asserts that its {@link Host} is marked up
     * for the given {@link Cluster} instance within 10 seconds.
     *
     * @param cluster cluster to wait for up status on.
     * @param dc      Data center node is in.
     * @param node    Node to start.
     */
    public void start(Cluster cluster, int dc, int node) {
        logger.debug("Starting node {} in {}.", node, datacenter(dc));
        start(cluster, ipSuffix(dc, node));
    }

    public List<Long> getTokensForDC(int dc) {
        // Offset DCs by dc * 100 to ensure unique tokens.
        int offset = (dc - 1) * 100;
        int dcNodeCount = nodes(dc).size();
        List<Long> tokens = Lists.newArrayListWithExpectedSize(dcNodeCount);
        for (int i = 0; i < dcNodeCount; i++) {
            tokens.add((i * ((long) Math.pow(2, 64) / dcNodeCount) + offset));
        }
        return tokens;
    }

    @SuppressWarnings("unchecked")
    private void primeMetadata(Scassandra node) {
        PrimingClient client = node.primingClient();
        int nodeCount = 0;

        ImmutableList.Builder<Map<String, ?>> rows = ImmutableList.builder();
        Set<ColumnMetadata> localMetadata = Sets.newHashSet(SELECT_LOCAL);
        Set<ColumnMetadata> peersMetadata = Sets.newHashSet(SELECT_PEERS);
        for (Integer dc : new TreeSet<Integer>(dcNodeMap.keySet())) {
            List<Scassandra> nodesInDc = dcNodeMap.get(dc);
            List<Long> tokens = getTokensForDC(dc);
            for (int n = 0; n < nodesInDc.size(); n++) {
                String address = ipPrefix + ++nodeCount;
                Scassandra peer = nodesInDc.get(n);
                Map<String, Object> row;
                if (node == peer) { // prime system.local.
                    row = Maps.newLinkedHashMap();
                    addPeerInfo(row, dc, n + 1, "key", "local");
                    addPeerInfo(row, dc, n + 1, "bootstrapped", "COMPLETED");
                    addPeerInfo(row, dc, n + 1, "broadcast_address", address);
                    addPeerInfo(row, dc, n + 1, "cluster_name", "scassandra");
                    addPeerInfo(row, dc, n + 1, "cql_version", "3.2.0");
                    addPeerInfo(row, dc, n + 1, "data_center", datacenter(dc));
                    addPeerInfo(row, dc, n + 1, "listen_address", getPeerInfo(dc, n + 1, "listen_address", address));
                    addPeerInfo(row, dc, n + 1, "partitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
                    addPeerInfo(row, dc, n + 1, "rack", getPeerInfo(dc, n + 1, "rack", "rack1"));
                    addPeerInfo(row, dc, n + 1, "release_version", getPeerInfo(dc, n + 1, "release_version", "2.1.8"));
                    addPeerInfo(row, dc, n + 1, "tokens", ImmutableSet.of(tokens.get(n)));
                    addPeerInfo(row, dc, n + 1, "schema_version", schemaVersion);
                    addPeerInfo(row, dc, n + 1, "graph", false);

                    if (addPeerInfoIfExists(row, dc, n + 1, "dse_version"))
                        localMetadata.add(column("dse_version", TEXT));
                    if (addPeerInfoIfExists(row, dc, n + 1, "workload"))
                        localMetadata.add(column("workload", TEXT));
                    if (addPeerInfoIfExists(row, dc, n + 1, "workloads"))
                        localMetadata.add(column("workloads", set(TEXT)));
                    client.prime(PrimingRequest.queryBuilder()
                            .withQuery("SELECT * FROM system.local WHERE key='local'")
                            .withThen(then()
                                    .withColumnTypes(localMetadata.toArray(new ColumnMetadata[localMetadata.size()]))
                                    .withRows(row)
                                    .build())
                            .build());

                } else { // prime system.peers.
                    row = Maps.newLinkedHashMap();
                    addPeerInfo(row, dc, n + 1, "peer", address);
                    addPeerInfo(row, dc, n + 1, "rpc_address", address);
                    addPeerInfo(row, dc, n + 1, "data_center", datacenter(dc));
                    addPeerInfo(row, dc, n + 1, "rack", getPeerInfo(dc, n + 1, "rack", "rack1"));
                    addPeerInfo(row, dc, n + 1, "release_version", getPeerInfo(dc, n + 1, "release_version", "2.1.8"));
                    addPeerInfo(row, dc, n + 1, "tokens", ImmutableSet.of(Long.toString(tokens.get(n))));
                    addPeerInfo(row, dc, n + 1, "host_id", UUIDs.random());
                    addPeerInfo(row, dc, n + 1, "schema_version", schemaVersion);
                    addPeerInfo(row, dc, n + 1, "graph", false);

                    if (addPeerInfoIfExists(row, dc, n + 1, "listen_address"))
                        peersMetadata.add(column("listen_address", INET));
                    if (addPeerInfoIfExists(row, dc, n + 1, "dse_version"))
                        peersMetadata.add(column("dse_version", TEXT));
                    if (addPeerInfoIfExists(row, dc, n + 1, "workload"))
                        peersMetadata.add(column("workload", TEXT));
                    if (addPeerInfoIfExists(row, dc, n + 1, "workloads"))
                        peersMetadata.add(column("workloads", set(TEXT)));
                    rows.add(row);
                    client.prime(PrimingRequest.queryBuilder()
                            .withQuery("SELECT * FROM system.peers WHERE peer='" + address + "'")
                            .withThen(then()
                                    .withColumnTypes(peersMetadata.toArray(new ColumnMetadata[peersMetadata.size()]))
                                    .withRows(row)
                                    .build())
                            .build());
                }
            }
        }
        client.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.peers")
                .withThen(then()
                        .withColumnTypes(peersMetadata.toArray(new ColumnMetadata[peersMetadata.size()]))
                        .withRows(rows.build())
                        .build())
                .build());

        // Needed to ensure cluster_name matches what we expect on connection.
        Map<String, Object> clusterNameRow = ImmutableMap.<String, Object>builder()
                .put("cluster_name", "scassandra")
                .build();
        client.prime(PrimingRequest.queryBuilder()
                .withQuery("select cluster_name from system.local")
                .withThen(then()
                        .withColumnTypes(SELECT_CLUSTER_NAME)
                        .withRows(clusterNameRow)
                        .build())
                .build());

        // Prime keyspaces
        client.prime(PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.schema_keyspaces")
                .withThen(then()
                        .withColumnTypes(SELECT_SCHEMA_KEYSPACES)
                        .withRows(keyspaceRows)
                        .build())
                .build());
    }

    private void addPeerInfo(Map<String, Object> input, int dc, int node, String property, Object defaultValue) {
        Object peerInfo = getPeerInfo(dc, node, property, defaultValue);
        if (peerInfo != null) {
            input.put(property, peerInfo);
        }
    }

    private boolean addPeerInfoIfExists(Map<String, Object> input, int dc, int node, String property) {
        Map<Integer, Map<String, Object>> forDc = forcedPeerInfos.get(dc);
        if (forDc == null)
            return false;

        Map<String, Object> forNode = forDc.get(node);
        if (forNode == null)
            return false;

        if (forNode.containsKey(property)) {
            input.put(property, forNode.get(property));
            return true;
        }
        return false;
    }

    private Object getPeerInfo(int dc, int node, String property, Object defaultValue) {
        Map<Integer, Map<String, Object>> forDc = forcedPeerInfos.get(dc);
        if (forDc == null)
            return defaultValue;

        Map<String, Object> forNode = forDc.get(node);
        if (forNode == null)
            return defaultValue;

        return (forNode.containsKey(property))
                ? forNode.get(property)
                : defaultValue;
    }

    public static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_PEERS = {
            column("peer", INET),
            column("rpc_address", INET),
            column("data_center", TEXT),
            column("rack", TEXT),
            column("release_version", TEXT),
            column("tokens", set(TEXT)),
            column("listen_address", INET),
            column("host_id", UUID),
            column("graph", BOOLEAN),
            column("schema_version", UUID)
    };

    public static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_LOCAL = {
            column("key", TEXT),
            column("bootstrapped", TEXT),
            column("broadcast_address", INET),
            column("cluster_name", TEXT),
            column("cql_version", TEXT),
            column("data_center", TEXT),
            column("listen_address", INET),
            column("partitioner", TEXT),
            column("rack", TEXT),
            column("release_version", TEXT),
            column("tokens", set(TEXT)),
            column("graph", BOOLEAN),
            column("schema_version", UUID)
    };

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_CLUSTER_NAME = {
            column("cluster_name", TEXT)
    };

    static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_SCHEMA_KEYSPACES = {
            column("durable_writes", BOOLEAN),
            column("keyspace_name", TEXT),
            column("strategy_class", TEXT),
            column("strategy_options", TEXT)
    };

    public static ScassandraClusterBuilder builder() {
        return new ScassandraClusterBuilder();
    }

    public static class ScassandraClusterBuilder {

        private Integer nodes[] = {1};
        private String ipPrefix = TestUtils.IP_PREFIX;
        private final List<Map<String, ?>> keyspaceRows = Lists.newArrayList();
        private final Map<Integer, Map<Integer, Map<String, Object>>> forcedPeerInfos = Maps.newHashMap();

        public ScassandraClusterBuilder withNodes(Integer... nodes) {
            this.nodes = nodes;
            return this;
        }

        public ScassandraClusterBuilder withIpPrefix(String ipPrefix) {
            this.ipPrefix = ipPrefix;
            return this;
        }

        public ScassandraClusterBuilder withSimpleKeyspace(String name, int replicationFactor) {
            Map<String, Object> simpleKeyspaceRow = ImmutableMap.<String, Object>builder()
                    .put("durable_writes", false)
                    .put("keyspace_name", name)
                    .put("strategy_class", "SimpleStrategy")
                    .put("strategy_options", "{\"replication_factor\":\"" + replicationFactor + "\"}")
                    .build();

            keyspaceRows.add(simpleKeyspaceRow);
            return this;
        }

        public ScassandraClusterBuilder withNetworkTopologyKeyspace(String name, Map<Integer, Integer> replicationFactors) {
            StringBuilder strategyOptionsBuilder = new StringBuilder("{");
            for (Map.Entry<Integer, Integer> dc : replicationFactors.entrySet()) {
                strategyOptionsBuilder.append("\"");
                strategyOptionsBuilder.append(datacenter(dc.getKey()));
                strategyOptionsBuilder.append("\":\"");
                strategyOptionsBuilder.append(dc.getValue());
                strategyOptionsBuilder.append("\",");
            }

            String strategyOptions = strategyOptionsBuilder.substring(0, strategyOptionsBuilder.length() - 1) + "}";

            Map<String, Object> ntsKeyspaceRow = ImmutableMap.<String, Object>builder()
                    .put("durable_writes", false)
                    .put("keyspace_name", name)
                    .put("strategy_class", "NetworkTopologyStrategy")
                    .put("strategy_options", strategyOptions)
                    .build();

            keyspaceRows.add(ntsKeyspaceRow);
            return this;
        }

        public ScassandraClusterBuilder forcePeerInfo(int dc, int node, String name, Object value) {
            Map<Integer, Map<String, Object>> forDc = forcedPeerInfos.get(dc);
            if (forDc == null) {
                forDc = Maps.newHashMap();
                forcedPeerInfos.put(dc, forDc);
            }
            Map<String, Object> forNode = forDc.get(node);
            if (forNode == null) {
                forNode = Maps.newHashMap();
                forDc.put(node, forNode);
            }
            forNode.put(name, value);
            return this;
        }

        public ScassandraCluster build() {
            return new ScassandraCluster(nodes, ipPrefix, TestUtils.findAvailablePort(), TestUtils.findAvailablePort(), keyspaceRows, forcedPeerInfos);
        }
    }
}
