/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.driver.core;

import static com.datastax.driver.core.Assertions.assertThat;
import static org.scassandra.cql.MapType.map;
import static org.scassandra.cql.PrimitiveType.BIG_INT;
import static org.scassandra.cql.PrimitiveType.BOOLEAN;
import static org.scassandra.cql.PrimitiveType.DOUBLE;
import static org.scassandra.cql.PrimitiveType.INET;
import static org.scassandra.cql.PrimitiveType.INT;
import static org.scassandra.cql.PrimitiveType.TEXT;
import static org.scassandra.cql.PrimitiveType.UUID;
import static org.scassandra.cql.SetType.set;
import static org.scassandra.http.client.PrimingRequest.then;
import static org.scassandra.http.client.types.ColumnMetadata.column;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.scassandra.Scassandra;
import org.scassandra.ScassandraFactory;
import org.scassandra.cql.MapType;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.scassandra.http.client.types.ColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScassandraCluster {

  private static final Logger logger = LoggerFactory.getLogger(ScassandraCluster.class);

  private final int binaryPort;

  private final List<Scassandra> instances;
  private final List<InetSocketAddress> binaryAddresses;
  private final List<InetSocketAddress> listenAddresses;

  private final Map<Integer, List<Scassandra>> dcNodeMap;

  private final List<Map<String, ?>> keyspaceRows;

  private final String cassandraVersion;

  private static final java.util.UUID schemaVersion = UUIDs.random();

  private final Map<Integer, Map<Integer, Map<String, Object>>> forcedPeerInfos;

  private final String keyspaceQuery;

  private final org.scassandra.http.client.types.ColumnMetadata[] keyspaceColumnTypes;

  private final boolean peersV2;

  ScassandraCluster(
      Integer[] nodes,
      String ipPrefix,
      int binaryPort,
      int adminPort,
      int listenPort,
      List<Map<String, ?>> keyspaceRows,
      String cassandraVersion,
      Map<Integer, Map<Integer, Map<String, Object>>> forcedPeerInfos,
      boolean peersV2) {
    this(
        nodes,
        binaryPort,
        buildAddresses(nodes, ipPrefix, binaryPort),
        buildAddresses(nodes, ipPrefix, adminPort),
        buildAddresses(nodes, ipPrefix, listenPort),
        keyspaceRows,
        cassandraVersion,
        forcedPeerInfos,
        peersV2);
  }

  ScassandraCluster(
      Integer[] nodes,
      int binaryPort,
      List<InetSocketAddress> binaryAddresses,
      List<InetSocketAddress> adminAddresses,
      List<InetSocketAddress> listenAddresses,
      List<Map<String, ?>> keyspaceRows,
      String cassandraVersion,
      Map<Integer, Map<Integer, Map<String, Object>>> forcedPeerInfos,
      boolean peersV2) {
    this.binaryPort = binaryPort;
    this.binaryAddresses = binaryAddresses;
    this.listenAddresses = listenAddresses;
    // If cassandraVersion is not explicitly provided, use 3.0.10 as current version of SCassandra
    // that
    // supports up to protocol version 4.  Without specifying a newer version, could cause the
    // driver
    // to think the node doesn't support a protocol version that it indeed does.
    this.cassandraVersion = cassandraVersion != null ? cassandraVersion : "3.0.10";
    this.forcedPeerInfos = forcedPeerInfos;

    int node = 1;
    ImmutableList.Builder<Scassandra> instanceListBuilder = ImmutableList.builder();
    ImmutableMap.Builder<Integer, List<Scassandra>> dcNodeMapBuilder = ImmutableMap.builder();
    for (int dc = 0; dc < nodes.length; dc++) {
      ImmutableList.Builder<Scassandra> dcNodeListBuilder = ImmutableList.builder();
      for (int n = 0; n < nodes[dc]; n++) {
        InetSocketAddress binaryAddress = binaryAddresses.get(node - 1);
        InetSocketAddress adminAddress = adminAddresses.get(node - 1);
        Scassandra instance =
            ScassandraFactory.createServer(
                binaryAddress.getAddress().getHostAddress(),
                binaryAddress.getPort(),
                adminAddress.getAddress().getHostAddress(),
                adminAddress.getPort());
        instanceListBuilder.add(instance);
        dcNodeListBuilder.add(instance);
        node++;
      }
      dcNodeMapBuilder.put(dc + 1, dcNodeListBuilder.build());
    }
    instances = instanceListBuilder.build();
    dcNodeMap = dcNodeMapBuilder.build();

    // Prime correct keyspace table based on C* version.
    String[] versionArray = this.cassandraVersion.split("\\.|-");
    double major = Double.parseDouble(versionArray[0] + "." + versionArray[1]);

    if (major < 3.0) {
      this.keyspaceQuery = "SELECT * FROM system.schema_keyspaces";
      this.keyspaceColumnTypes = SELECT_SCHEMA_KEYSPACES;
      this.keyspaceRows = Lists.newArrayList(keyspaceRows);
      // remove replication map as it is not part of the < 3.0 schema.
      for (Map<String, ?> keyspaceRow : this.keyspaceRows) {
        keyspaceRow.remove("replication");
      }
    } else {
      this.keyspaceQuery = "SELECT * FROM system_schema.keyspaces";
      this.keyspaceColumnTypes = SELECT_SCHEMA_KEYSPACES_V3;
      this.keyspaceRows = Lists.newArrayList(keyspaceRows);
      // remove strategy options and strategy class as these are not part of the 3.0+ schema.
      for (Map<String, ?> keyspaceRow : this.keyspaceRows) {
        keyspaceRow.remove("strategy_class");
        keyspaceRow.remove("strategy_options");
      }
    }

    this.peersV2 = peersV2;
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

  /**
   * @return The binary port for nodes in this cluster. Note that this is only relevant when {@link
   *     ScassandraClusterBuilder#withSharedIP} is not used.
   */
  public int getBinaryPort() {
    return binaryPort;
  }

  public InetSocketAddress address(int node) {
    return binaryAddresses.get(node - 1);
  }

  public InetSocketAddress listenAddress(int node) {
    return listenAddresses.get(node - 1);
  }

  public InetSocketAddress address(int dc, int node) {
    int ipSuffix = ipSuffix(dc, node);
    if (ipSuffix == -1) return null;
    return address(ipSuffix);
  }

  public InetSocketAddress listenAddress(int dc, int node) {
    int ipSuffix = ipSuffix(dc, node);
    if (ipSuffix == -1) return null;
    return listenAddress(ipSuffix);
  }

  public Host host(Cluster cluster, int dc, int node) {
    InetSocketAddress address = address(dc, node);
    for (Host host : cluster.getMetadata().getAllHosts()) {
      if (host.getEndPoint().resolve().equals(address)) {
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
      try {
        node.stop();
      } catch (Exception e) {
        logger.error("Could not stop node " + node, e);
      }
    }
  }

  /**
   * First stops each node in {@code dc} and then asserts that each node's {@link Host} is marked
   * down for the given {@link Cluster} instance within 10 seconds.
   *
   * <p>If any of the nodes are the control host, this node is stopped last, to reduce likelihood of
   * control connection choosing a host that will be shut down.
   *
   * @param cluster cluster to wait for down statuses on.
   * @param dc DC to stop.
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
   * Stops a node by id and then asserts that its {@link Host} is marked down for the given {@link
   * Cluster} instance within 10 seconds.
   *
   * @param cluster cluster to wait for down status on.
   * @param node Node to stop.
   */
  public void stop(Cluster cluster, int node) {
    logger.debug("Stopping node {}.", node);
    Scassandra scassandra = node(node);
    try {
      scassandra.stop();
    } catch (Exception e) {
      logger.error("Could not stop node " + scassandra, e);
    }
    assertThat(cluster).host(node).goesDownWithin(10, TimeUnit.SECONDS);
  }

  /**
   * Stops a node by dc and id and then asserts that its {@link Host} is marked down for the given
   * {@link Cluster} instance within 10 seconds.
   *
   * @param cluster cluster to wait for down status on.
   * @param dc Data center node is in.
   * @param node Node to stop.
   */
  public void stop(Cluster cluster, int dc, int node) {
    logger.debug("Stopping node {} in {}.", node, datacenter(dc));
    stop(cluster, ipSuffix(dc, node));
  }

  /**
   * First starts each node in {@code dc} and then asserts that each node's {@link Host} is marked
   * up for the given {@link Cluster} instance within 10 seconds.
   *
   * @param cluster cluster to wait for up statuses on.
   * @param dc DC to start.
   */
  public void startDC(Cluster cluster, int dc) {
    logger.debug("Starting all nodes in {}.", datacenter(dc));
    for (int i = 1; i <= nodes(dc).size(); i++) {
      int id = ipSuffix(dc, i);
      start(cluster, id);
    }
  }

  /**
   * Starts a node by id and then asserts that its {@link Host} is marked up for the given {@link
   * Cluster} instance within 10 seconds.
   *
   * @param cluster cluster to wait for up status on.
   * @param node Node to start.
   */
  public void start(Cluster cluster, int node) {
    logger.debug("Starting node {}.", node);
    Scassandra scassandra = node(node);
    scassandra.start();
    assertThat(cluster).host(node).comesUpWithin(10, TimeUnit.SECONDS);
  }

  /**
   * Starts a node by dc and id and then asserts that its {@link Host} is marked up for the given
   * {@link Cluster} instance within 10 seconds.
   *
   * @param cluster cluster to wait for up status on.
   * @param dc Data center node is in.
   * @param node Node to start.
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

  private void primeMetadata(Scassandra node) {
    PrimingClient client = node.primingClient();
    int nodeCount = 1;

    ImmutableList.Builder<Map<String, ?>> rows = ImmutableList.builder();
    ImmutableList.Builder<Map<String, ?>> rowsV2 = ImmutableList.builder();
    for (Integer dc : new TreeSet<Integer>(dcNodeMap.keySet())) {
      List<Scassandra> nodesInDc = dcNodeMap.get(dc);
      List<Long> tokens = getTokensForDC(dc);
      for (int n = 0; n < nodesInDc.size(); n++) {
        InetSocketAddress binaryAddress = address(nodeCount);
        InetSocketAddress listenAddress = listenAddress(nodeCount);
        nodeCount++;
        Scassandra peer = nodesInDc.get(n);
        if (node == peer) { // prime system.local.
          Map<String, Object> row = Maps.newHashMap();
          addPeerInfo(row, dc, n + 1, "key", "local");
          addPeerInfo(row, dc, n + 1, "bootstrapped", "COMPLETED");
          addPeerInfo(
              row, dc, n + 1, "broadcast_address", listenAddress.getAddress().getHostAddress());
          addPeerInfo(row, dc, n + 1, "cluster_name", "scassandra");
          addPeerInfo(row, dc, n + 1, "cql_version", "3.2.0");
          addPeerInfo(row, dc, n + 1, "data_center", datacenter(dc));
          addPeerInfo(
              row,
              dc,
              n + 1,
              "listen_address",
              getPeerInfo(
                  dc, n + 1, "listen_address", listenAddress.getAddress().getHostAddress()));
          addPeerInfo(row, dc, n + 1, "partitioner", "org.apache.cassandra.dht.Murmur3Partitioner");
          addPeerInfo(row, dc, n + 1, "rack", getPeerInfo(dc, n + 1, "rack", "rack1"));
          addPeerInfo(
              row,
              dc,
              n + 1,
              "release_version",
              getPeerInfo(dc, n + 1, "release_version", cassandraVersion));
          addPeerInfo(row, dc, n + 1, "tokens", ImmutableSet.of(tokens.get(n)));
          addPeerInfo(row, dc, n + 1, "host_id", UUIDs.random());
          addPeerInfo(row, dc, n + 1, "schema_version", schemaVersion);
          addPeerInfo(row, dc, n + 1, "graph", false);

          // These columns might not always be present, we don't have to specify them in the
          // scassandra
          // column metadata as it will default them to text columns.
          addPeerInfoIfExists(row, dc, n + 1, "dse_version");
          addPeerInfoIfExists(row, dc, n + 1, "workload");

          String query = "SELECT * FROM system.local WHERE key='local'";
          if (!peersV2) {
            client.prime(
                PrimingRequest.queryBuilder()
                    .withQuery(query)
                    .withThen(
                        then()
                            .withColumnTypes(SELECT_LOCAL)
                            .withRows(Collections.<Map<String, ?>>singletonList(row))
                            .build())
                    .build());
          } else {
            addPeerInfo(row, dc, n + 1, "broadcast_port", listenAddress.getPort());
            addPeerInfo(row, dc, n + 1, "listen_port", listenAddress.getPort());
            client.prime(
                PrimingRequest.queryBuilder()
                    .withQuery(query)
                    .withThen(
                        then()
                            .withColumnTypes(SELECT_LOCAL_V2)
                            .withRows(Collections.<Map<String, ?>>singletonList(row))
                            .build())
                    .build());
          }
        } else { // prime system.peers.
          Map<String, Object> row = Maps.newHashMap();
          Map<String, Object> rowV2 = Maps.newHashMap();

          addPeerInfo(row, dc, n + 1, "peer", listenAddress.getAddress().getHostAddress());
          addPeerInfo(rowV2, dc, n + 1, "peer", listenAddress.getAddress().getHostAddress());
          addPeerInfo(rowV2, dc, n + 1, "peer_port", listenAddress.getPort());

          addPeerInfo(row, dc, n + 1, "rpc_address", binaryAddress.getAddress().getHostAddress());
          addPeerInfo(
              rowV2, dc, n + 1, "native_address", binaryAddress.getAddress().getHostAddress());
          addPeerInfo(rowV2, dc, n + 1, "native_port", binaryAddress.getPort());

          addPeerInfo(row, dc, n + 1, "data_center", datacenter(dc));
          addPeerInfo(rowV2, dc, n + 1, "data_center", datacenter(dc));
          addPeerInfo(row, dc, n + 1, "rack", getPeerInfo(dc, n + 1, "rack", "rack1"));
          addPeerInfo(rowV2, dc, n + 1, "rack", getPeerInfo(dc, n + 1, "rack", "rack1"));
          addPeerInfo(
              row,
              dc,
              n + 1,
              "release_version",
              getPeerInfo(dc, n + 1, "release_version", cassandraVersion));
          addPeerInfo(
              rowV2,
              dc,
              n + 1,
              "release_version",
              getPeerInfo(dc, n + 1, "release_version", cassandraVersion));
          addPeerInfo(row, dc, n + 1, "tokens", ImmutableSet.of(Long.toString(tokens.get(n))));
          addPeerInfo(rowV2, dc, n + 1, "tokens", ImmutableSet.of(Long.toString(tokens.get(n))));

          java.util.UUID hostId = UUIDs.random();
          addPeerInfo(row, dc, n + 1, "host_id", hostId);
          addPeerInfo(rowV2, dc, n + 1, "host_id", hostId);

          addPeerInfo(row, dc, n + 1, "schema_version", schemaVersion);
          addPeerInfo(rowV2, dc, n + 1, "schema_version", schemaVersion);

          addPeerInfo(row, dc, n + 1, "graph", false);
          addPeerInfo(rowV2, dc, n + 1, "graph", false);

          addPeerInfoIfExists(row, dc, n + 1, "listen_address");
          addPeerInfoIfExists(row, dc, n + 1, "dse_version");
          addPeerInfoIfExists(row, dc, n + 1, "workload");

          addPeerInfoIfExists(rowV2, dc, n + 1, "dse_version");
          addPeerInfoIfExists(rowV2, dc, n + 1, "workload");

          rows.add(row);
          rowsV2.add(rowV2);

          client.prime(
              PrimingRequest.queryBuilder()
                  .withQuery(
                      "SELECT * FROM system.peers WHERE peer='"
                          + listenAddress.getAddress().getHostAddress()
                          + "'")
                  .withThen(
                      then()
                          .withColumnTypes(SELECT_PEERS)
                          .withRows(Collections.<Map<String, ?>>singletonList(row))
                          .build())
                  .build());

          if (peersV2) {
            client.prime(
                PrimingRequest.queryBuilder()
                    .withQuery(
                        "SELECT * FROM system.peers_v2 WHERE peer='"
                            + listenAddress.getAddress().getHostAddress()
                            + "' AND peer_port="
                            + listenAddress.getPort())
                    .withThen(
                        then()
                            .withColumnTypes(SELECT_PEERS_V2)
                            .withRows(Collections.<Map<String, ?>>singletonList(rowV2))
                            .build())
                    .build());
          }
        }
      }
    }

    client.prime(
        PrimingRequest.queryBuilder()
            .withQuery("SELECT * FROM system.peers")
            .withThen(then().withColumnTypes(SELECT_PEERS).withRows(rows.build()).build())
            .build());

    // return invalid error for peers_v2, indicating the table doesn't exist.
    if (!peersV2) {
      client.prime(
          PrimingRequest.queryBuilder()
              .withQuery("SELECT * FROM system.peers_v2")
              .withThen(then().withResult(Result.invalid))
              .build());
    } else {
      client.prime(
          PrimingRequest.queryBuilder()
              .withQuery("SELECT * FROM system.peers_v2")
              .withThen(then().withColumnTypes(SELECT_PEERS_V2).withRows(rowsV2.build()).build())
              .build());
    }

    // Needed to ensure cluster_name matches what we expect on connection.
    Map<String, Object> clusterNameRow =
        ImmutableMap.<String, Object>builder().put("cluster_name", "scassandra").build();
    client.prime(
        PrimingRequest.queryBuilder()
            .withQuery("select cluster_name from system.local where key = 'local'")
            .withThen(
                then()
                    .withColumnTypes(SELECT_CLUSTER_NAME)
                    .withRows(Collections.<Map<String, ?>>singletonList(clusterNameRow))
                    .build())
            .build());

    client.prime(
        PrimingRequest.queryBuilder()
            .withQuery(keyspaceQuery)
            .withThen(then().withColumnTypes(keyspaceColumnTypes).withRows(keyspaceRows).build())
            .build());
  }

  private void addPeerInfo(
      Map<String, Object> input, int dc, int node, String property, Object defaultValue) {
    Object peerInfo = getPeerInfo(dc, node, property, defaultValue);
    if (peerInfo != null) {
      input.put(property, peerInfo);
    }
  }

  private void addPeerInfoIfExists(Map<String, Object> input, int dc, int node, String property) {
    Map<Integer, Map<String, Object>> forDc = forcedPeerInfos.get(dc);
    if (forDc == null) return;

    Map<String, Object> forNode = forDc.get(node);
    if (forNode == null) return;

    if (forNode.containsKey(property)) input.put(property, forNode.get(property));
  }

  private Object getPeerInfo(int dc, int node, String property, Object defaultValue) {
    Map<Integer, Map<String, Object>> forDc = forcedPeerInfos.get(dc);
    if (forDc == null) return defaultValue;

    Map<String, Object> forNode = forDc.get(node);
    if (forNode == null) return defaultValue;

    return (forNode.containsKey(property)) ? forNode.get(property) : defaultValue;
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

  /* system.peers was re-worked for DSE 6.8 */
  public static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_PEERS_DSE68 = {
    column("peer", INET),
    column("rpc_address", INET),
    column("data_center", TEXT),
    column("rack", TEXT),
    column("release_version", TEXT),
    column("tokens", set(TEXT)),
    column("host_id", UUID),
    column("graph", BOOLEAN),
    column("schema_version", UUID),
    column("native_transport_address", INET),
    column("native_transport_port", INT),
    column("native_transport_port_ssl", INT)
  };

  public static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_PEERS_V2 = {
    column("peer", INET),
    column("peer_port", INT),
    column("native_address", INET),
    column("native_port", INT),
    column("native_port_ssl", INT),
    column("data_center", TEXT),
    column("rack", TEXT),
    column("release_version", TEXT),
    column("tokens", set(TEXT)),
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
    column("host_id", UUID),
    column("schema_version", UUID)
  };

  public static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_LOCAL_V2 = {
    column("key", TEXT),
    column("bootstrapped", TEXT),
    column("broadcast_address", INET),
    column("broadcast_port", INT),
    column("cluster_name", TEXT),
    column("cql_version", TEXT),
    column("data_center", TEXT),
    column("listen_address", INET),
    column("listen_port", INT),
    column("partitioner", TEXT),
    column("rack", TEXT),
    column("release_version", TEXT),
    column("tokens", set(TEXT)),
    column("graph", BOOLEAN),
    column("host_id", UUID),
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

  static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_SCHEMA_KEYSPACES_V3 = {
    column("durable_writes", BOOLEAN),
    column("keyspace_name", TEXT),
    column("replication", MapType.map(TEXT, TEXT))
  };

  static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_SCHEMA_COLUMN_FAMILIES = {
    column("bloom_filter_fp_chance", DOUBLE),
    column("caching", TEXT),
    column("cf_id", UUID),
    column("column_aliases", TEXT),
    column("columnfamily_name", TEXT),
    column("comment", TEXT),
    column("compaction_strategy_class", TEXT),
    column("compaction_strategy_options", TEXT),
    column("comparator", TEXT),
    column("compression_parameters", TEXT),
    column("default_time_to_live", INT),
    column("default_validator", TEXT),
    column("dropped_columns", map(TEXT, BIG_INT)),
    column("gc_grace_seconds", INT),
    column("index_interval", INT),
    column("is_dense", BOOLEAN),
    column("key_aliases", TEXT),
    column("key_validator", TEXT),
    column("keyspace_name", TEXT),
    column("local_read_repair_chance", DOUBLE),
    column("max_compaction_threshold", INT),
    column("max_index_interval", INT),
    column("memtable_flush_period_in_ms", INT),
    column("min_compaction_threshold", INT),
    column("min_index_interval", INT),
    column("read_repair_chance", DOUBLE),
    column("speculative_retry", TEXT),
    column("subcomparator", TEXT),
    column("type", TEXT),
    column("value_alias", TEXT)
  };

  static final org.scassandra.http.client.types.ColumnMetadata[] SELECT_SCHEMA_COLUMNS = {
    column("column_name", TEXT),
    column("columnfamily_name", TEXT),
    column("component_index", INT),
    column("index_name", TEXT),
    column("index_options", TEXT),
    column("index_type", TEXT),
    column("keyspace_name", TEXT),
    column("type", TEXT),
    column("validator", TEXT),
  };

  // Primes a minimal system.local row on an Scassandra node.
  // We need a host_id so that the driver can store it in Metadata.hosts
  public static void primeSystemLocalRow(Scassandra scassandra) {
    Set<ColumnMetadata> localMetadata = Sets.newHashSet(SELECT_LOCAL);
    Map<String, Object> row = new HashMap<String, Object>();
    row.put("host_id", java.util.UUID.randomUUID());
    scassandra
        .primingClient()
        .prime(
            PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.local WHERE key='local'")
                .withThen(
                    then()
                        .withColumnTypes(
                            localMetadata.toArray(new ColumnMetadata[localMetadata.size()]))
                        .withRows(Collections.<Map<String, ?>>singletonList(row))));
  }

  public static ScassandraClusterBuilder builder() {
    return new ScassandraClusterBuilder();
  }

  public static class ScassandraClusterBuilder {

    private Integer nodes[] = {1};
    private boolean sharedIP = false;
    private boolean peersV2 = false;
    private String ipPrefix = TestUtils.IP_PREFIX;
    private final List<Map<String, ?>> keyspaceRows = Lists.newArrayList();
    private final Map<Integer, Map<Integer, Map<String, Object>>> forcedPeerInfos =
        Maps.newHashMap();
    private String cassandraVersion = null;

    public ScassandraClusterBuilder withNodes(Integer... nodes) {
      this.nodes = nodes;
      return this;
    }

    public ScassandraClusterBuilder withIpPrefix(String ipPrefix) {
      this.ipPrefix = ipPrefix;
      return this;
    }

    public ScassandraClusterBuilder withSimpleKeyspace(String name, int replicationFactor) {
      Map<String, Object> simpleKeyspaceRow = Maps.newHashMap();
      simpleKeyspaceRow.put("durable_writes", false);
      simpleKeyspaceRow.put("keyspace_name", name);
      simpleKeyspaceRow.put(
          "replication",
          ImmutableMap.<String, String>builder()
              .put("class", "org.apache.cassandra.locator.SimpleStrategy")
              .put("replication_factor", "" + replicationFactor)
              .build());
      simpleKeyspaceRow.put("strategy_class", "SimpleStrategy");
      simpleKeyspaceRow.put(
          "strategy_options", "{\"replication_factor\":\"" + replicationFactor + "\"}");
      keyspaceRows.add(simpleKeyspaceRow);
      return this;
    }

    public ScassandraClusterBuilder withNetworkTopologyKeyspace(
        String name, Map<Integer, Integer> replicationFactors) {
      StringBuilder strategyOptionsBuilder = new StringBuilder("{");
      ImmutableMap.Builder<String, String> replicationBuilder = ImmutableMap.builder();
      replicationBuilder.put("class", "org.apache.cassandra.locator.NetworkTopologyStrategy");

      for (Map.Entry<Integer, Integer> dc : replicationFactors.entrySet()) {
        strategyOptionsBuilder.append("\"");
        strategyOptionsBuilder.append(datacenter(dc.getKey()));
        strategyOptionsBuilder.append("\":\"");
        strategyOptionsBuilder.append(dc.getValue());
        strategyOptionsBuilder.append("\",");
        replicationBuilder.put(datacenter(dc.getKey()), "" + dc.getValue());
      }

      String strategyOptions =
          strategyOptionsBuilder.substring(0, strategyOptionsBuilder.length() - 1) + "}";

      Map<String, Object> ntsKeyspaceRow = Maps.newHashMap();
      ntsKeyspaceRow.put("durable_writes", false);
      ntsKeyspaceRow.put("keyspace_name", name);
      ntsKeyspaceRow.put("strategy_class", "NetworkTopologyStrategy");
      ntsKeyspaceRow.put("strategy_options", strategyOptions);
      ntsKeyspaceRow.put("replication", replicationBuilder.build());
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

    public ScassandraClusterBuilder withPeersV2(boolean enabled) {
      this.peersV2 = enabled;
      return this;
    }

    public ScassandraClusterBuilder withPeersV2() {
      return withPeersV2(true);
    }

    public ScassandraClusterBuilder withSharedIP(boolean enabled) {
      this.sharedIP = enabled;
      if (this.sharedIP) {
        this.withPeersV2();
      }
      return this;
    }

    public ScassandraClusterBuilder withSharedIP() {
      return withSharedIP(true);
    }

    public ScassandraClusterBuilder withCassandraVersion(String version) {
      this.cassandraVersion = version;
      return this;
    }

    public ScassandraCluster build() {
      if (sharedIP) {
        try {
          InetAddress address = InetAddress.getByName(ipPrefix + "1");
          return new ScassandraCluster(
              nodes,
              TestUtils.findAvailablePort(),
              buildAddresses(nodes, address),
              buildAddresses(nodes, address),
              buildAddresses(nodes, address),
              keyspaceRows,
              cassandraVersion,
              forcedPeerInfos,
              peersV2);
        } catch (UnknownHostException uhe) {
          throw new RuntimeException(uhe);
        }
      } else {
        return new ScassandraCluster(
            nodes,
            ipPrefix,
            TestUtils.findAvailablePort(),
            TestUtils.findAvailablePort(),
            TestUtils.findAvailablePort(),
            keyspaceRows,
            cassandraVersion,
            forcedPeerInfos,
            peersV2);
      }
    }
  }

  public static List<InetSocketAddress> buildAddresses(Integer[] nodes, String ipPrefix, int port) {
    int node = 1;
    List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
    for (int nodesInDcCount : nodes) {
      for (int n = 0; n < nodesInDcCount; n++) {
        String ip = ipPrefix + node++;
        addresses.add(new InetSocketAddress(ip, port));
      }
    }
    return addresses;
  }

  public static List<InetSocketAddress> buildAddresses(Integer[] nodes, InetAddress address) {
    List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
    for (int nodesInDcCount : nodes) {
      for (int n = 0; n < nodesInDcCount; n++) {
        addresses.add(new InetSocketAddress(address, TestUtils.findAvailablePort()));
      }
    }
    return addresses;
  }
}
