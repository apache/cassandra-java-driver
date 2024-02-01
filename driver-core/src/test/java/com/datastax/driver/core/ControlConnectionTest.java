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
import static com.datastax.driver.core.CreateCCM.TestMode.PER_METHOD;
import static com.datastax.driver.core.ScassandraCluster.SELECT_LOCAL;
import static com.datastax.driver.core.ScassandraCluster.SELECT_PEERS;
import static com.datastax.driver.core.ScassandraCluster.SELECT_PEERS_DSE68;
import static com.datastax.driver.core.ScassandraCluster.SELECT_PEERS_V2;
import static com.datastax.driver.core.ScassandraCluster.datacenter;
import static com.datastax.driver.core.TestUtils.nonQuietClusterCloseOptions;
import static com.google.common.collect.Lists.newArrayList;
import static org.scassandra.http.client.PrimingRequest.then;

import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.DelegatingLoadBalancingPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.utils.CassandraVersion;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Level;
import org.scassandra.http.client.PrimingClient;
import org.scassandra.http.client.PrimingRequest;
import org.scassandra.http.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CreateCCM(PER_METHOD)
@CCMConfig(dirtiesContext = true, createCluster = false)
public class ControlConnectionTest extends CCMTestsSupport {

  static final Logger logger = LoggerFactory.getLogger(ControlConnectionTest.class);

  @Test(groups = "short")
  @CCMConfig(numberOfNodes = 2)
  public void should_prevent_simultaneous_reconnection_attempts() throws InterruptedException {

    // Custom load balancing policy that counts the number of calls to newQueryPlan().
    // Since we don't open any session from our Cluster, only the control connection reattempts are
    // calling this
    // method, therefore the invocation count is equal to the number of attempts.
    QueryPlanCountingPolicy loadBalancingPolicy =
        new QueryPlanCountingPolicy(Policies.defaultLoadBalancingPolicy());
    AtomicInteger reconnectionAttempts = loadBalancingPolicy.counter;

    // Custom reconnection policy with a very large delay (longer than the test duration), to make
    // sure we count
    // only the first reconnection attempt of each reconnection handler.
    ReconnectionPolicy reconnectionPolicy = new ConstantReconnectionPolicy(60 * 1000);

    // We pass only the first host as contact point, so we know the control connection will be on
    // this host
    Cluster cluster =
        register(
            createClusterBuilder()
                .withReconnectionPolicy(reconnectionPolicy)
                .withLoadBalancingPolicy(loadBalancingPolicy)
                .build());
    cluster.init();

    // Kill the control connection host, there should be exactly one reconnection attempt
    ccm().stop(1);
    TimeUnit.SECONDS.sleep(
        1); // Sleep for a while to make sure our final count is not the result of lucky timing
    assertThat(reconnectionAttempts.get()).isEqualTo(1);

    ccm().stop(2);
    TimeUnit.SECONDS.sleep(1);
    assertThat(reconnectionAttempts.get()).isEqualTo(2);
  }

  /**
   * Test for JAVA-509: UDT definitions were not properly parsed when using the default protocol
   * version.
   *
   * <p>This did not appear with other tests because the UDT needs to already exist when the driver
   * initializes. Therefore we use two different driver instances in this test.
   */
  @Test(groups = "short")
  @CassandraVersion("2.1.0")
  public void should_parse_UDT_definitions_when_using_default_protocol_version() {
    // First driver instance: create UDT
    Cluster cluster = register(createClusterBuilder().build());
    Session session = cluster.connect();
    session.execute(
        "create keyspace ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
    session.execute("create type ks.foo (i int)");
    cluster.close();

    // Second driver instance: read UDT definition
    Cluster cluster2 = register(createClusterBuilder().build());
    UserType fooType = cluster2.getMetadata().getKeyspace("ks").getUserType("foo");

    assertThat(fooType.getFieldNames()).containsExactly("i");
  }

  /**
   * Ensures that if the host that the Control Connection is connected to is removed/decommissioned
   * that the Control Connection is reestablished to another host.
   *
   * @jira_ticket JAVA-597
   * @expected_result Control Connection is reestablished to another host.
   * @test_category control_connection
   * @since 2.0.9
   */
  @Test(groups = "long")
  @CCMConfig(numberOfNodes = 3)
  public void should_reestablish_if_control_node_decommissioned() throws InterruptedException {
    InetSocketAddress firstHost = ccm().addressOfNode(1);
    Cluster cluster = register(createClusterBuilderNoDebouncing().build());
    cluster.init();

    // Ensure the control connection host is that of the first node.
    InetSocketAddress controlHost =
        cluster.manager.controlConnection.connectedHost().getEndPoint().resolve();
    assertThat(controlHost).isEqualTo(firstHost);

    // Decommission the node.
    ccm().decommission(1);

    // Ensure that the new control connection is not null and it's host is not equal to the
    // decommissioned node.
    Host newHost = cluster.manager.controlConnection.connectedHost();
    assertThat(newHost).isNotNull();
    assertThat(newHost.getAddress()).isNotEqualTo(controlHost);
  }

  /**
   * Ensures that contact points are randomized when determining the initial control connection by
   * default. Initializes a cluster with 5 contact points 100 times and ensures that all 5 were
   * used.
   *
   * @jira_ticket JAVA-618
   * @expected_result All 5 hosts were chosen within 100 attempts. There is a very small possibility
   *     that this may not be the case and this is not actually an error.
   * @test_category control_connection
   * @since 2.0.11, 2.1.8, 2.2.0
   */
  @Test(groups = "short")
  @CCMConfig(createCcm = false)
  public void should_randomize_contact_points_when_determining_control_connection() {
    int hostCount = 5;
    int iterations = 100;
    ScassandraCluster scassandras = ScassandraCluster.builder().withNodes(hostCount).build();
    scassandras.init();

    try {
      Collection<InetAddress> contactPoints = newArrayList();
      for (int i = 1; i <= hostCount; i++) {
        contactPoints.add(scassandras.address(i).getAddress());
      }
      final HashMultiset<InetAddress> occurrencesByHost = HashMultiset.create(hostCount);
      for (int i = 0; i < iterations; i++) {
        Cluster cluster =
            Cluster.builder()
                .addContactPoints(contactPoints)
                .withPort(scassandras.getBinaryPort())
                .withNettyOptions(nonQuietClusterCloseOptions)
                .build();

        try {
          cluster.init();
          occurrencesByHost.add(
              cluster
                  .manager
                  .controlConnection
                  .connectedHost()
                  .getEndPoint()
                  .resolve()
                  .getAddress());
        } finally {
          cluster.close();
        }
      }
      if (logger.isDebugEnabled()) {
        Map<InetAddress, Integer> hostCounts =
            Maps.toMap(
                occurrencesByHost.elementSet(),
                new Function<InetAddress, Integer>() {
                  @Override
                  public Integer apply(InetAddress input) {
                    return occurrencesByHost.count(input);
                  }
                });
        logger.debug("Control Connection Use Counts by Host: {}", hostCounts);
      }
      // There is an incredibly low chance that a host may not be used based on randomness.
      // This probability is very low however.
      assertThat(occurrencesByHost.elementSet().size())
          .as(
              "Not all hosts were used as contact points.  There is a very small chance"
                  + " of this happening based on randomness, investigate whether or not this"
                  + " is a bug.")
          .isEqualTo(hostCount);
    } finally {
      scassandras.stop();
    }
  }

  /**
   * @return Configurations of columns that are missing, whether or not the peers_v2 table should be
   *     present and whether or not an extended peer check is required to fail validation.
   */
  @DataProvider
  public static Object[][] disallowedNullColumnsInPeerData() {
    return new Object[][] {
      {"host_id", false, false}, // JAVA-2171: host_id does not require extended peer check anymore
      {"data_center", false, true},
      {"rack", false, true},
      {"tokens", false, true},
      {"data_center,rack,tokens", false, true},
      {"rpc_address", false, false},
      {"host_id", true, false},
      {"data_center", true, true},
      {"rack", true, true},
      {"tokens", true, true},
      {"data_center,rack,tokens", true, true},
      {"native_address", true, false},
      {"native_port", true, false},
      {"native_address,native_port", true, false},
    };
  }

  /**
   * Validates that if the com.datastax.driver.EXTENDED_PEER_CHECK system property is set to true
   * that a peer with null values for host_id, data_center, rack, or tokens is ignored.
   *
   * @test_category host:metadata
   * @jira_ticket JAVA-852
   * @since 2.1.10
   */
  @Test(groups = "isolated", dataProvider = "disallowedNullColumnsInPeerData")
  @CCMConfig(createCcm = false)
  public void should_ignore_peer_if_extended_peer_check_is_enabled(
      String columns,
      boolean withPeersV2,
      @SuppressWarnings("unused") boolean extendPeerCheckRequired) {
    System.setProperty("com.datastax.driver.EXTENDED_PEER_CHECK", "true");
    run_with_null_peer_info(columns, false, withPeersV2);
  }

  /**
   * Validates that a peer with null values for host_id, data_center, rack, or tokens is ignored.
   *
   * @test_category host:metadata
   * @jira_ticket JAVA-852
   * @since 2.1.10
   */
  @Test(groups = "short", dataProvider = "disallowedNullColumnsInPeerData")
  @CCMConfig(createCcm = false)
  public void should_ignore_and_warn_peers_with_null_entries_by_default(
      String columns,
      boolean withPeersV2,
      @SuppressWarnings("unused") boolean extendedPeerCheckRequired) {
    run_with_null_peer_info(columns, false, withPeersV2);
  }

  static void run_with_null_peer_info(String columns, boolean expectPeer2, boolean withPeersV2) {
    // given: A cluster with peer 2 having a null rack.
    ScassandraCluster.ScassandraClusterBuilder builder = ScassandraCluster.builder().withNodes(3);

    if (withPeersV2) {
      builder.withPeersV2(true);
    }

    StringBuilder columnDataBuilder = new StringBuilder();
    for (String column : columns.split(",")) {
      builder = builder.forcePeerInfo(1, 2, column, null);
      columnDataBuilder.append(String.format("%s=null, ", column));
    }

    String columnData = columnDataBuilder.toString();
    if (columnData.endsWith(", ")) {
      columnData = columnData.substring(0, columnData.length() - 2);
    }

    ScassandraCluster scassandraCluster = builder.build();

    Cluster cluster =
        Cluster.builder()
            .addContactPoints(scassandraCluster.address(1).getAddress())
            .withPort(scassandraCluster.getBinaryPort())
            .withNettyOptions(nonQuietClusterCloseOptions)
            .build();

    // Capture logs to ensure appropriate warnings are logged.
    org.apache.log4j.Logger cLogger = org.apache.log4j.Logger.getLogger("com.datastax.driver.core");
    Level originalLevel = cLogger.getLevel();
    if (originalLevel != null && !originalLevel.isGreaterOrEqual(Level.WARN)) {
      cLogger.setLevel(Level.WARN);
    }
    MemoryAppender logs = new MemoryAppender();
    cLogger.addAppender(logs);

    try {
      scassandraCluster.init();

      // when: Initializing a cluster instance and grabbing metadata.
      cluster.init();

      InetAddress node2Address = scassandraCluster.address(2).getAddress();
      String invalidValues =
          withPeersV2
              ? columnData
              : String.format(
                  "missing native_transport_address, missing native_transport_port, missing native_transport_port_ssl, %s",
                  columnData);
      String expectedError =
          String.format(
              "Found invalid row in system.peers: [peer=%s, %s]. "
                  + "This is likely a gossip or snitch issue, this host will be ignored.",
              node2Address, invalidValues);
      String log = logs.get();
      // then: A peer with a null rack should not show up in host metadata, unless allowed via
      // system property.
      if (expectPeer2) {
        assertThat(cluster.getMetadata().getAllHosts())
            .hasSize(3)
            .extractingResultOf("getAddress")
            .contains(node2Address);

        assertThat(log).doesNotContain(expectedError);
      } else {
        assertThat(cluster.getMetadata().getAllHosts())
            .hasSize(2)
            .extractingResultOf("getAddress")
            .doesNotContain(node2Address);

        assertThat(log).containsOnlyOnce(expectedError);
      }
    } finally {
      cLogger.removeAppender(logs);
      cLogger.setLevel(originalLevel);
      cluster.close();
      scassandraCluster.stop();
    }
  }

  /**
   * Ensures that when a node changes its broadcast address (for example, after a shutdown and
   * startup on EC2 and its public IP has changed), the driver will be able to detect that change
   * and recognize the host in the system.peers table in spite of that change.
   *
   * @jira_ticket JAVA-1038
   * @expected_result The driver should be able to detect that a host has changed its broadcast
   *     address and update its metadata accordingly.
   * @test_category control_connection
   * @since 2.1.10
   */
  @SuppressWarnings("unchecked")
  @Test(groups = "short")
  @CCMConfig(createCcm = false)
  public void should_fetch_whole_peers_table_if_broadcast_address_changed()
      throws UnknownHostException {
    ScassandraCluster scassandras = ScassandraCluster.builder().withNodes(2).build();
    scassandras.init();

    InetSocketAddress node2RpcAddress = scassandras.address(2);

    Cluster cluster =
        Cluster.builder()
            .addContactPoints(scassandras.address(1).getAddress())
            .withPort(scassandras.getBinaryPort())
            .withNettyOptions(nonQuietClusterCloseOptions)
            .build();

    try {

      cluster.init();

      Host host2 = cluster.getMetadata().getHost(node2RpcAddress);
      assertThat(host2).isNotNull();

      InetSocketAddress node2OldBroadcastAddress = host2.getBroadcastSocketAddress();
      InetSocketAddress node2NewBroadcastAddress =
          new InetSocketAddress(InetAddress.getByName("1.2.3.4"), scassandras.getBinaryPort());

      // host 2 has the old broadcast_address (which is identical to its rpc_broadcast_address)
      assertThat(host2.getEndPoint().resolve().getAddress())
          .isEqualTo(node2OldBroadcastAddress.getAddress());

      // simulate a change in host 2 public IP
      Map<String, ?> rows =
          ImmutableMap.<String, Object>builder()
              .put(
                  "peer", node2NewBroadcastAddress.getAddress()) // new broadcast address for host 2
              .put(
                  "rpc_address",
                  host2
                      .getEndPoint()
                      .resolve()
                      .getAddress()) // rpc_broadcast_address remains unchanged
              .put("host_id", host2.getHostId())
              .put("data_center", datacenter(1))
              .put("rack", "rack1")
              .put("release_version", "2.1.8")
              .put("tokens", ImmutableSet.of(Long.toString(scassandras.getTokensForDC(1).get(1))))
              .build();

      scassandras.node(1).primingClient().clearAllPrimes();

      // the driver will attempt to locate host2 in system.peers by its old broadcast address, and
      // that will fail
      scassandras
          .node(1)
          .primingClient()
          .prime(
              PrimingRequest.queryBuilder()
                  .withQuery(
                      "SELECT * FROM system.peers WHERE peer='"
                          + node2OldBroadcastAddress.getAddress().getHostAddress()
                          + "'")
                  .withThen(then().withColumnTypes(SELECT_PEERS).build())
                  .build());

      // the driver will then attempt to fetch the whole system.peers
      scassandras
          .node(1)
          .primingClient()
          .prime(
              PrimingRequest.queryBuilder()
                  .withQuery("SELECT * FROM system.peers")
                  .withThen(then().withColumnTypes(SELECT_PEERS).withRows(rows).build())
                  .build());

      assertThat(cluster.manager.controlConnection.refreshNodeInfo(host2)).isTrue();

      host2 = cluster.getMetadata().getHost(node2RpcAddress);

      // host2 should now have a new broadcast address
      assertThat(host2).isNotNull();
      assertThat(host2.getBroadcastSocketAddress().getAddress())
          .isEqualTo(node2NewBroadcastAddress.getAddress());

      // host 2 should keep its old rpc broadcast address
      assertThat(host2.getEndPoint().resolve()).isEqualTo(node2RpcAddress);

    } finally {
      cluster.close();
      scassandras.stop();
    }
  }

  /**
   * Ensures that multiple C* nodes can share the same ip address (but use different port) if they
   * support the system.peers_v2 table.
   *
   * @jira_ticket JAVA-1388
   * @since 3.6.0
   */
  @Test(groups = "short", dataProviderClass = DataProviders.class, dataProvider = "bool")
  @CCMConfig(createCcm = false)
  public void should_use_port_from_peers_v2_table(boolean sharedIP) {
    ScassandraCluster sCluster =
        ScassandraCluster.builder().withNodes(5).withPeersV2(true).withSharedIP(sharedIP).build();

    Cluster.Builder builder =
        Cluster.builder()
            .addContactPointsWithPorts(sCluster.address(1))
            .withNettyOptions(nonQuietClusterCloseOptions);

    // need to specify port in non peers_v2 case as driver can't infer ports without it.
    if (!sharedIP) {
      builder.withPort(sCluster.getBinaryPort());
    }

    Cluster cluster = builder.build();

    try {
      sCluster.init();
      cluster.connect();
      assertThat(cluster.getMetadata().getAllHosts()).hasSize(5);

      Set<InetAddress> uniqueAddresses = new HashSet<InetAddress>();
      Set<InetSocketAddress> uniqueSocketAddresses = new HashSet<InetSocketAddress>();
      for (int i = 1; i <= 5; i++) {
        Host host = sCluster.host(cluster, 1, i);

        // host is up and broadcast address matches what was configured.
        assertThat(host)
            .isNotNull()
            .isUp()
            .hasSocketAddress(sCluster.address(i))
            .hasBroadcastSocketAddress(sCluster.listenAddress(i));

        // host should only have listen address if it is control connection, and the
        // address should match what was configured.
        if (i == 1) {
          assertThat(host).hasListenSocketAddress(sCluster.listenAddress(i));
        } else {
          assertThat(host).hasNoListenSocketAddress();
        }
        uniqueAddresses.add(host.getEndPoint().resolve().getAddress());
        uniqueSocketAddresses.add(host.getEndPoint().resolve());
      }

      if (!sharedIP) {
        // each host should have its own address
        assertThat(uniqueAddresses).hasSize(5);
      } else {
        // all hosts share the same ip...
        assertThat(uniqueAddresses).hasSize(1);
        // but have a unique port.
        assertThat(uniqueSocketAddresses).hasSize(5);
      }
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Ensures that if cluster does not have the system.peers_v2 table that cluster initialization
   * still succeeds.
   *
   * @jira_ticket JAVA-1388
   * @since 3.6.0
   */
  @Test(groups = "short")
  @CCMConfig(createCcm = false)
  public void should_connect_when_peers_v2_table_not_present() {
    ScassandraCluster sCluster =
        ScassandraCluster.builder().withNodes(5).withPeersV2(false).build();

    Cluster cluster =
        Cluster.builder()
            .addContactPointsWithPorts(sCluster.address(1))
            .withNettyOptions(nonQuietClusterCloseOptions)
            .withPort(sCluster.getBinaryPort())
            .build();

    try {
      sCluster.init();
      cluster.connect();

      assertThat(cluster.getMetadata().getAllHosts()).hasSize(5);
    } finally {
      cluster.close();
      sCluster.stop();
    }
  }

  /**
   * Cassandra 4.0 supports native_address and native_port columns in system.peers_v2. We want to
   * validate our ability to build correct metadata when drawing data from these tables.
   */
  @Test(groups = "short")
  @CCMConfig(createCcm = false)
  public void should_extract_hosts_using_native_address_port_from_peersv2()
      throws UnknownHostException {

    InetAddress expectedAddress = InetAddress.getByName("4.3.2.1");
    int expectedPort = 2409;
    PeerRowState state =
        PeerRowState.builder()
            .peersV2("native_address", expectedAddress)
            .peersV2("native_port", expectedPort)
            .expectedAddress(expectedAddress)
            .expectedPort(expectedPort)
            .build();
    runPeerTest(state);
  }

  /** DSE 6.8 includes native_transport_address and native_transport_port in system.peers. */
  @Test(groups = "short")
  @CCMConfig(createCcm = false)
  public void should_extract_hosts_using_native_transport_address_port_from_peers()
      throws UnknownHostException {

    InetAddress expectedAddress = InetAddress.getByName("4.3.2.1");
    int expectedPort = 2409;
    PeerRowState state =
        PeerRowState.builder()
            .peers("native_transport_address", expectedAddress)
            .peers("native_transport_port", expectedPort)
            .expectedAddress(expectedAddress)
            .expectedPort(expectedPort)
            .build();
    runPeerTest(state);
  }

  /**
   * If both native_transport_port and native_transport_port_ssl are present we expect the latter to
   * be selected if the Cluster is created with SSL support (i.e. if {@link
   * Cluster.Builder#withSSL()} is used).
   */
  @Test(groups = "short", enabled = false /* Requires SSL support in scassandra */)
  @CCMConfig(createCcm = false)
  public void should_extract_hosts_using_native_transport_address_port_ssl_from_peers_dse()
      throws UnknownHostException {

    InetAddress expectedAddress = InetAddress.getByName("4.3.2.1");
    int expectedPort = 2409;
    PeerRowState state =
        PeerRowState.builder()
            .peers("native_transport_address", expectedAddress)
            .peers("native_transport_port", expectedPort - 100)
            .peers("native_transport_port_ssl", expectedPort)
            .expectedAddress(expectedAddress)
            .expectedPort(expectedPort)
            .build();
    runPeerTest(state);
  }

  /**
   * As of CASSANDRA-16999 the relevant column names for OSS Cassandra are slightly different so
   * test those as well.
   */
  @Test(groups = "short", enabled = false /* Requires SSL support in scassandra */)
  @CCMConfig(createCcm = false)
  public void should_extract_hosts_using_native_transport_address_port_ssl_from_peers_cassandra()
      throws UnknownHostException {

    InetAddress expectedAddress = InetAddress.getByName("4.3.2.1");
    int expectedPort = 2409;
    PeerRowState state =
        PeerRowState.builder()
            .peers("native_address", expectedAddress)
            .peers("native_port", expectedPort - 100)
            .peers("native_port_ssl", expectedPort)
            .expectedAddress(expectedAddress)
            .expectedPort(expectedPort)
            .build();
    runPeerTest(state);
  }

  /**
   * The default case. If we can't get native_address/port out of system.peers_v2 or
   * native_transport_address/port out of system.peers the fall back to rpc_address + a default port
   */
  @Test(groups = "short")
  @CCMConfig(createCcm = false)
  public void should_extract_hosts_using_rpc_address_from_peers() throws UnknownHostException {

    InetAddress expectedAddress = InetAddress.getByName("4.3.2.1");
    PeerRowState state =
        PeerRowState.builder()
            .peers("rpc_address", expectedAddress)
            /* DefaultEndPointFactory isn't happy if we don't have a value for
             * both peer and rpc_address */
            .peers("peer", InetAddress.getByName("1.2.3.4"))
            .expectedAddress(expectedAddress)
            .build();
    runPeerTest(state);
  }

  private void runPeerTest(PeerRowState state) {

    ScassandraCluster scassandras =
        ScassandraCluster.builder().withNodes(2).withPeersV2(state.usePeersV2()).build();
    scassandras.init();

    Cluster cluster = null;
    try {

      scassandras.node(1).primingClient().clearAllPrimes();

      PrimingClient primingClient = scassandras.node(1).primingClient();

      /* Note that we always prime system.local; ControlConnection.refreshNodeAndTokenMap() gets angry
       * if this is empty */
      primingClient.prime(
          PrimingRequest.queryBuilder()
              .withQuery("SELECT * FROM system.local WHERE key='local'")
              .withThen(then().withColumnTypes(SELECT_LOCAL).withRows(state.getLocalRow()).build())
              .build());

      if (state.shouldPrimePeers()) {

        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.peers")
                .withThen(
                    then()
                        .withColumnTypes(state.isDse68() ? SELECT_PEERS_DSE68 : SELECT_PEERS)
                        .withRows(state.getPeersRow())
                        .build())
                .build());
      }
      if (state.shouldPrimePeersV2()) {

        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.peers_v2")
                .withThen(
                    then().withColumnTypes(SELECT_PEERS_V2).withRows(state.getPeersV2Row()).build())
                .build());
      } else {

        /* Must return an error code in this case in order to trigger the driver's downgrade to system.peers */
        primingClient.prime(
            PrimingRequest.queryBuilder()
                .withQuery("SELECT * FROM system.peers_v2")
                .withThen(then().withResult(Result.invalid).build()));
      }

      cluster =
          Cluster.builder()
              .addContactPoints(scassandras.address(1).getAddress())
              .withPort(scassandras.getBinaryPort())
              .withNettyOptions(nonQuietClusterCloseOptions)
              .build();
      cluster.connect();

      Collection<EndPoint> hostEndPoints =
          Collections2.transform(
              cluster.getMetadata().allHosts(),
              new Function<Host, EndPoint>() {
                public EndPoint apply(Host host) {
                  return host.getEndPoint();
                }
              });
      assertThat(hostEndPoints).contains(state.getExpectedEndPoint(scassandras));
    } finally {
      if (cluster != null) cluster.close();
      scassandras.stop();
    }
  }

  static class PeerRowState {

    private final ImmutableMap<String, Object> peers;
    private final ImmutableMap<String, Object> peersV2;
    private final ImmutableMap<String, Object> local;

    private final InetAddress expectedAddress;
    private final Optional<Integer> expectedPort;

    private final boolean shouldPrimePeers;
    private final boolean shouldPrimePeersV2;

    private PeerRowState(
        ImmutableMap<String, Object> peers,
        ImmutableMap<String, Object> peersV2,
        ImmutableMap<String, Object> local,
        InetAddress expectedAddress,
        Optional<Integer> expectedPort,
        boolean shouldPrimePeers,
        boolean shouldPrimePeersV2) {
      this.peers = peers;
      this.peersV2 = peersV2;
      this.local = local;

      this.expectedAddress = expectedAddress;
      this.expectedPort = expectedPort;

      this.shouldPrimePeers = shouldPrimePeers;
      this.shouldPrimePeersV2 = shouldPrimePeersV2;
    }

    public static Builder builder() {
      return new Builder();
    }

    public boolean usePeersV2() {
      return !this.peersV2.isEmpty();
    }

    public boolean isDse68() {
      return this.peers.containsKey("native_transport_address")
          || this.peers.containsKey("native_transport_port")
          || this.peers.containsKey("native_transport_port_ssl");
    }

    public boolean shouldPrimePeers() {
      return this.shouldPrimePeers;
    }

    public boolean shouldPrimePeersV2() {
      return this.shouldPrimePeersV2;
    }

    public ImmutableMap<String, Object> getPeersRow() {
      return this.peers;
    }

    public ImmutableMap<String, Object> getPeersV2Row() {
      return this.peersV2;
    }

    public ImmutableMap<String, Object> getLocalRow() {
      return this.local;
    }

    public EndPoint getExpectedEndPoint(ScassandraCluster cluster) {
      return new TranslatedAddressEndPoint(
          new InetSocketAddress(
              this.expectedAddress, this.expectedPort.or(cluster.getBinaryPort())));
    }

    static class Builder {

      private ImmutableMap.Builder<String, Object> peers = this.basePeerRow();
      private ImmutableMap.Builder<String, Object> peersV2 = this.basePeerRow();
      private ImmutableMap.Builder<String, Object> local = this.basePeerRow();

      private InetAddress expectedAddress;
      private Optional<Integer> expectedPort = Optional.absent();

      private boolean shouldPrimePeers = false;
      private boolean shouldPrimePeersV2 = false;

      public PeerRowState build() {
        return new PeerRowState(
            this.peers.build(),
            this.peersV2.build(),
            this.local.build(),
            this.expectedAddress,
            this.expectedPort,
            this.shouldPrimePeers,
            this.shouldPrimePeersV2);
      }

      public Builder peers(String name, Object val) {
        this.peers.put(name, val);
        this.shouldPrimePeers = true;
        return this;
      }

      public Builder peersV2(String name, Object val) {
        this.peersV2.put(name, val);
        this.shouldPrimePeersV2 = true;
        return this;
      }

      public Builder local(String name, Object val) {
        this.local.put(name, val);
        return this;
      }

      public Builder expectedAddress(InetAddress address) {
        this.expectedAddress = address;
        return this;
      }

      public Builder expectedPort(int port) {
        this.expectedPort = Optional.of(port);
        return this;
      }

      private ImmutableMap.Builder<String, Object> basePeerRow() {
        return ImmutableMap.<String, Object>builder()
            /* Required to support Metadata.addIfAbsent(Host) which is used by host loading code */
            .put("host_id", UUID.randomUUID())
            /* Elements below required to pass peer row validation */
            .put("data_center", datacenter(1))
            .put("rack", "rack1")
            .put("tokens", ImmutableSet.of(Long.toString(new Random().nextLong())));
      }
    }
  }

  static class QueryPlanCountingPolicy extends DelegatingLoadBalancingPolicy {

    final AtomicInteger counter = new AtomicInteger();

    public QueryPlanCountingPolicy(LoadBalancingPolicy delegate) {
      super(delegate);
    }

    @Override
    public Iterator<Host> newQueryPlan(String loggedKeyspace, Statement statement) {
      counter.incrementAndGet();
      return super.newQueryPlan(loggedKeyspace, statement);
    }
  }
}
