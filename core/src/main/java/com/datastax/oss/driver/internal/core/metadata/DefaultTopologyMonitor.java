/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default topology monitor, based on {@link ControlConnection}.
 *
 * <p>Note that event processing is implemented directly in the control connection, not here.
 */
public class DefaultTopologyMonitor implements TopologyMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTopologyMonitor.class);

  // Assume topology queries never need paging
  private static final int INFINITE_PAGE_SIZE = -1;

  private final ControlConnection controlConnection;
  private final AddressTranslator addressTranslator;
  private final Duration timeout;

  @VisibleForTesting volatile int port = -1;

  public DefaultTopologyMonitor(InternalDriverContext context) {
    this.controlConnection = context.controlConnection();
    addressTranslator = context.addressTranslator();
    DriverConfigProfile config = context.config().defaultProfile();
    this.timeout = config.getDuration(CoreDriverOption.CONTROL_CONNECTION_TIMEOUT);
  }

  @Override
  public CompletionStage<Void> init() {
    return controlConnection.init(true);
  }

  @Override
  public CompletionStage<Optional<NodeInfo>> refreshNode(Node node) {
    LOG.debug("Refreshing info for {}", node);
    DriverChannel channel = controlConnection.channel();
    if (node.getConnectAddress().equals(channel.address())) {
      // refreshNode is called for nodes that just came up. If the control node just came up, it
      // means the control connection just reconnected, which means we did a full node refresh. So
      // we don't need to process this call.
      LOG.debug("Ignoring refresh of control node");
      return CompletableFuture.completedFuture(Optional.empty());
    } else if (node.getBroadcastAddress().isPresent()) {
      return query(
              channel,
              "SELECT * FROM system.peers WHERE peer = :address",
              ImmutableMap.of("address", node.getBroadcastAddress().get()))
          .thenApply(this::buildNodeInfoFromFirstRow);
    } else {
      return query(channel, "SELECT * FROM system.peers")
          .thenApply(result -> this.findInPeers(result, node.getConnectAddress()));
    }
  }

  @Override
  public CompletionStage<Optional<NodeInfo>> getNewNodeInfo(InetSocketAddress connectAddress) {
    LOG.debug("Fetching info for new node {}", connectAddress);
    DriverChannel channel = controlConnection.channel();
    return query(channel, "SELECT * FROM system.peers")
        .thenApply(result -> this.findInPeers(result, connectAddress));
  }

  @Override
  public CompletionStage<Iterable<NodeInfo>> refreshNodeList() {
    LOG.debug("Refreshing node list");
    DriverChannel channel = controlConnection.channel();
    savePort(channel);

    CompletionStage<AdminResult> localQuery = query(channel, "SELECT * FROM system.local");
    CompletionStage<AdminResult> peersQuery = query(channel, "SELECT * FROM system.peers");

    return localQuery.thenCombine(
        peersQuery,
        (controlNodeResult, peersResult) -> {
          List<NodeInfo> nodeInfos = new ArrayList<>();
          nodeInfos.add(buildNodeInfo(controlNodeResult.iterator().next()));
          for (AdminResult.Row row : peersResult) {
            nodeInfos.add(buildNodeInfo(row));
          }
          return nodeInfos;
        });
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> query(
      DriverChannel channel, String queryString, Map<String, Object> parameters) {
    return AdminRequestHandler.query(channel, queryString, parameters, timeout, INFINITE_PAGE_SIZE)
        .start();
  }

  private CompletionStage<AdminResult> query(DriverChannel channel, String queryString) {
    return query(channel, queryString, Collections.emptyMap());
  }

  private NodeInfo buildNodeInfo(AdminResult.Row row) {
    InetAddress broadcastRpcAddress = row.getInetAddress("rpc_address");
    InetSocketAddress connectAddress =
        addressTranslator.translate(new InetSocketAddress(broadcastRpcAddress, port));
    return buildNodeInfo(row, connectAddress);
  }

  private NodeInfo buildNodeInfo(AdminResult.Row row, InetSocketAddress connectAddress) {
    DefaultNodeInfo.Builder builder = DefaultNodeInfo.builder().withConnectAddress(connectAddress);

    InetAddress broadcastAddress = row.getInetAddress("broadcast_address"); // in system.local
    if (broadcastAddress == null) {
      broadcastAddress = row.getInetAddress("peer"); // in system.peers
    }
    builder.withBroadcastAddress(broadcastAddress);

    builder.withListenAddress(row.getInetAddress("listen_address"));
    builder.withDatacenter(row.getString("data_center"));
    builder.withRack(row.getString("rack"));
    builder.withCassandraVersion(row.getString("release_version"));
    builder.withTokens(row.getSetOfString("tokens"));

    return builder.build();
  }

  private Optional<NodeInfo> buildNodeInfoFromFirstRow(AdminResult result) {
    Iterator<AdminResult.Row> iterator = result.iterator();
    if (iterator.hasNext()) {
      return Optional.of(buildNodeInfo(iterator.next()));
    } else {
      return Optional.empty();
    }
  }

  private Optional<NodeInfo> findInPeers(AdminResult result, InetSocketAddress connectAddress) {
    // The peers table is keyed by broadcast_address, but we only have the translated
    // broadcast_rpc_address, so we have to traverse the whole table and check the rows one by one.
    for (AdminResult.Row row : result) {
      InetAddress broadcastRpcAddress = row.getInetAddress("rpc_address");
      if (broadcastRpcAddress != null
          && addressTranslator
              .translate(new InetSocketAddress(broadcastRpcAddress, port))
              .equals(connectAddress)) {
        return Optional.of(buildNodeInfo(row, connectAddress));
      }
    }
    LOG.debug("Could not find any peer row matching {}", connectAddress);
    return Optional.empty();
  }

  // Current versions of Cassandra (3.11 at the time of writing), require the same port for all
  // nodes. As a consequence, the port is not stored in system tables.
  // We save it the first time we get a control connection channel.
  private void savePort(DriverChannel channel) {
    if (port < 0 && channel.address() instanceof InetSocketAddress) {
      port = ((InetSocketAddress) channel.address()).getPort();
    }
  }
}
