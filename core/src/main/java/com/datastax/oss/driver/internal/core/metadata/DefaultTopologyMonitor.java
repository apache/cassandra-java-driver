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
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The default topology monitor, based on the control connection. */
public class DefaultTopologyMonitor implements TopologyMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTopologyMonitor.class);

  // Assume topology queries never need paging
  private static final int INFINITE_PAGE_SIZE = -1;

  private final ControlConnection controlConnection;
  private final AddressTranslator addressTranslator;
  private final Duration timeout;

  private volatile int port = -1;

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
  public CompletionStage<NodeInfo> refreshNode(InetSocketAddress address) {
    return CompletableFutures.failedFuture(new UnsupportedOperationException("TODO"));
  }

  @Override
  public CompletionStage<Iterable<NodeInfo>> refreshNodeList() {
    LOG.debug("Refreshing node list");
    DriverChannel channel = controlConnection.channel();
    savePort(channel);

    CompletionStage<AdminResult> controlNodeStage =
        AdminRequestHandler.query(
                channel, "SELECT * FROM system.local", timeout, INFINITE_PAGE_SIZE)
            .start();
    CompletionStage<AdminResult> peersStage =
        AdminRequestHandler.query(
                channel, "SELECT * FROM system.peers", timeout, INFINITE_PAGE_SIZE)
            .start();

    return controlNodeStage.thenCombine(
        peersStage,
        (controlNodeResult, peersResult) -> {
          List<NodeInfo> nodeInfos = new ArrayList<>();
          nodeInfos.add(buildNodeInfo(controlNodeResult.iterator().next()));
          for (AdminResult.Row row : peersResult) {
            nodeInfos.add(buildNodeInfo(row));
          }
          return nodeInfos;
        });
  }

  private NodeInfo buildNodeInfo(AdminResult.Row row) {
    DefaultNodeInfo.Builder builder = DefaultNodeInfo.builder();

    InetAddress broadcastRpcAddress = row.getInet("rpc_address");
    if (broadcastRpcAddress != null) {
      builder.withConnectAddress(
          addressTranslator.translate(new InetSocketAddress(broadcastRpcAddress, port)));
    }

    InetAddress broadcastAddress = row.getInet("broadcast_address"); // in system.local
    if (broadcastAddress == null) {
      broadcastAddress = row.getInet("peer"); // in system.peers
    }
    builder.withBroadcastAddress(broadcastAddress);

    builder.withListenAddress(row.getInet("listen"));
    builder.withDatacenter(row.getVarchar("data_center"));
    builder.withRack(row.getVarchar("rack"));
    builder.withCassandraVersion(row.getVarchar("release_version"));
    builder.withTokens(row.getSetOfVarchar("tokens"));

    return builder.build();
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
