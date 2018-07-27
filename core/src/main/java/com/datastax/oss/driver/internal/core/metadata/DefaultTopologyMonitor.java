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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.control.ControlConnection;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
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
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The default topology monitor, based on {@link ControlConnection}.
 *
 * <p>Note that event processing is implemented directly in the control connection, not here.
 */
@ThreadSafe
public class DefaultTopologyMonitor implements TopologyMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultTopologyMonitor.class);

  // Assume topology queries never need paging
  private static final int INFINITE_PAGE_SIZE = -1;

  private final String logPrefix;
  private final InternalDriverContext context;
  private final ControlConnection controlConnection;
  private final AddressTranslator addressTranslator;
  private final Duration timeout;
  private final boolean reconnectOnInit;
  private final CompletableFuture<Void> closeFuture;

  @VisibleForTesting volatile boolean isSchemaV2;
  @VisibleForTesting volatile int port = -1;

  public DefaultTopologyMonitor(InternalDriverContext context) {
    this.logPrefix = context.getSessionName();
    this.context = context;
    this.controlConnection = context.getControlConnection();
    this.addressTranslator = context.getAddressTranslator();
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    this.timeout = config.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT);
    this.reconnectOnInit = config.getBoolean(DefaultDriverOption.RECONNECT_ON_INIT);
    this.closeFuture = new CompletableFuture<>();
    // Set this to true initially, after the first refreshNodes is called this will either stay true
    // or be set to false;
    this.isSchemaV2 = true;
  }

  @Override
  public CompletionStage<Void> init() {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    return controlConnection.init(true, reconnectOnInit);
  }

  @Override
  public CompletionStage<Void> initFuture() {
    return controlConnection.initFuture();
  }

  @Override
  public CompletionStage<Optional<NodeInfo>> refreshNode(Node node) {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    LOG.debug("[{}] Refreshing info for {}", logPrefix, node);
    DriverChannel channel = controlConnection.channel();
    if (node.getConnectAddress().equals(channel.remoteAddress())) {
      // refreshNode is called for nodes that just came up. If the control node just came up, it
      // means the control connection just reconnected, which means we did a full node refresh. So
      // we don't need to process this call.
      LOG.debug("[{}] Ignoring refresh of control node", logPrefix);
      return CompletableFuture.completedFuture(Optional.empty());
    } else if (node.getBroadcastAddress().isPresent()) {
      CompletionStage<AdminResult> query;
      if (isSchemaV2) {
        query =
            query(
                channel,
                "SELECT * FROM "
                    + retrievePeerTableName()
                    + " WHERE peer = :address and peer_port = :port",
                ImmutableMap.of(
                    "address",
                    node.getBroadcastAddress().get().getAddress(),
                    "peer",
                    node.getBroadcastAddress().get().getPort()));
      } else {
        query =
            query(
                channel,
                "SELECT * FROM " + retrievePeerTableName() + " WHERE peer = :address",
                ImmutableMap.of("address", node.getBroadcastAddress().get().getAddress()));
      }
      return query.thenApply(this::firstRowAsNodeInfo);
    } else {
      return query(channel, "SELECT * FROM " + retrievePeerTableName())
          .thenApply(result -> this.findInPeers(result, node.getConnectAddress()));
    }
  }

  @Override
  public CompletionStage<Optional<NodeInfo>> getNewNodeInfo(InetSocketAddress connectAddress) {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    LOG.debug("[{}] Fetching info for new node {}", logPrefix, connectAddress);
    DriverChannel channel = controlConnection.channel();
    return query(channel, "SELECT * FROM " + retrievePeerTableName())
        .thenApply(result -> this.findInPeers(result, connectAddress));
  }

  @Override
  public CompletionStage<Iterable<NodeInfo>> refreshNodeList() {
    if (closeFuture.isDone()) {
      return CompletableFutures.failedFuture(new IllegalStateException("closed"));
    }
    LOG.debug("[{}] Refreshing node list", logPrefix);
    DriverChannel channel = controlConnection.channel();

    // This cast always succeeds in production. The only way it could fail is in a test that uses a
    // local channel, and we don't have such tests at the moment.
    InetSocketAddress controlAddress = (InetSocketAddress) channel.remoteAddress();

    savePort(channel);

    CompletionStage<AdminResult> localQuery = query(channel, "SELECT * FROM system.local");
    CompletionStage<AdminResult> peersV2Query = query(channel, "SELECT * FROM system.peers_v2");
    CompletableFuture<AdminResult> peersQuery = new CompletableFuture<>();

    peersV2Query.whenComplete(
        (r, t) -> {
          if (t != null) {
            // The query to system.peers_v2 failed, we should not attempt this query in the
            // future.
            this.isSchemaV2 = false;
            CompletableFutures.completeFrom(
                query(channel, "SELECT * FROM system.peers"), peersQuery);
          } else {
            peersQuery.complete(r);
          }
        });

    return localQuery.thenCombine(
        peersQuery,
        (controlNodeResult, peersResult) -> {
          List<NodeInfo> nodeInfos = new ArrayList<>();
          // Don't rely on system.local.rpc_address for the control row, because it mistakenly
          // reports the normal RPC address instead of the broadcast one (CASSANDRA-11181). We
          // already know the address since we've just used it to query.
          nodeInfos.add(
              nodeInfoBuilder(controlNodeResult.iterator().next(), controlAddress).build());
          for (AdminRow row : peersResult) {
            nodeInfos.add(asNodeInfo(row));
          }
          return nodeInfos;
        });
  }

  @Override
  public CompletionStage<Boolean> checkSchemaAgreement() {
    if (closeFuture.isDone()) {
      return CompletableFuture.completedFuture(true);
    }
    DriverChannel channel = controlConnection.channel();
    return new SchemaAgreementChecker(channel, context, port, logPrefix).run();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    closeFuture.complete(null);
    return closeFuture;
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return closeAsync();
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> query(
      DriverChannel channel, String queryString, Map<String, Object> parameters) {
    return AdminRequestHandler.query(
            channel, queryString, parameters, timeout, INFINITE_PAGE_SIZE, logPrefix)
        .start();
  }

  private CompletionStage<AdminResult> query(DriverChannel channel, String queryString) {
    return query(channel, queryString, Collections.emptyMap());
  }

  private String retrievePeerTableName() {
    if (isSchemaV2) {
      return "system.peers_v2";
    }
    return "system.peers";
  }

  private NodeInfo asNodeInfo(AdminRow row) {
    InetSocketAddress connectAddress = getNativeAddressFromPeers(row);
    if (connectAddress == null) {
      throw new IllegalArgumentException(
          "Missing rpc_address or native in system row, can't refresh node");
    }
    return nodeInfoBuilder(row, connectAddress).build();
  }

  private Optional<NodeInfo> firstRowAsNodeInfo(AdminResult result) {
    Iterator<AdminRow> iterator = result.iterator();
    if (iterator.hasNext()) {
      return Optional.of(asNodeInfo(iterator.next()));
    } else {
      return Optional.empty();
    }
  }

  protected DefaultNodeInfo.Builder nodeInfoBuilder(
      AdminRow row, InetSocketAddress connectAddress) {
    DefaultNodeInfo.Builder builder = DefaultNodeInfo.builder().withConnectAddress(connectAddress);

    InetAddress broadcastAddress = row.getInetAddress("broadcast_address"); // in system.local
    if (broadcastAddress == null) {
      broadcastAddress = row.getInetAddress("peer"); // in system.peers
    }
    int broadcastPort = 0;
    if (row.contains("peer_port")) {
      broadcastPort = row.getInteger("peer_port");
    }
    builder.withBroadcastAddress(new InetSocketAddress(broadcastAddress, broadcastPort));
    InetAddress listenAddress = row.getInetAddress("listen_address");
    int listen_port = 0;
    if (row.contains("listen_port")) {
      listen_port = row.getInteger("listen_port");
    }
    builder.withListenAddress(new InetSocketAddress(listenAddress, listen_port));
    builder.withDatacenter(row.getString("data_center"));
    builder.withRack(row.getString("rack"));
    builder.withCassandraVersion(row.getString("release_version"));
    builder.withTokens(row.getSetOfString("tokens"));
    builder.withPartitioner(row.getString("partitioner"));
    builder.withHostId(row.getUuid("host_id"));
    builder.withSchemaVersion(row.getUuid("schema_version"));
    return builder;
  }

  private Optional<NodeInfo> findInPeers(AdminResult result, InetSocketAddress connectAddress) {
    // The peers table is keyed by broadcast_address, but we only have the translated
    // broadcast_rpc_address, so we have to traverse the whole table and check the rows one by one.
    for (AdminRow row : result) {
      InetSocketAddress nativeAddress = getNativeAddressFromPeers(row);
      if (nativeAddress != null && nativeAddress.equals(connectAddress)) {
        return Optional.of(nodeInfoBuilder(row, nativeAddress).build());
      }
    }
    LOG.debug("[{}] Could not find any peer row matching {}", logPrefix, connectAddress);
    return Optional.empty();
  }

  // Current versions of Cassandra (3.11 at the time of writing), require the same port for all
  // nodes. As a consequence, the port is not stored in system tables.
  // We save it the first time we get a control connection channel.
  private void savePort(DriverChannel channel) {
    if (port < 0 && channel.remoteAddress() instanceof InetSocketAddress) {
      port = ((InetSocketAddress) channel.remoteAddress()).getPort();
    }
  }

  private InetSocketAddress getNativeAddressFromPeers(AdminRow row) {
    InetAddress nativeAddress = row.getInetAddress("native_address");
    if (nativeAddress == null) {
      nativeAddress = row.getInetAddress("rpc_address");
    }
    if (nativeAddress == null) {
      return null;
    }
    int row_port = port;
    return addressTranslator.translate(new InetSocketAddress(nativeAddress, row_port));
  }
}
