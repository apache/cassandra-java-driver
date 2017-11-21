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

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class SchemaAgreementChecker {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaAgreementChecker.class);
  private static final int INFINITE_PAGE_SIZE = -1;
  @VisibleForTesting static final InetAddress BIND_ALL_ADDRESS;

  static {
    try {
      BIND_ALL_ADDRESS = InetAddress.getByAddress(new byte[4]);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private final DriverChannel channel;
  private final InternalDriverContext context;
  private final int port;
  private final String logPrefix;
  private final Duration queryTimeout;
  private final long intervalNs;
  private final long timeoutNs;
  private final boolean warnOnFailure;
  private final long start;
  private final CompletableFuture<Boolean> result = new CompletableFuture<>();

  SchemaAgreementChecker(
      DriverChannel channel, InternalDriverContext context, int port, String logPrefix) {
    this.channel = channel;
    this.context = context;
    this.port = port;
    this.logPrefix = logPrefix;
    DriverConfigProfile config = context.config().getDefaultProfile();
    this.queryTimeout = config.getDuration(CoreDriverOption.CONTROL_CONNECTION_TIMEOUT);
    this.intervalNs =
        config.getDuration(CoreDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL).toNanos();
    this.timeoutNs =
        config.getDuration(CoreDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT).toNanos();
    this.warnOnFailure = config.getBoolean(CoreDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN);
    this.start = System.nanoTime();
  }

  public CompletionStage<Boolean> run() {
    LOG.debug("[{}] Checking schema agreement", logPrefix);
    if (timeoutNs == 0) {
      result.complete(false);
    } else {
      sendQueries();
    }
    return result;
  }

  private void sendQueries() {
    long elapsedNs = System.nanoTime() - start;
    if (elapsedNs > timeoutNs) {
      String message =
          String.format(
              "[%s] Schema agreement not reached after %s", logPrefix, NanoTime.format(elapsedNs));
      if (warnOnFailure) {
        LOG.warn(message);
      } else {
        LOG.debug(message);
      }
      result.complete(false);
    } else {
      CompletionStage<AdminResult> localQuery =
          query("SELECT schema_version FROM system.local WHERE key='local'");
      CompletionStage<AdminResult> peersQuery =
          query("SELECT peer, rpc_address, schema_version FROM system.peers");

      localQuery
          .thenCombine(peersQuery, this::extractSchemaVersions)
          .whenComplete(this::completeOrReschedule);
    }
  }

  private Set<UUID> extractSchemaVersions(AdminResult controlNodeResult, AdminResult peersResult) {
    ImmutableSet.Builder<UUID> uuids = ImmutableSet.builder();

    UUID uuid;
    Iterator<AdminRow> iterator = controlNodeResult.iterator();
    if (iterator.hasNext() && (uuid = iterator.next().getUuid("schema_version")) != null) {
      uuids.add(uuid);
    }

    Map<InetSocketAddress, Node> nodes = context.metadataManager().getMetadata().getNodes();
    for (AdminRow peerRow : peersResult) {
      InetSocketAddress connectAddress = getConnectAddress(peerRow);
      Node node = nodes.get(connectAddress);
      if (node == null || node.getState() != NodeState.UP) {
        continue;
      }
      uuid = peerRow.getUuid("schema_version");
      if (uuid != null) {
        uuids.add(uuid);
      }
    }
    return uuids.build();
  }

  private InetSocketAddress getConnectAddress(AdminRow peerRow) {
    // This is actually broadcast_address
    InetAddress broadcastAddress = peerRow.getInetAddress("peer");
    // The address we are looking for (this corresponds to broadcast_rpc_address in the peer's
    // cassandra yaml file; if this setting if unset, it defaults to the value for rpc_address or
    // rpc_interface
    InetAddress rpcAddress = peerRow.getInetAddress("rpc_address");

    if (rpcAddress == null) {
      LOG.warn(
          "[{}] Found corrupted row with null rpc_address in system.peers (peer = {}), "
              + "excluding from schema agreement",
          logPrefix,
          broadcastAddress);
      return null;
    } else if (rpcAddress.equals(BIND_ALL_ADDRESS)) {
      LOG.warn(
          "[{}] Found peer with 0.0.0.0 as rpc_address in system.peers, using peer ({}) instead",
          logPrefix,
          broadcastAddress);
      rpcAddress = broadcastAddress;
    }
    return context.addressTranslator().translate(new InetSocketAddress(rpcAddress, port));
  }

  private void completeOrReschedule(Set<UUID> uuids, Throwable error) {
    if (error != null) {
      LOG.debug(
          "[{}] Error while checking schema agreement, completing now (false)", logPrefix, error);
      result.complete(false);
    } else if (uuids.size() == 1) {
      LOG.debug(
          "[{}] Schema agreement reached ({}), completing", logPrefix, uuids.iterator().next());
      result.complete(true);
    } else {
      LOG.debug(
          "[{}] Schema agreement not reached yet ({}), rescheduling in {}",
          logPrefix,
          uuids,
          NanoTime.format(intervalNs));
      channel
          .eventLoop()
          .schedule(this::sendQueries, intervalNs, TimeUnit.NANOSECONDS)
          .addListener(
              f -> {
                if (!f.isSuccess()) {
                  LOG.debug(
                      "[{}] Error while rescheduling schema agreement, completing now (false)",
                      logPrefix,
                      f.cause());
                }
              });
    }
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> query(String queryString) {
    return AdminRequestHandler.query(
            channel,
            queryString,
            Collections.emptyMap(),
            queryTimeout,
            INFINITE_PAGE_SIZE,
            logPrefix)
        .start();
  }
}
