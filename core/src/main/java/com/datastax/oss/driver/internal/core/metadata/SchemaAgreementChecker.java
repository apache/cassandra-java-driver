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
package com.datastax.oss.driver.internal.core.metadata;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.NodeState;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.util.NanoTime;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
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
  private final String logPrefix;
  private final Duration queryTimeout;
  private final long intervalNs;
  private final long timeoutNs;
  private final boolean warnOnFailure;
  private final long start;
  private final CompletableFuture<Boolean> result = new CompletableFuture<>();

  SchemaAgreementChecker(DriverChannel channel, InternalDriverContext context, String logPrefix) {
    this.channel = channel;
    this.context = context;
    this.logPrefix = logPrefix;
    DriverExecutionProfile config = context.getConfig().getDefaultProfile();
    this.queryTimeout = config.getDuration(DefaultDriverOption.CONTROL_CONNECTION_TIMEOUT);
    this.intervalNs =
        config.getDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_INTERVAL).toNanos();
    this.timeoutNs =
        config.getDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT).toNanos();
    this.warnOnFailure = config.getBoolean(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_WARN);
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
      CompletionStage<AdminResult> peersQuery = query("SELECT * FROM system.peers");

      localQuery
          .thenCombine(peersQuery, this::extractSchemaVersions)
          .whenComplete(this::completeOrReschedule);
    }
  }

  private Set<UUID> extractSchemaVersions(AdminResult controlNodeResult, AdminResult peersResult) {
    // Gather the versions of all the nodes that are UP
    ImmutableSet.Builder<UUID> schemaVersions = ImmutableSet.builder();

    // Control node (implicitly UP, we've just queried it)
    Iterator<AdminRow> iterator = controlNodeResult.iterator();
    if (iterator.hasNext()) {
      AdminRow localRow = iterator.next();
      UUID schemaVersion = localRow.getUuid("schema_version");
      if (schemaVersion == null) {
        LOG.warn(
            "[{}] Missing schema_version for control node {}, "
                + "excluding from schema agreement check",
            logPrefix,
            channel.getEndPoint());
      } else {
        schemaVersions.add(schemaVersion);
      }
    } else {
      LOG.warn(
          "[{}] Missing system.local row for control node {}, "
              + "excluding from schema agreement check",
          logPrefix,
          channel.getEndPoint());
    }

    Map<UUID, Node> nodes = context.getMetadataManager().getMetadata().getNodes();
    for (AdminRow peerRow : peersResult) {
      if (isPeerValid(peerRow, nodes)) {
        UUID schemaVersion = Objects.requireNonNull(peerRow.getUuid("schema_version"));
        schemaVersions.add(schemaVersion);
      }
    }
    return schemaVersions.build();
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
            channel, queryString, queryTimeout, INFINITE_PAGE_SIZE, logPrefix)
        .start();
  }

  protected boolean isPeerValid(AdminRow peerRow, Map<UUID, Node> nodes) {
    if (PeerRowValidator.isValid(peerRow)) {
      UUID hostId = peerRow.getUuid("host_id");
      Node node = nodes.get(hostId);
      if (node == null) {
        LOG.warn("[{}] Unknown peer {}, excluding from schema agreement check", logPrefix, hostId);
        return false;
      } else if (node.getState() != NodeState.UP) {
        LOG.debug("[{}] Peer {} is down, excluding from schema agreement check", logPrefix, hostId);
        return false;
      }
      return true;
    } else {
      LOG.warn(
          "[{}] Found invalid system.peers row for peer: {}, excluding from schema agreement check.",
          logPrefix,
          peerRow.getInetAddress("peer"));
      return false;
    }
  }
}
