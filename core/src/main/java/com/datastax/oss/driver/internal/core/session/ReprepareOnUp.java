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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.api.core.connection.BusyConnectionException;
import com.datastax.oss.driver.api.core.session.throttling.RequestThrottler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRow;
import com.datastax.oss.driver.internal.core.adminrequest.ThrottledAdminRequestHandler;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandler;
import com.datastax.oss.driver.internal.core.metrics.SessionMetricUpdater;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;
import com.datastax.oss.driver.internal.core.util.concurrent.RunOrSchedule;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.util.Bytes;
import io.netty.util.concurrent.EventExecutor;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import net.jcip.annotations.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures that a newly added or restarted node knows all the prepared statements created from this
 * driver instance.
 *
 * <p>See the comments in {@code reference.conf} for more explanations about this process. If any
 * prepare request fail, we ignore the error because it will be retried on the fly (see {@link
 * CqlRequestHandler}).
 *
 * <p>Logically this code belongs to {@link DefaultSession}, but it was extracted for modularity and
 * testability.
 */
@ThreadSafe
class ReprepareOnUp {

  private static final Logger LOG = LoggerFactory.getLogger(ReprepareOnUp.class);
  private static final Query QUERY_SERVER_IDS =
      new Query("SELECT prepared_id FROM system.prepared_statements");

  private final String logPrefix;
  private final ChannelPool pool;
  private final EventExecutor adminExecutor;
  private final Map<ByteBuffer, RepreparePayload> repreparePayloads;
  private final Runnable whenPrepared;
  private final boolean checkSystemTable;
  private final int maxStatements;
  private final int maxParallelism;
  private final Duration timeout;
  private final RequestThrottler throttler;
  private final SessionMetricUpdater metricUpdater;

  // After the constructor, everything happens on adminExecutor, so these fields do not need any
  // synchronization.
  private Set<ByteBuffer> serverKnownIds;
  private Queue<RepreparePayload> toReprepare;
  private int runningWorkers;

  ReprepareOnUp(
      String logPrefix,
      ChannelPool pool,
      EventExecutor adminExecutor,
      Map<ByteBuffer, RepreparePayload> repreparePayloads,
      InternalDriverContext context,
      Runnable whenPrepared) {

    this.logPrefix = logPrefix;
    this.pool = pool;
    this.adminExecutor = adminExecutor;
    this.repreparePayloads = repreparePayloads;
    this.whenPrepared = whenPrepared;
    this.throttler = context.getRequestThrottler();

    DriverConfig config = context.getConfig();
    this.checkSystemTable =
        config.getDefaultProfile().getBoolean(DefaultDriverOption.REPREPARE_CHECK_SYSTEM_TABLE);
    this.timeout = config.getDefaultProfile().getDuration(DefaultDriverOption.REPREPARE_TIMEOUT);
    this.maxStatements =
        config.getDefaultProfile().getInt(DefaultDriverOption.REPREPARE_MAX_STATEMENTS);
    this.maxParallelism =
        config.getDefaultProfile().getInt(DefaultDriverOption.REPREPARE_MAX_PARALLELISM);

    this.metricUpdater = context.getMetricsFactory().getSessionUpdater();
  }

  void start() {
    if (repreparePayloads.isEmpty()) {
      LOG.debug("[{}] No statements to reprepare, done", logPrefix);
      whenPrepared.run();
    } else {
      // Check log level because ConcurrentMap.size is not a constant operation
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "[{}] {} statements to reprepare on newly added/up node",
            logPrefix,
            repreparePayloads.size());
      }
      if (checkSystemTable) {
        LOG.debug("[{}] Checking which statements the server knows about", logPrefix);
        queryAsync(QUERY_SERVER_IDS, Collections.emptyMap(), "QUERY system.prepared_statements")
            .whenCompleteAsync(this::gatherServerIds, adminExecutor);
      } else {
        LOG.debug(
            "[{}] {} is disabled, repreparing directly",
            logPrefix,
            DefaultDriverOption.REPREPARE_CHECK_SYSTEM_TABLE.getPath());
        RunOrSchedule.on(
            adminExecutor,
            () -> {
              serverKnownIds = Collections.emptySet();
              gatherPayloadsToReprepare();
            });
      }
    }
  }

  private void gatherServerIds(AdminResult rows, Throwable error) {
    assert adminExecutor.inEventLoop();
    if (serverKnownIds == null) {
      serverKnownIds = new HashSet<>();
    }
    if (error != null) {
      LOG.debug(
          "[{}] Error querying system.prepared_statements ({}), proceeding without server ids",
          logPrefix,
          error.toString());
      gatherPayloadsToReprepare();
    } else {
      for (AdminRow row : rows) {
        serverKnownIds.add(row.getByteBuffer("prepared_id"));
      }
      if (rows.hasNextPage()) {
        LOG.debug("[{}] system.prepared_statements has more pages", logPrefix);
        rows.nextPage().whenCompleteAsync(this::gatherServerIds, adminExecutor);
      } else {
        LOG.debug("[{}] Gathered {} server ids, proceeding", logPrefix, serverKnownIds.size());
        gatherPayloadsToReprepare();
      }
    }
  }

  private void gatherPayloadsToReprepare() {
    assert adminExecutor.inEventLoop();
    toReprepare = new ArrayDeque<>();
    for (RepreparePayload payload : repreparePayloads.values()) {
      if (serverKnownIds.contains(payload.id)) {
        LOG.trace(
            "[{}] Skipping statement {} because it is already known to the server",
            logPrefix,
            Bytes.toHexString(payload.id));
      } else {
        if (maxStatements > 0 && toReprepare.size() == maxStatements) {
          LOG.debug(
              "[{}] Limiting number of statements to reprepare to {} as configured, "
                  + "but there are more",
              logPrefix,
              maxStatements);
          break;
        } else {
          toReprepare.add(payload);
        }
      }
    }
    if (toReprepare.isEmpty()) {
      LOG.debug(
          "[{}] No statements to reprepare that are not known by the server already, done",
          logPrefix);
      whenPrepared.run();
    } else {
      startWorkers();
    }
  }

  private void startWorkers() {
    assert adminExecutor.inEventLoop();
    runningWorkers = Math.min(maxParallelism, toReprepare.size());
    LOG.debug(
        "[{}] Repreparing {} statements with {} parallel workers",
        logPrefix,
        toReprepare.size(),
        runningWorkers);
    for (int i = 0; i < runningWorkers; i++) {
      startWorker();
    }
  }

  private void startWorker() {
    assert adminExecutor.inEventLoop();
    if (toReprepare.isEmpty()) {
      runningWorkers -= 1;
      if (runningWorkers == 0) {
        LOG.debug("[{}] All workers finished, done", logPrefix);
        whenPrepared.run();
      }
    } else {
      RepreparePayload payload = toReprepare.poll();
      prepareAsync(
              new Prepare(
                  payload.query, (payload.keyspace == null ? null : payload.keyspace.asInternal())),
              payload.customPayload)
          .handleAsync(
              (result, error) -> {
                // Don't log, AdminRequestHandler does already
                startWorker();
                return null;
              },
              adminExecutor);
    }
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> queryAsync(
      Message message, Map<String, ByteBuffer> customPayload, String debugString) {
    DriverChannel channel = pool.next();
    if (channel == null) {
      return CompletableFutures.failedFuture(
          new BusyConnectionException("Found no channel to execute reprepare query"));
    } else {
      ThrottledAdminRequestHandler<AdminResult> reprepareHandler =
          ThrottledAdminRequestHandler.query(
              channel,
              false,
              message,
              customPayload,
              timeout,
              throttler,
              metricUpdater,
              logPrefix,
              debugString);
      return reprepareHandler.start();
    }
  }

  @VisibleForTesting
  protected CompletionStage<ByteBuffer> prepareAsync(
      Message message, Map<String, ByteBuffer> customPayload) {
    DriverChannel channel = pool.next();
    if (channel == null) {
      return CompletableFutures.failedFuture(
          new BusyConnectionException("Found no channel to execute reprepare query"));
    } else {
      ThrottledAdminRequestHandler<ByteBuffer> reprepareHandler =
          ThrottledAdminRequestHandler.prepare(
              channel, false, message, customPayload, timeout, throttler, metricUpdater, logPrefix);
      return reprepareHandler.start();
    }
  }
}
