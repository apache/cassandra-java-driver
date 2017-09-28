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
package com.datastax.oss.driver.internal.core.session;

import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfig;
import com.datastax.oss.driver.internal.core.adminrequest.AdminRequestHandler;
import com.datastax.oss.driver.internal.core.adminrequest.AdminResult;
import com.datastax.oss.driver.internal.core.channel.DriverChannel;
import com.datastax.oss.driver.internal.core.cql.CqlRequestHandlerBase;
import com.datastax.oss.driver.internal.core.pool.ChannelPool;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ensures that a newly added or restarted node knows all the prepared statements created from this
 * driver instance.
 *
 * <p>See the comments in {@code reference.conf} for more explanations about this process. If any
 * prepare request fail, we ignore the error because it will be retried on the fly (see {@link
 * CqlRequestHandlerBase}).
 *
 * <p>Logically this code belongs to {@link DefaultSession}, but it was extracted for modularity and
 * testability.
 */
class ReprepareOnUp {

  private static final Logger LOG = LoggerFactory.getLogger(ReprepareOnUp.class);
  private static final Query QUERY_SERVER_IDS =
      new Query("SELECT prepared_id FROM system.prepared_statements");

  private final String logPrefix;
  private final DriverChannel channel;
  private final Map<ByteBuffer, RepreparePayload> repreparePayloads;
  private final Runnable whenPrepared;
  private final boolean checkSystemTable;
  private final int maxStatements;
  private final int maxParallelism;
  private final Duration timeout;

  // After the constructor, everything happens on the channel's event loop, so these fields do not
  // need any synchronization.
  private Set<ByteBuffer> serverKnownIds;
  private Queue<RepreparePayload> toReprepare;
  private int runningWorkers;

  ReprepareOnUp(
      String logPrefix,
      ChannelPool pool,
      Map<ByteBuffer, RepreparePayload> repreparePayloads,
      DriverConfig config,
      Runnable whenPrepared) {

    this.logPrefix = logPrefix;
    this.channel = pool.next();
    this.repreparePayloads = repreparePayloads;
    this.whenPrepared = whenPrepared;

    this.checkSystemTable =
        config.getDefaultProfile().getBoolean(CoreDriverOption.REPREPARE_CHECK_SYSTEM_TABLE);
    this.timeout = config.getDefaultProfile().getDuration(CoreDriverOption.REPREPARE_TIMEOUT);
    this.maxStatements =
        config.getDefaultProfile().getInt(CoreDriverOption.REPREPARE_MAX_STATEMENTS);
    this.maxParallelism =
        config.getDefaultProfile().getInt(CoreDriverOption.REPREPARE_MAX_PARALLELISM);
  }

  void start() {
    if (repreparePayloads.isEmpty()) {
      LOG.debug("[{}] No statements to reprepare, done", logPrefix);
      whenPrepared.run();
    } else if (this.channel == null) {
      // Should not happen, but handle cleanly
      LOG.debug("[{}] No channel available to reprepare, done", logPrefix);
      whenPrepared.run();
    } else {
      if (LOG.isDebugEnabled()) { // check because ConcurrentMap.size is not a constant operation
        LOG.debug(
            "[{}] {} statements to reprepare on newly added/up node",
            logPrefix,
            repreparePayloads.size());
      }
      if (checkSystemTable) {
        LOG.debug("[{}] Checking which statements the server knows about", logPrefix);
        queryAsync(QUERY_SERVER_IDS, Collections.emptyMap(), "QUERY system.prepared_statements")
            .whenComplete(this::gatherServerIds);
      } else {
        LOG.debug(
            "[{}] {} is disabled, repreparing directly",
            logPrefix,
            CoreDriverOption.REPREPARE_CHECK_SYSTEM_TABLE.getPath());
        serverKnownIds = Collections.emptySet();
        gatherPayloadsToReprepare();
      }
    }
  }

  private void gatherServerIds(AdminResult rows, Throwable error) {
    assert channel.eventLoop().inEventLoop();
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
      for (AdminResult.Row row : rows) {
        serverKnownIds.add(row.getByteBuffer("prepared_id"));
      }
      if (rows.hasNextPage()) {
        LOG.debug("[{}] system.prepared_statements has more pages", logPrefix);
        rows.nextPage().whenComplete(this::gatherServerIds);
      } else {
        LOG.debug("[{}] Gathered {} server ids, proceeding", logPrefix, serverKnownIds.size());
        gatherPayloadsToReprepare();
      }
    }
  }

  private void gatherPayloadsToReprepare() {
    assert channel.eventLoop().inEventLoop();
    toReprepare = new LinkedList<>();
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
    assert channel.eventLoop().inEventLoop();
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
    assert channel.eventLoop().inEventLoop();
    if (toReprepare.isEmpty()) {
      runningWorkers -= 1;
      if (runningWorkers == 0) {
        LOG.debug("[{}] All workers finished, done", logPrefix);
        whenPrepared.run();
      }
    } else {
      RepreparePayload payload = toReprepare.poll();
      queryAsync(
              new Prepare(payload.query),
              payload.customPayload,
              String.format("Reprepare '%s'", payload.query))
          .handle(
              (result, error) -> {
                // Don't log, AdminRequestHandler does already
                startWorker();
                return null;
              });
    }
  }

  @VisibleForTesting
  protected CompletionStage<AdminResult> queryAsync(
      Message message, Map<String, ByteBuffer> customPayload, String debugString) {
    AdminRequestHandler reprepareHandler =
        new AdminRequestHandler(channel, message, timeout, logPrefix, debugString);
    return reprepareHandler.start(customPayload);
  }
}
