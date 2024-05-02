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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.TraceEvent;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import io.netty.util.concurrent.EventExecutor;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import net.jcip.annotations.ThreadSafe;

@ThreadSafe
class QueryTraceFetcher {

  private final UUID tracingId;
  private final CqlSession session;
  private final DriverExecutionProfile config;
  private final int maxAttempts;
  private final long intervalNanos;
  private final EventExecutor scheduler;
  private final CompletableFuture<QueryTrace> resultFuture = new CompletableFuture<>();

  QueryTraceFetcher(
      UUID tracingId,
      CqlSession session,
      InternalDriverContext context,
      DriverExecutionProfile config) {
    this.tracingId = tracingId;
    this.session = session;

    String regularConsistency = config.getString(DefaultDriverOption.REQUEST_CONSISTENCY);
    String traceConsistency = config.getString(DefaultDriverOption.REQUEST_TRACE_CONSISTENCY);
    this.config =
        traceConsistency.equals(regularConsistency)
            ? config
            : config.withString(DefaultDriverOption.REQUEST_CONSISTENCY, traceConsistency);

    this.maxAttempts = config.getInt(DefaultDriverOption.REQUEST_TRACE_ATTEMPTS);
    this.intervalNanos = config.getDuration(DefaultDriverOption.REQUEST_TRACE_INTERVAL).toNanos();
    this.scheduler = context.getNettyOptions().adminEventExecutorGroup().next();

    querySession(maxAttempts);
  }

  CompletionStage<QueryTrace> fetch() {
    return resultFuture;
  }

  private void querySession(int remainingAttempts) {
    session
        .executeAsync(
            SimpleStatement.builder("SELECT * FROM system_traces.sessions WHERE session_id = ?")
                .addPositionalValue(tracingId)
                .setExecutionProfile(config)
                .build())
        .whenComplete(
            (rs, error) -> {
              if (error != null) {
                resultFuture.completeExceptionally(error);
              } else {
                Row row = rs.one();
                if (row == null || row.isNull("duration") || row.isNull("started_at")) {
                  // Trace is incomplete => fail if last try, or schedule retry
                  if (remainingAttempts == 1) {
                    resultFuture.completeExceptionally(
                        new IllegalStateException(
                            String.format(
                                "Trace %s still not complete after %d attempts",
                                tracingId, maxAttempts)));
                  } else {
                    scheduler.schedule(
                        () -> querySession(remainingAttempts - 1),
                        intervalNanos,
                        TimeUnit.NANOSECONDS);
                  }
                } else {
                  queryEvents(row, new ArrayList<>(), null);
                }
              }
            });
  }

  private void queryEvents(Row sessionRow, List<Row> events, ByteBuffer pagingState) {
    session
        .executeAsync(
            SimpleStatement.builder("SELECT * FROM system_traces.events WHERE session_id = ?")
                .addPositionalValue(tracingId)
                .setPagingState(pagingState)
                .setExecutionProfile(config)
                .build())
        .whenComplete(
            (rs, error) -> {
              if (error != null) {
                resultFuture.completeExceptionally(error);
              } else {
                Iterables.addAll(events, rs.currentPage());
                ByteBuffer nextPagingState = rs.getExecutionInfo().getPagingState();
                if (nextPagingState == null) {
                  resultFuture.complete(buildTrace(sessionRow, events));
                } else {
                  queryEvents(sessionRow, events, nextPagingState);
                }
              }
            });
  }

  private QueryTrace buildTrace(Row sessionRow, Iterable<Row> eventRows) {
    ImmutableList.Builder<TraceEvent> eventsBuilder = ImmutableList.builder();
    for (Row eventRow : eventRows) {
      UUID eventId = eventRow.getUuid("event_id");
      int sourcePort = 0;
      if (eventRow.getColumnDefinitions().contains("source_port")) {
        sourcePort = eventRow.getInt("source_port");
      }
      eventsBuilder.add(
          new DefaultTraceEvent(
              eventRow.getString("activity"),
              eventId == null ? -1 : eventId.timestamp(),
              new InetSocketAddress(eventRow.getInetAddress("source"), sourcePort),
              eventRow.getInt("source_elapsed"),
              eventRow.getString("thread")));
    }
    Instant startedAt = sessionRow.getInstant("started_at");
    int coordinatorPort = 0;
    if (sessionRow.getColumnDefinitions().contains("coordinator_port")) {
      coordinatorPort = sessionRow.getInt("coordinator_port");
    }
    return new DefaultQueryTrace(
        tracingId,
        sessionRow.getString("request"),
        sessionRow.getInt("duration"),
        new InetSocketAddress(sessionRow.getInetAddress("coordinator"), coordinatorPort),
        sessionRow.getMap("parameters", String.class, String.class),
        startedAt == null ? -1 : startedAt.toEpochMilli(),
        eventsBuilder.build());
  }
}
