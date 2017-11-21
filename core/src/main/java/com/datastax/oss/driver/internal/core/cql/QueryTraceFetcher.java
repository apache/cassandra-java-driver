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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.config.CoreDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.cql.CqlSession;
import com.datastax.oss.driver.api.core.cql.QueryTrace;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.TraceEvent;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.netty.util.concurrent.EventExecutor;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

class QueryTraceFetcher {

  private final UUID tracingId;
  private final CqlSession session;
  private final DriverConfigProfile configProfile;
  private final int maxAttempts;
  private final long intervalNanos;
  private final EventExecutor scheduler;
  private final CompletableFuture<QueryTrace> resultFuture = new CompletableFuture<>();

  QueryTraceFetcher(
      UUID tracingId,
      CqlSession session,
      InternalDriverContext context,
      DriverConfigProfile configProfile) {
    this.tracingId = tracingId;
    this.session = session;

    ConsistencyLevel regularConsistency =
        configProfile.getConsistencyLevel(CoreDriverOption.REQUEST_CONSISTENCY);
    ConsistencyLevel traceConsistency =
        configProfile.getConsistencyLevel(CoreDriverOption.REQUEST_TRACE_CONSISTENCY);
    this.configProfile =
        (traceConsistency == regularConsistency)
            ? configProfile
            : configProfile.withConsistencyLevel(
                CoreDriverOption.REQUEST_CONSISTENCY, traceConsistency);

    this.maxAttempts = configProfile.getInt(CoreDriverOption.REQUEST_TRACE_ATTEMPTS);
    this.intervalNanos =
        configProfile.getDuration(CoreDriverOption.REQUEST_TRACE_INTERVAL).toNanos();
    this.scheduler = context.nettyOptions().adminEventExecutorGroup().next();

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
                .withConfigProfile(configProfile)
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
                .withPagingState(pagingState)
                .withConfigProfile(configProfile)
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
      eventsBuilder.add(
          new DefaultTraceEvent(
              eventRow.getString("activity"),
              eventRow.getUuid("event_id").timestamp(),
              eventRow.getInetAddress("source"),
              eventRow.getInt("source_elapsed"),
              eventRow.getString("thread")));
    }
    return new DefaultQueryTrace(
        tracingId,
        sessionRow.getString("request"),
        sessionRow.getInt("duration"),
        sessionRow.getInetAddress("coordinator"),
        sessionRow.getMap("parameters", String.class, String.class),
        sessionRow.getInstant("started_at").toEpochMilli(),
        eventsBuilder.build());
  }
}
